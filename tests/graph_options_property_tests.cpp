#include <algorithm>
#include <array>
#include <atomic>
#include <climits>
#include <dagflow/detail/test_faults.hpp>
#include <dagflow/task_graph.hpp>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include "property_support.hpp"

namespace {
using dagflow::test::property::Command;
using dagflow::test::property::Random;
using dagflow::test::property::require;
using Graph = dagflow::TaskGraph;
namespace faults = dagflow::detail::test_faults;

// Keep one callable specialization for every successful/rejected option and
// limit transition; template branch aggregation must not split the scenarios
// into unrelated lambda instantiations.
struct Callable {
  std::atomic<unsigned>* calls;
  std::shared_ptr<int> lifetime;
  void operator()() const { calls->fetch_add(1); }
};

struct FaultReset {
  ~FaultReset() { faults::reset(); }
};

class GraphOptionsMachine {
  enum Op : unsigned {
    Define,
    DefineBypass,
    LimitAdd,
    Tokens,
    Seal,
    Run,
    Reset,
    Clear,
    Count
  };
  struct Model {
    bool defined = false;
    bool invalid = false;
    Graph::Overflow overflow = Graph::Overflow::Block;
    unsigned capacity = 1;
    unsigned tokens = 1;
    unsigned expected = 0;
    bool last_error = false;
    unsigned steps = 0;
  } model_;
  std::atomic<unsigned> calls_{0};
  std::weak_ptr<int> capture_;
  std::vector<std::pair<dagflow::Handle, bool>> results_;
  dagflow::Pool pool_{dagflow::Config{.threads = 2}};
  Graph graph_{pool_};

  static Graph::NodeOptions options(unsigned selector, unsigned concurrency) {
    Graph::NodeOptions value;
    constexpr std::array<int, 5> widths{-1, 0, 1, 2, INT_MAX};
    value.concurrency = widths[concurrency];
    value.capacity = 1;
    if (selector & 1) value.priority = dagflow::Priority::High;
    if (selector & 2) value.affinity = UINT32_MAX;
    if (selector == 1) value.capacity = 3;
    if (selector == 2 || selector == 3) {
      value.overflow = Graph::Overflow::Drop;
      value.capacity = selector == 2 ? 0 : 2;
    }
    if (selector == 4 || selector == 5) {
      value.overflow = Graph::Overflow::Fail;
      value.capacity = selector == 4 ? 0 : 2;
    }
    if (selector == 6) value.capacity = 0;
    if (selector == 7) value.priority = static_cast<dagflow::Priority>(255);
    if (selector == 8) value.overflow = static_cast<Graph::Overflow>(255);
    return value;
  }

  void define(unsigned selector, unsigned width, bool bypass, bool limited) {
    FaultReset guard;
    faults::bypass_graph_early_validation = bypass;
    faults::graph_node_limit = limited ? 1 : 0;
    auto lifetime = std::make_shared<int>(42);
    const auto opt = options(selector, width);
    bool invalid_argument = false;
    bool limit_error = false;
    Graph::NodeId id;
    try {
      id = graph_.emplace(Callable{&calls_, lifetime}, opt);
    } catch (const std::invalid_argument&) {
      invalid_argument = true;
    } catch (const std::length_error&) {
      limit_error = true;
    }
    const bool expect_invalid = selector == 6 && !bypass;
    const bool expect_limit = limited && model_.defined;
    require(invalid_argument == expect_invalid,
            "Block capacity zero is rejected before definition commit");
    require(limit_error == expect_limit,
            "lowered node limit rejects exactly a full builder");
    if (expect_invalid || expect_limit) {
      require(lifetime.use_count() == 1,
              "rejected option/limit append retains no callable owner");
      return;
    }
    require(id.idx == 0, "single-node model commits index zero");
    capture_ = lifetime;
    model_.defined = true;
    model_.invalid = selector >= 6;
    model_.overflow = opt.overflow;
    model_.capacity = opt.capacity;
    model_.tokens = 1;
  }

  void execute() {
    if (model_.invalid) {
      bool rejected = false;
      try {
        (void)graph_.run();
      } catch (const std::invalid_argument&) {
        rejected = true;
      }
      require(rejected, "malformed definition cannot start a run");
      return;
    }
    bool failed = false;
    if (model_.defined) {
      failed = model_.overflow == Graph::Overflow::Fail &&
               model_.tokens > model_.capacity;
      if (!failed)
        model_.expected += model_.overflow == Graph::Overflow::Drop
                               ? std::min(model_.tokens, model_.capacity)
                               : model_.tokens;
    }
    auto result = graph_.run();
    pool_.wait(result);
    require(result.valid() == model_.defined,
            "empty and defined graphs have distinct completion validity");
    model_.last_error = failed;
    results_.emplace_back(std::move(result), failed);
  }

 public:
  Command next(Random& random) const {
    static const auto prefix = [] {
      std::vector<Command> commands;
      for (unsigned option = 0; option < 6; ++option) {
        commands.push_back({Define, option, option % 5});
        commands.push_back({Tokens, 0, 0});
        commands.push_back({Seal, 0, 0});
        commands.push_back({Run, 0, 0});
        commands.push_back({Tokens, 1, 0});
        commands.push_back({Run, 0, 0});
        commands.push_back({Tokens, 3, 0});
        commands.push_back({Run, 0, 0});
        commands.push_back({Reset, 0, 0});
        commands.push_back({Run, 0, 0});
        commands.push_back({Clear, 0, 0});
      }
      commands.push_back({Define, 6, 0});
      commands.push_back({LimitAdd, 0, 0});
      commands.push_back({LimitAdd, 0, 0});
      commands.push_back({Run, 0, 0});
      commands.push_back({Clear, 0, 0});
      for (unsigned option : {7u, 8u}) {
        commands.push_back({Define, option, 0});
        commands.push_back({Seal, 0, 0});
        commands.push_back({Run, 0, 0});
        commands.push_back({Clear, 0, 0});
      }
      commands.push_back({DefineBypass, 6, 0});
      commands.push_back({Seal, 0, 0});
      commands.push_back({Run, 0, 0});
      commands.push_back({Clear, 0, 0});
      return commands;
    }();
    if (model_.steps < prefix.size()) return prefix[model_.steps];
    auto op = static_cast<Op>(random.bounded(Count));
    if (model_.defined && (op == Define || op == DefineBypass)) op = Clear;
    if (!model_.defined && op == Tokens) op = Define;
    if (op == Define) return {op, random.bounded(9), random.bounded(5)};
    if (op == DefineBypass) return {op, 6, random.bounded(5)};
    if (op == Tokens) return {op, random.bounded(9), 0};
    return {op, 0, 0};
  }

  bool precondition(const Command& command) const {
    if (command.op >= Count) return false;
    if (command.op == Define)
      return !model_.defined && command.a < 9 && command.b < 5;
    if (command.op == DefineBypass)
      return !model_.defined && command.a == 6 && command.b < 5;
    if (command.op == Tokens)
      return model_.defined && command.a < 9 && command.b == 0;
    return command.a == 0 && command.b == 0;
  }

  void apply(const Command& command) {
    ++model_.steps;
    switch (command.op) {
      case Define:
      case DefineBypass:
        define(command.a, command.b, command.op == DefineBypass, false);
        break;
      case LimitAdd:
        define(0, 0, false, true);
        break;
      case Tokens:
        graph_.set_tokens({0}, command.a);
        model_.tokens = command.a == 0 ? 1 : command.a;
        break;
      case Seal: {
        bool rejected = false;
        try {
          require(graph_.seal(), "valid single-node definition is acyclic");
        } catch (const std::invalid_argument&) {
          rejected = true;
        }
        require(rejected == model_.invalid,
                "seal independently validates admitted options");
        break;
      }
      case Run:
        execute();
        break;
      case Reset:
        graph_.reset();
        model_.last_error = false;
        break;
      case Clear:
        graph_.clear();
        model_.defined = false;
        model_.invalid = false;
        model_.last_error = false;
        require(capture_.expired(), "clear releases even malformed definition");
        break;
    }
  }

  void invariant() const {
    require(graph_.size() == unsigned(model_.defined),
            "option/limit rejection preserves definition cardinality");
    require(calls_.load() == model_.expected,
            "Block/Drop/Fail execute their exact admitted token count");
    require(bool(graph_.last_error()) == model_.last_error,
            "result error changes only through run/reset/clear");
    require(capture_.expired() == !model_.defined,
            "definition owns its callable until clear");
    for (const auto& [result, error] : results_) {
      require(result.ready(), "old options result remains terminal");
      bool failed = false;
      try {
        result.rethrow_if_failed();
      } catch (const std::runtime_error&) {
        failed = true;
      }
      require(failed == error, "rerun and reset preserve historical errors");
    }
    require(
        faults::graph_node_limit == 0 && !faults::bypass_graph_early_validation,
        "test-only validation seams reset after each transition");
  }

  void finish() {
    execute();
    invariant();
    apply({Clear, 0, 0});
  }
};
}  // namespace

int main() {
  dagflow::test::property::run<GraphOptionsMachine>(
      "TaskGraph options validation and admission model", 16, 128);
}
