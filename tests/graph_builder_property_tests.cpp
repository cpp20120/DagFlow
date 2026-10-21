#include <array>
#include <atomic>
#include <climits>
#include <dagflow/task_graph.hpp>
#include <memory>
#include <mutex>
#include <new>
#include <stdexcept>
#include <utility>
#include <vector>

#include "adversarial_allocation.hpp"
#include "property_support.hpp"

namespace {
using dagflow::test::property::Command;
using dagflow::test::property::Random;
using dagflow::test::property::require;
using Graph = dagflow::TaskGraph;
namespace allocation = adversarial::allocation;

struct Observation {
  std::mutex mutex;
  std::atomic<unsigned> calls{0};
  std::atomic<unsigned> callable_state{0};
  int copies_before_failure = -1;
  int moves_before_failure = -1;
};

void construction_checkpoint(int& remaining) {
  if (remaining == 0) throw std::runtime_error("graph callable construction");
  if (remaining > 0) --remaining;
}

// A single named type exercises both successful and throwing copies through
// each public wrapper specialization. Nothrow move permits inline Work storage.
struct InlineCallable {
  std::shared_ptr<Observation> observation;
  unsigned state = 0;

  explicit InlineCallable(std::shared_ptr<Observation> value)
      : observation(std::move(value)) {}
  InlineCallable(const InlineCallable& other)
      : observation(other.observation), state(other.state) {
    construction_checkpoint(observation->copies_before_failure);
  }
  InlineCallable(InlineCallable&&) noexcept = default;
  void operator()() {
    // Options may admit parallel tokens: serialize this intentionally mutable
    // callable state, so checking its persistence never introduces a data race.
    std::lock_guard lock(observation->mutex);
    observation->callable_state.store(++state);
    observation->calls.fetch_add(1);
  }
};
static_assert(sizeof(InlineCallable) <= DAGFLOW_TASK_FN_SIZE);

// The same throwing-move specialization succeeds normally or fails at either
// capture initialization or subsequent Work construction, depending on budget.
struct ThrowingCallable : InlineCallable {
  using InlineCallable::InlineCallable;
  ThrowingCallable(const ThrowingCallable&) = default;
  ThrowingCallable(ThrowingCallable&& other)
      : InlineCallable(prepare_move(other)) {}

 private:
  static InlineCallable&& prepare_move(ThrowingCallable& other) {
    construction_checkpoint(other.observation->moves_before_failure);
    return std::move(other);
  }
};

struct AllocationGuard {
  allocation::Fault saved = allocation::fault;
  explicit AllocationGuard(int budget) { allocation::fault = {budget}; }
  ~AllocationGuard() { allocation::fault = saved; }
};

class GraphBuilderMachine {
  enum Op : unsigned {
    Add,
    RejectCopy,
    RejectMove,
    RejectPendingAllocation,
    FaultSeal,
    Tokens,
    Seal,
    Run,
    Reset,
    Clear,
    Count
  };
  static constexpr unsigned limit = 16;
  struct Node {
    std::shared_ptr<Observation> observation;
    Graph::NodeOptions options;
    unsigned tokens = 1;
    unsigned expected = 0;
  };
  struct Model {
    std::vector<Node> nodes;
    bool last_error = false;
    unsigned steps = 0;
  } model_;
  std::vector<std::pair<dagflow::Handle, bool>> results_;
  dagflow::Pool pool_{dagflow::Config{.threads = 2}};
  Graph graph_{pool_};

  static Graph::NodeOptions options(unsigned selector) {
    Graph::NodeOptions result;
    result.capacity = selector + 1;
    result.concurrency = selector % 3;
    if (selector & 1) result.priority = dagflow::Priority::High;
    if (selector & 2) result.affinity = UINT32_MAX;
    return result;
  }

  template <class Callable>
  Graph::NodeId insert(Callable& callable, unsigned wrapper,
                       Graph::NodeOptions opt) {
    switch (wrapper) {
      case 0:
        return graph_.emplace(callable);
      case 1:
        return graph_.emplace(std::move(callable));
      case 2:
        return graph_.add_node(callable);
      case 3:
        return graph_.add_node(std::move(callable));
      case 4:
        return graph_.emplace(callable, opt);
      case 5:
        return graph_.emplace(std::move(callable), opt);
      case 6:
        return graph_.add_node(callable, opt);
      default:
        return graph_.add_node(std::move(callable), opt);
    }
  }

  Graph::NodeId insert(unsigned variant,
                       const std::shared_ptr<Observation>& observation,
                       Graph::NodeOptions opt) {
    if (variant < 8) {
      InlineCallable callable(observation);
      return insert(callable, variant, opt);
    }
    ThrowingCallable callable(observation);
    return insert(callable, variant - 8, opt);
  }

  void add(unsigned variant, unsigned selector) {
    Node node;
    node.observation = std::make_shared<Observation>();
    // The no-options overloads intentionally ignore the supplied selector.
    node.options = variant % 8 < 4 ? Graph::NodeOptions{} : options(selector);
    const auto id = insert(variant, node.observation, node.options);
    require(id.idx == model_.nodes.size(),
            "successful wrapper returns the next committed node index");
    if (id.idx) graph_.add_edge({id.idx - 1}, id);
    model_.nodes.push_back(std::move(node));
  }

  void execute() {
    bool error = false;
    // A chain gives exact outcomes even with Fail or zero-capacity Drop.
    // This is a public admission-policy model, not a lane/scheduler simulation.
    for (auto& node : model_.nodes) {
      if (node.options.overflow == Graph::Overflow::Fail &&
          node.tokens > node.options.capacity) {
        error = true;
        break;
      }
      node.expected +=
          node.options.overflow == Graph::Overflow::Drop
              ? std::min<std::size_t>(node.tokens, node.options.capacity)
              : node.tokens;
    }
    auto handle = graph_.run();
    pool_.wait(handle);
    pool_.wait_idle();
    model_.last_error = model_.nodes.empty() ? model_.last_error : error;
    results_.emplace_back(std::move(handle), error);
  }

 public:
  GraphBuilderMachine() {
    // Grow builder storage through public operations, then clear it. clear()
    // retains vector capacity. A later seal() empties pending Work storage, so
    // its first allocation is unambiguously AFTER insertion of a builder node.
    for (unsigned i = 0; i < limit; ++i) graph_.emplace([] {});
    graph_.clear();
    add(0, 0);
    add(9, 0);
    add(6, 0);
    require(graph_.seal(), "initial callable state compiles");
    execute();
  }

  Command next(Random& random) const {
    // Stable boundary prefix covers both normal and exceptional paths through
    // the SAME named template specializations before the generated history.
    constexpr std::array<Command, 47> prefix{{
        {RejectPendingAllocation, 0, 0},
        {Run, 0, 0},
        {Clear, 0, 0},
        {Add, 0, 0},
        {Add, 1, 0},
        {Add, 2, 0},
        {Add, 3, 0},
        {Add, 4, 0},
        {Add, 5, 0},
        {Add, 6, 0},
        {Add, 7, 0},
        {Add, 8, 0},
        {Add, 9, 0},
        {Add, 10, 0},
        {Add, 11, 0},
        {Add, 12, 0},
        {Add, 13, 0},
        {Add, 14, 0},
        {Add, 15, 0},
        {Run, 0, 0},
        {Clear, 0, 0},
        {Add, 0, 0},
        {Add, 9, 0},
        {Add, 6, 0},
        {Run, 0, 0},
        {RejectCopy, 0, 0},
        {RejectCopy, 2, 0},
        {RejectCopy, 4, 0},
        {RejectCopy, 6, 0},
        {RejectCopy, 8, 0},
        {RejectCopy, 10, 0},
        {RejectCopy, 12, 0},
        {RejectCopy, 14, 0},
        {RejectMove, 9, 0},
        {RejectMove, 9, 1},
        {RejectMove, 11, 0},
        {RejectMove, 11, 1},
        {RejectMove, 13, 0},
        {RejectMove, 13, 1},
        {RejectMove, 15, 0},
        {RejectMove, 15, 1},
        {FaultSeal, 0, 0},
        {FaultSeal, 0, 1},
        {Run, 0, 0},
        {Add, 13, 0},
        {FaultSeal, 0, 4},
        {Run, 0, 0},
    }};
    if (model_.steps < prefix.size()) return prefix[model_.steps];
    auto op = static_cast<Op>(random.bounded(Count));
    if (model_.nodes.size() == limit &&
        (op == Add || op == RejectCopy || op == RejectMove ||
         op == RejectPendingAllocation))
      op = Clear;
    if (model_.nodes.empty() && (op == FaultSeal || op == Tokens)) op = Add;
    if (op == Add) return {op, random.bounded(16), random.bounded(8)};
    if (op == RejectCopy) return {op, 2 * random.bounded(8), 0};
    if (op == RejectMove)
      return {op, 9 + 2 * random.bounded(4), random.bounded(2)};
    if (op == RejectPendingAllocation) return {op, random.bounded(8), 0};
    if (op == FaultSeal)
      return {op, random.bounded(model_.nodes.size()), random.bounded(8)};
    if (op == Tokens)
      return {op, random.bounded(model_.nodes.size()), random.bounded(9)};
    return {op, 0, 0};
  }

  bool precondition(const Command& command) const {
    if (command.op >= Count) return false;
    if (command.op <= RejectPendingAllocation && model_.nodes.size() == limit)
      return false;
    switch (command.op) {
      case Add:
        return command.a < 16 && command.b < 8;
      case RejectCopy:
        return command.a < 16 && command.a % 2 == 0 && command.b == 0;
      case RejectMove:
        return command.a >= 9 && command.a < 16 && command.a % 2 == 1 &&
               command.b < 2;
      case RejectPendingAllocation:
        return command.a < 8 && command.b == 0;
      case FaultSeal:
        return command.a < model_.nodes.size() && command.b < 8;
      case Tokens:
        return command.a < model_.nodes.size() && command.b < 9;
      default:
        return command.a == 0 && command.b == 0;
    }
  }

  void apply(const Command& command) {
    ++model_.steps;
    if (command.op == Add) {
      add(command.a, command.b);
    } else if (command.op == RejectCopy || command.op == RejectMove) {
      std::weak_ptr<Observation> weak;
      bool rejected = false;
      {
        auto observation = std::make_shared<Observation>();
        weak = observation;
        if (command.op == RejectCopy)
          observation->copies_before_failure = 0;
        else
          observation->moves_before_failure = command.b;
        try {
          (void)insert(command.a, observation, {});
        } catch (const std::runtime_error&) {
          rejected = true;
        }
        require(observation->calls.load() == 0,
                "rejected callable construction never invokes user work");
      }
      require(rejected, "selected wrapper construction checkpoint throws");
      require(weak.expired(),
              "failed capture/Work construction leaves no owner");
    } else if (command.op == RejectPendingAllocation) {
      require(graph_.seal(), "compile existing definition before append fault");
      struct Checkpoint {
        Graph* graph;
        std::size_t size_at_failure = 0;
      } checkpoint{&graph_};
      bool rejected = false;
      std::weak_ptr<Observation> weak;
      const auto live_before = allocation::live.load();
      {
        auto observation = std::make_shared<Observation>();
        weak = observation;
        AllocationGuard guard(0);
        allocation::fault.context = &checkpoint;
        allocation::fault.on_failure = [](void* context) noexcept {
          auto& point = *static_cast<Checkpoint*>(context);
          point.size_at_failure = point.graph->size();
        };
        try {
          (void)insert(command.a, observation, {});
        } catch (const std::bad_alloc&) {
          rejected = true;
        }
      }
      require(rejected, "pending callable allocation failure propagates");
      require(checkpoint.size_at_failure == model_.nodes.size() + 1,
              "fault occurred after provisional builder-node insertion");
      require(weak.expired(), "pending allocation rollback releases callable");
      require(allocation::live.load() == live_before,
              "pending insertion rollback retains no runtime allocation");
    } else if (command.op == FaultSeal) {
      auto& node = model_.nodes[command.a];
      graph_.set_tokens({command.a}, node.tokens);  // Invalidate compilation.
      {
        AllocationGuard guard(command.b);
        try {
          require(graph_.seal(), "acyclic definition seals when budget allows");
        } catch (const std::bad_alloc&) {
          // Failed compilation must retain both old and pending callables.
        }
      }
    } else if (command.op == Tokens) {
      graph_.set_tokens({command.a}, command.b);
      model_.nodes[command.a].tokens = command.b == 0 ? 1 : command.b;
    } else if (command.op == Seal) {
      require(graph_.seal(), "valid definition can be sealed and resealed");
    } else if (command.op == Run) {
      execute();
    } else if (command.op == Reset) {
      graph_.reset();
      model_.last_error = false;
    } else if (command.op == Clear) {
      std::vector<std::weak_ptr<Observation>> captures;
      for (const auto& node : model_.nodes)
        captures.push_back(node.observation);
      graph_.clear();
      model_.nodes.clear();
      model_.last_error = false;
      for (const auto& capture : captures)
        require(capture.expired(), "clear releases compiled and pending work");
    }
  }

  void invariant() const {
    require(graph_.size() == model_.nodes.size(),
            "append rejection rolls back provisional node insertion");
    require(bool(graph_.last_error()) == model_.last_error,
            "builder changes preserve the previous execution result");
    for (const auto& node : model_.nodes) {
      require(node.observation.use_count() == 2,
              "exactly one graph callable owns each model observation");
      require(node.observation->calls.load() == node.expected,
              "builder failures preserve admitted work and exact token count");
      require(node.observation->callable_state.load() == node.expected,
              "compiled callable mutable state survives additions and reseal");
    }
    for (const auto& [result, error] : results_) {
      require(result.ready(), "prior completion survives builder mutation");
      bool threw = false;
      try {
        result.rethrow_if_failed();
      } catch (const std::runtime_error&) {
        threw = true;
      }
      require(threw == error, "prior completion retains its own failure");
    }
    require(allocation::fault.remaining == -1,
            "allocation fault does not escape a model transition");
  }

  void finish() {
    execute();
    invariant();
    apply({Clear, 0, 0});
  }
};
}  // namespace

int main() {
  dagflow::test::property::run<GraphBuilderMachine>(
      "TaskGraph builder ownership and construction rollback", 32, 80);
}
