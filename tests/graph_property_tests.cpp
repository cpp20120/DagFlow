#include <algorithm>
#include <array>
#include <atomic>
#include <dagflow/dagflow.hpp>
#include <memory>
#include <stdexcept>
#include <vector>

#include "property_support.hpp"

namespace {
using dagflow::test::property::Command;
using dagflow::test::property::Random;
using dagflow::test::property::require;
using Graph = dagflow::TaskGraph;

// Model node counts and predecessor relations are independent of CSR storage,
// lanes, packets and scheduler order. Only complete runs mutate the definition.
class GraphMachine {
  static constexpr unsigned limit = 8;
  enum Op : unsigned { add, edge, tokens, seal, start, wait, reset, clear };
  struct Node {
    unsigned tokens = 1;
    unsigned capacity = 1;
    bool drop = false;
    unsigned total = 0;
  };
  struct Model {
    std::vector<Node> nodes;
    std::vector<std::pair<unsigned, unsigned>> edges;
    bool pending = false;
  } model_;
  std::array<std::atomic<unsigned>, limit> calls_{};
  std::array<unsigned, limit> target_{};
  std::atomic<bool> wrong_order_{false};
  std::vector<std::weak_ptr<int>> captures_;
  std::vector<dagflow::Handle> old_results_;
  dagflow::Handle result_;
  dagflow::Pool pool_{dagflow::Config{.threads = 2}};
  Graph graph_{pool_};

  bool acyclic() const {
    // Transitive closure, deliberately distinct from the implementation's
    // indegree-based topological compilation; parallel edges are harmless.
    std::array<std::array<bool, limit>, limit> reach{};
    for (auto [a, b] : model_.edges) reach[a][b] = true;
    for (unsigned k = 0; k < model_.nodes.size(); ++k)
      for (unsigned i = 0; i < model_.nodes.size(); ++i)
        for (unsigned j = 0; j < model_.nodes.size(); ++j)
          reach[i][j] = reach[i][j] || (reach[i][k] && reach[k][j]);
    for (unsigned i = 0; i < model_.nodes.size(); ++i)
      if (reach[i][i]) return false;
    return true;
  }

 public:
  Command next(Random& random) const {
    if (model_.pending) return {wait, 0, 0};
    auto op = static_cast<Op>(random.bounded(8));
    if ((op == edge || op == tokens) && model_.nodes.empty()) op = add;
    if (op == add && model_.nodes.size() == limit) op = start;
    if (op == wait) op = start;  // Waiting requires an in-flight run.
    if (op == edge)
      return {op, random.bounded(model_.nodes.size()),
              random.bounded(model_.nodes.size())};
    if (op == tokens) {
      constexpr std::array<unsigned, 7> counts{0, 1, 2, 63, 64, 65, 129};
      return {op, random.bounded(model_.nodes.size()),
              counts[random.bounded(counts.size())]};
    }
    return {op, random.bounded(4), random.bounded(8)};
  }
  bool precondition(const Command& command) const {
    if (command.op > clear) return false;
    if (model_.pending) return command.op == wait;
    if (command.op == wait) return false;
    if (command.op == add) return model_.nodes.size() < limit;
    if (command.op == edge)
      return command.a < model_.nodes.size() && command.b < model_.nodes.size();
    if (command.op == tokens)
      return command.a < model_.nodes.size() && command.b <= 129;
    return true;
  }
  void apply(const Command& command) {
    switch (command.op) {
      case add: {
        const unsigned index = model_.nodes.size();
        Node node;
        node.drop = (command.a & 1) != 0;
        node.capacity = command.b % 8 + (node.drop ? 0 : 1);
        Graph::NodeOptions options;
        options.concurrency = static_cast<int>(command.a % 4 + 1);
        options.capacity = node.capacity;
        options.overflow =
            node.drop ? Graph::Overflow::Drop : Graph::Overflow::Block;
        auto capture = std::make_shared<int>(42);
        captures_.push_back(capture);
        auto id = graph_.emplace(
            [this, index, capture = std::move(capture)] {
              for (auto [a, b] : model_.edges)
                if (b == index && calls_[a].load() != target_[a])
                  wrong_order_.store(true);
              calls_[index].fetch_add(1);
            },
            options);
        require(id.idx == index, "new node has its model index");
        model_.nodes.push_back(node);
        break;
      }
      case edge:
        graph_.add_edge({command.a}, {command.b});
        model_.edges.emplace_back(command.a, command.b);
        break;
      case tokens:
        graph_.set_tokens({command.a}, command.b);
        model_.nodes[command.a].tokens = command.b ? command.b : 1;
        break;
      case seal:
        require(graph_.seal() == acyclic(), "seal agrees with reachability");
        break;
      case start: {
        if (!acyclic()) {
          bool rejected = false;
          try {
            (void)graph_.run();
          } catch (const std::logic_error&) {
            rejected = true;
          }
          require(rejected, "cyclic definition cannot start");
          break;
        }
        for (unsigned i = 0; i < model_.nodes.size(); ++i) {
          const auto& node = model_.nodes[i];
          target_[i] =
              node.total +
              (node.drop ? std::min(node.tokens, node.capacity) : node.tokens);
        }
        result_ = graph_.run();
        require(result_.valid() == !model_.nodes.empty(),
                "only an empty graph has an invalid result");
        model_.pending = true;
        break;
      }
      case wait:
        pool_.wait(result_);
        result_.rethrow_if_failed();
        require(result_.ready(), "wait observes completion");
        for (unsigned i = 0; i < model_.nodes.size(); ++i)
          model_.nodes[i].total = target_[i];
        old_results_.push_back(result_);
        model_.pending = false;
        break;
      case reset:
        graph_.reset();
        require(!graph_.last_error(), "reset discards the last result");
        break;
      case clear:
        graph_.clear();
        for (const auto& capture : captures_)
          require(capture.expired(), "clear releases definition captures");
        captures_.clear();
        model_.nodes.clear();
        model_.edges.clear();
        for (auto& count : calls_) count.store(0);
        break;
    }
  }
  void finish() {
    if (model_.pending) apply({wait, 0, 0});
  }
  void invariant() const {
    require(graph_.size() == model_.nodes.size(), "definition size matches");
    require(graph_.empty() == model_.nodes.empty(), "empty matches model");
    require(!wrong_order_.load(), "all predecessor tokens precede successors");
    if (!model_.pending)
      for (unsigned i = 0; i < model_.nodes.size(); ++i)
        require(calls_[i].load() == model_.nodes[i].total,
                "exact admitted token count across runs");
    for (const auto& capture : captures_)
      require(!capture.expired(), "definition retains callable across runs");
    for (const auto& result : old_results_) {
      require(result.ready(), "old results remain ready after reuse/clear");
      result.rethrow_if_failed();
    }
  }
};

// A serial dependency chain makes cancellation/error outcomes exact, without
// assuming which of several concurrent roots wins an execution race.
class GraphOutcomeMachine {
  enum Op { succeed, fail, cancel, reset, observe };
  enum class Outcome { success, failure, cancellation };
  struct Model {
    std::array<unsigned, 6> calls{};
    bool error = false;
  } model_;
  std::array<std::atomic<unsigned>, 6> calls_{};
  Outcome outcome_ = Outcome::success;
  unsigned stop_ = 0;
  std::vector<std::pair<dagflow::Handle, bool>> results_;
  dagflow::Pool pool_{dagflow::Config{.threads = 2}};
  Graph graph_{pool_};

 public:
  GraphOutcomeMachine() {
    for (unsigned i = 0; i < calls_.size(); ++i) {
      graph_.emplace([this, i] {
        calls_[i].fetch_add(1);
        if (i == stop_) {
          if (outcome_ == Outcome::failure)
            throw std::runtime_error("model graph failure");
          if (outcome_ == Outcome::cancellation) graph_.cancel();
        }
      });
      if (i) graph_.add_edge({i - 1}, {i});
    }
  }
  Command next(Random& random) const {
    return {static_cast<unsigned>(random.bounded(5)), random.bounded(6), 0};
  }
  bool precondition(const Command& command) const {
    return command.op <= observe && command.a < calls_.size();
  }
  void apply(const Command& command) {
    if (command.op == reset) {
      graph_.reset();
      model_.error = false;
      return;
    }
    if (command.op == observe) return;
    outcome_ = command.op == fail     ? Outcome::failure
               : command.op == cancel ? Outcome::cancellation
                                      : Outcome::success;
    stop_ = command.a;
    auto result = graph_.run();
    pool_.wait(result);
    const unsigned executed = outcome_ == Outcome::success ? 6 : stop_ + 1;
    for (unsigned i = 0; i < executed; ++i) ++model_.calls[i];
    model_.error = outcome_ == Outcome::failure;
    results_.emplace_back(std::move(result), model_.error);
  }
  void invariant() const {
    for (unsigned i = 0; i < calls_.size(); ++i)
      require(calls_[i].load() == model_.calls[i],
              "failure/cancellation skips precisely the chain suffix");
    require(bool(graph_.last_error()) == model_.error,
            "last error resets on reset or next successful run");
    for (const auto& [result, error] : results_) {
      require(result.ready(), "historical run remains ready");
      bool failed = false;
      try {
        result.rethrow_if_failed();
      } catch (const std::runtime_error&) {
        failed = true;
      }
      require(failed == error, "historical result preserves its own error");
    }
  }
};

// GraphScope has its own state machine: dirty definitions start implicitly,
// repeated run observes one run, and wait consumes that run exactly once.
class GraphScopeMachine {
  enum Op : unsigned { add, then, join, start, wait, clear, destroy };
  struct Model {
    unsigned nodes = 0;
    bool dirty = false;
    bool started = false;
    std::array<unsigned, 8> total{};
    std::vector<std::pair<unsigned, unsigned>> edges;
  } model_;
  std::array<std::atomic<unsigned>, 8> calls_{};
  std::array<unsigned, 8> target_{};
  std::atomic<bool> wrong_order_{false};
  std::vector<dagflow::Handle> results_;
  dagflow::Pool pool_{dagflow::Config{.threads = 2}};
  std::unique_ptr<dagflow::GraphScope> scope_ =
      std::make_unique<dagflow::GraphScope>(pool_);

  void begin_run() {
    for (unsigned i = 0; i < model_.nodes; ++i)
      target_[i] = model_.total[i] + 1;
    model_.started = true;
    model_.dirty = false;
  }
  void finish_run() {
    if (model_.started)
      for (unsigned i = 0; i < model_.nodes; ++i) model_.total[i] = target_[i];
    model_.started = false;
  }
  void check_calls() const {
    for (unsigned i = 0; i < model_.nodes; ++i)
      require(calls_[i].load() == model_.total[i],
              "scope runs each definition exactly when its model starts it");
  }

 public:
  Command next(Random& random) const {
    auto op = static_cast<Op>(random.bounded(7));
    if (model_.started && op <= join) op = wait;
    if (model_.nodes == 8 && op <= join) op = clear;
    if (model_.nodes == 0 && (op == then || op == join)) op = add;
    return {op, random.bounded(std::max(1u, model_.nodes)),
            random.bounded(std::max(1u, model_.nodes))};
  }
  bool precondition(const Command& command) const {
    if (command.op > destroy) return false;
    if (command.op <= join) {
      if (model_.started || model_.nodes == 8) return false;
      if (command.op != add)
        return command.a < model_.nodes && command.b < model_.nodes;
    }
    return true;
  }
  void apply(const Command& command) {
    if (command.op <= join) {
      const unsigned index = model_.nodes;
      auto work = [this, index] {
        for (auto [a, b] : model_.edges)
          if (b == index && calls_[a].load() != target_[a])
            wrong_order_.store(true);
        calls_[index].fetch_add(1);
      };
      dagflow::JobHandle job;
      if (command.op == add) {
        job = scope_->emplace(work);
      } else if (command.op == then) {
        job = scope_->then(dagflow::JobHandle{command.a}, work);
        model_.edges.emplace_back(command.a, index);
      } else {
        job = scope_->when_all(
            {dagflow::JobHandle{command.a}, dagflow::JobHandle{command.b}},
            work);
        model_.edges.emplace_back(command.a, index);
        model_.edges.emplace_back(command.b, index);
      }
      require(job.valid() && job.id() == index, "scope assigns model IDs");
      ++model_.nodes;
      model_.dirty = true;
    } else if (command.op == start) {
      if (!model_.started) begin_run();
      results_.push_back(scope_->run());
    } else if (command.op == wait) {
      if (!model_.started && model_.dirty) begin_run();
      scope_->wait();
      finish_run();
    } else {
      if (command.op == destroy && !model_.started && model_.dirty) begin_run();
      if (command.op == clear)
        scope_->clear();
      else
        scope_.reset();
      finish_run();
      check_calls();
      model_ = {};
      for (auto& count : calls_) count.store(0);
      if (!scope_) scope_ = std::make_unique<dagflow::GraphScope>(pool_);
    }
  }
  void finish() { apply({destroy, 0, 0}); }
  void invariant() const {
    require(scope_->size() == model_.nodes, "scope definition size matches");
    require(!wrong_order_.load(), "then/when_all observe all predecessors");
    if (!model_.started) {
      check_calls();
      for (const auto& result : results_)
        require(result.ready(), "scope result survives wait/clear/destruction");
    }
  }
};
}  // namespace

int main() {
  dagflow::test::property::run<GraphMachine>("TaskGraph topology and tokens",
                                             32, 64);
  dagflow::test::property::run<GraphOutcomeMachine>(
      "TaskGraph failure cancellation and reuse", 24, 48);
  dagflow::test::property::run<GraphScopeMachine>("GraphScope lifecycle", 32,
                                                  64);
}
