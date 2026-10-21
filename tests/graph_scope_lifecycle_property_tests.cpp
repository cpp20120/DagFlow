#include <atomic>
#include <dagflow/graph_scope.hpp>
#include <memory>
#include <utility>
#include <vector>

#include "property_support.hpp"

namespace {
using namespace dagflow::test::property;
struct TaskError {};

// A parent occupies the only worker. A run published from that parent cannot
// execute until wait/clear/destruction helps it, so "active" never depends on
// the OS letting us reach the next assertion quickly enough.
class GraphScopeLifecycle {
  enum Op {
    EmptyWait,
    ImplicitWait,
    RepeatedRun,
    ClearPending,
    DestroyPending,
    DestroyDirty,
    ErrorWait,
    ErrorRethrow,
    RetryAfterError,
    Count
  };
  struct Model {
    unsigned roots{}, successors{}, destroyed{};
  } expected_;
  std::atomic<unsigned> roots_{0}, successors_{0}, destroyed_{0};
  std::vector<std::pair<dagflow::Handle, bool>> results_;
  dagflow::Pool pool_{[] {
    dagflow::Config config;
    config.threads = 1;
    config.pin_threads = false;
    return config;
  }()};

  struct Capture {
    std::atomic<unsigned>& destroyed;
    ~Capture() { ++destroyed; }
  };

  static void check_result(const dagflow::Handle& handle, bool error) {
    require(handle.ready(), "completed scope results remain ready");
    bool failed = false;
    try {
      handle.rethrow_if_failed();
    } catch (const TaskError&) {
      failed = true;
    }
    require(failed == error, "scope results retain their original task error");
  }

  void exercise(const Command& c) {
    const bool initially_fails = c.op >= ErrorWait || c.b != 0;
    bool failing = initially_fails;
    auto scope = std::make_unique<dagflow::GraphScope>(pool_);
    if (c.op == EmptyWait) {
      scope->wait();
      scope->wait();
      require(scope->empty() && !scope->last_error(),
              "waiting an empty unstarted scope is a no-op");
      return;
    }
    auto capture = std::make_shared<Capture>(destroyed_);
    std::weak_ptr<Capture> borrowed = capture;
    const auto root =
        scope->emplace([this, capture = std::move(capture), &failing] {
          ++roots_;
          if (failing) throw TaskError{};
        });
    scope->then(root, [this] { ++successors_; });
    dagflow::Handle result;
    if (c.op != ImplicitWait && c.op != DestroyDirty && c.op != ErrorRethrow) {
      result = scope->run();
      require(result.valid() && !result.ready(),
              "graph is genuinely pending while its sole worker runs parent");
      require(roots_.load() == expected_.roots,
              "run publishes work without executing it inline");
      results_.emplace_back(result, initially_fails);
    }
    if (c.op == ClearPending) {
      scope->clear();
      require(scope->empty() && !scope->last_error(),
              "clear waits for active work then clears definition and error");
      require(result.ready() && borrowed.expired(),
              "clear returns only after completion and capture release");
    } else if (c.op == DestroyPending || c.op == DestroyDirty) {
      scope.reset();
      require(borrowed.expired(), "destruction drains and releases captures");
      if (result.valid())
        require(result.ready(), "destructor drains active run");
    } else {
      if (c.op == RepeatedRun) {
        auto duplicate = scope->run();
        require(duplicate.valid() && !duplicate.ready(),
                "second run observes the already pending execution");
        results_.emplace_back(duplicate, initially_fails);
      }
      bool threw = false;
      try {
        if (c.op == ErrorRethrow)
          scope->run_and_wait();
        else
          scope->wait();
      } catch (const TaskError&) {
        threw = true;
      }
      require(threw == (c.op == ErrorRethrow),
              "wait records task errors; run_and_wait rethrows them");
      require(bool(scope->last_error()) == initially_fails,
              "wait exposes the completed run's error");
      require(!borrowed.expired(), "wait retains the reusable definition");
      scope->wait();
      require(roots_.load() == expected_.roots + 1,
              "repeated run before wait and repeated wait do not rerun work");
      if (c.op == RetryAfterError) {
        failing = false;
        auto retry = scope->run();
        require(!retry.ready(), "retry is a new pending run");
        results_.emplace_back(retry, false);
        scope->wait();
        require(!scope->last_error(),
                "successful retry replaces current error");
        ++expected_.roots;
        ++expected_.successors;
      }
      scope->clear();
      require(borrowed.expired(),
              "clear releases retained definition captures");
    }
    ++expected_.roots;
    expected_.successors += !initially_fails;
    ++expected_.destroyed;
  }

 public:
  Command next(Random& random) const {
    return {static_cast<unsigned>(random.bounded(Count)), 0, random.bounded(2)};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a == 0 && c.b < 2;
  }
  void apply(const Command& c) {
    pool_.wait_and_rethrow(pool_.submit([this, c] { exercise(c); }));
  }
  void invariant() const {
    require(roots_.load() == expected_.roots,
            "each modeled run invokes its root exactly once");
    require(successors_.load() == expected_.successors,
            "failed roots suppress successors; retry restores execution");
    require(destroyed_.load() == expected_.destroyed,
            "every retired graph definition destroys its capture once");
    for (const auto& [handle, failed] : results_) check_result(handle, failed);
  }
  void finish() { pool_.wait_idle(); }
};
}  // namespace

int main() {
  run<GraphScopeLifecycle>("GraphScope pending lifecycle and error recovery",
                           24, 64);
}
