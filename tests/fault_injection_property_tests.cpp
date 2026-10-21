#include <array>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <dagflow/detail/idle_accounting.hpp>
#include <dagflow/detail/parking_lot.hpp>
#include <dagflow/detail/test_faults.hpp>
#include <dagflow/task_graph.hpp>
#include <dagflow/task_scope.hpp>
#include <dagflow/thread_pool.hpp>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <vector>

#if defined(__linux__) && !defined(__SANITIZE_THREAD__)
#include <sys/wait.h>
#include <unistd.h>
#endif
#include "property_support.hpp"

namespace {
using dagflow::test::property::Command;
using dagflow::test::property::Random;
using dagflow::test::property::require;
using dagflow::test::property::run;
namespace fault = dagflow::detail::test_faults;

template <typename Exception, typename F>
bool throws_as(F&& action) {
  try {
    action();
  } catch (const Exception&) {
    return true;
  }
  return false;
}

// A real accounting instance must retain its obligations after an injected
// condition-variable error, and only transition to idle after a retirement.
// Saturation failures are side-effect free: the same instance remains usable.
class AccountingFaultMachine {
  enum Op { FailedWait, FailedWorkerPublish, FailedExternalPublish, Count };
  unsigned operations_{};
 public:
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)), rng.bounded(4), 0};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 4 && c.b == 0;
  }
  void apply(const Command& c) {
    fault::ResetOnExit reset;
    dagflow::detail::IdleAccounting accounting(2);
    switch (c.op) {
      case FailedWait:
        accounting.publish_external();
        fault::fail_idle_wait = true;
        // No test-side mutation of outstanding credits before the error.
        {
          bool failed = false;
          try { accounting.wait(); }
          catch (const fault::injected_wait_error&) { failed = true; }
          require(failed, "wait forwards its injected error");
        }
        require(!fault::fail_idle_wait, "wait fault consumed once");
        accounting.retire(0);
        accounting.worker_idle();
        accounting.wait();
        break;
      case FailedWorkerPublish:
        fault::force_worker_publish_overflow = true;
        require(throws_as<std::overflow_error>(
                    [&] { accounting.publish_worker(0); }),
                "saturated publication rejects credit before increment");
        fault::force_worker_publish_overflow = false;
        accounting.publish_worker(0);
        accounting.retire(0);
        accounting.wait();
        break;
      case FailedExternalPublish:
        fault::force_worker_publish_overflow = true;
        require(throws_as<std::overflow_error>(
                    [&] { accounting.publish_external(); }),
                "saturated producer lane rejects without registering a task");
        fault::force_worker_publish_overflow = false;
        accounting.publish_external();
        accounting.retire(1);
        accounting.wait();
        break;
    }
    ++operations_;
  }
  void invariant() const { require(operations_ < 10000, "bounded accounting history"); }
};

// Throw while sleeping=true and while the idle bitmap contains a registration.
// Catch must clear both. The next real wake cannot target a stale waiter.
class ParkingFaultMachine {
  enum Op { FailedWait, Timeout, Stopped, Count };
  dagflow::detail::Scheduler scheduler_{1, 1, 1};
  dagflow::detail::ParkingLot lot_{scheduler_};
  uint64_t expected_epoch_{};
 public:
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)), rng.bounded(5), 0};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 5 && c.b == 0;
  }
  void apply(const Command& c) {
    fault::ResetOnExit reset;
    const auto epoch = lot_.prepare(0);
    require(epoch == expected_epoch_, "registration observes expected epoch");
    std::atomic<bool> stop{c.op == Stopped};
    if (c.op == FailedWait) fault::fail_parking_wait = true;
    bool failed = false;
    try {
      lot_.wait(0, epoch, 0, stop);
    } catch (const fault::injected_wait_error&) {
      failed = true;
    }
    require(failed == (c.op == FailedWait), "only faulted waits throw");
    lot_.wake_one(0); // No registration should survive a wait or a throw.
    require(lot_.prepare(0) == expected_epoch_,
            "failure/timeout/stop clears idle registration");
    lot_.wake_one(0);
    ++expected_epoch_;
    require(lot_.prepare(0) == expected_epoch_,
            "new registration is not lost after an exceptional wait");
    lot_.cancel(0);
  }
  void invariant() const { require(expected_epoch_ < 10000, "bounded wake history"); }
};

// Rejected publisher reservations leave the Pool/TaskScope open and usable.
// A failed batch with an accepted prefix must retire EVERY capture and run
// exactly the prefix, including when the first publication fails.
class PublicationFaultMachine {
  enum Op { PoolSaturation, ScopeSaturation, BatchPrefix, Count };
  dagflow::Pool pool_{[] { dagflow::Config c; c.threads = 2; return c; }()};
  unsigned operations_{};
 public:
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)), rng.bounded(6),
            rng.bounded(6)};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 6 && c.b < 6;
  }
  void apply(const Command& c) {
    fault::ResetOnExit reset;
    if (c.op == PoolSaturation) {
      fault::force_pool_admission_overflow = true;
      require(throws_as<std::overflow_error>(
                  [&] { (void)pool_.submit([]{}); }),
              "saturated pool admission rejects before publication");
      fault::force_pool_admission_overflow = false;
      pool_.wait_and_rethrow(pool_.submit([]{}));
    } else if (c.op == ScopeSaturation) {
      dagflow::TaskScope scope(pool_);
      fault::force_scope_admission_overflow = true;
      require(throws_as<std::overflow_error>(
                  [&] { (void)scope.spawn([]{}); }),
              "saturated scope reservation rejects without a credit");
      fault::force_scope_admission_overflow = false;
      std::atomic<int> calls{0};
      require(scope.spawn([&] { calls.fetch_add(1); }),
              "scope accepts a following healthy publisher");
      scope.join();
      require(calls.load() == 1, "scope join observes exactly one body");
    } else {
      const auto n = unsigned(c.a + 2);
      const auto accepted_prefix = unsigned(c.b % n);
      std::atomic<unsigned> executed{0};
      std::vector<std::function<void()>> tasks;
      std::vector<std::weak_ptr<int>> owners;
      tasks.reserve(n);
      owners.reserve(n);
      for (unsigned i = 0; i < n; ++i) {
        auto guard = std::make_shared<int>(static_cast<int>(i));
        owners.push_back(guard);
        tasks.emplace_back([guard = std::move(guard), &executed] {
          executed.fetch_add(1, std::memory_order_relaxed);
        });
      }
      fault::fail_external_publication_at = static_cast<int>(accepted_prefix);
      require(throws_as<fault::injected_publication_error>(
                  [&] { pool_.submit_batch_detached(std::span{tasks}); }),
              "exact configured batch publication fails");
      fault::fail_external_publication_at = -1;
      // This barrier also includes user capture destruction, not just calls.
      pool_.wait_idle();
      tasks.clear();
      require(executed.load() == accepted_prefix,
              "only admitted prefix runs; tail is destroyed unexecuted");
      for (const auto& owner : owners)
        require(owner.expired(),
                "accepted and rejected batch captures retire at idle");
      pool_.wait_and_rethrow(pool_.submit([]{}));
    }
    ++operations_;
  }
  void invariant() const { require(operations_ < 10000, "bounded publication history"); }
  void finish() { pool_.wait_idle(); }
};

// A configurable small limit exercises the same checks as UINT32_MAX without
// allocating four billion definitions. The duplicate seal validation is
// deliberately reached only via bypassing the early check in THIS test target.
class GraphLimitMachine {
  enum Op { NodeLimit, EdgeLimit, SealValidation, Count };
  dagflow::Pool pool_{[] { dagflow::Config c; c.threads = 1; return c; }()};
  std::atomic<unsigned> calls_{0};
  unsigned expected_{};
 public:
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)), rng.bounded(3), 0};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 3 && c.b == 0;
  }
  void apply(const Command& c) {
    fault::ResetOnExit reset;
    dagflow::TaskGraph graph(pool_);
    if (c.op == NodeLimit) {
      const auto n = unsigned(c.a + 1);
      fault::graph_node_limit = n;
      for (unsigned i = 0; i < n; ++i)
        graph.emplace([this] { calls_.fetch_add(1); });
      require(throws_as<std::length_error>(
                  [&] { graph.emplace([]{}); }),
              "node limit rejects append without modifying builder");
      fault::graph_node_limit = 0;
      require(graph.seal(), "limit rejection preserves original DAG");
      pool_.wait_and_rethrow(graph.run());
      expected_ += n;
    } else if (c.op == EdgeLimit) {
      const auto a = graph.emplace([this] { calls_.fetch_add(1); });
      const auto b = graph.emplace([this] { calls_.fetch_add(1); });
      fault::graph_edge_limit = 1;
      graph.add_edge(a, b);
      require(throws_as<std::length_error>(
                  [&] { graph.add_edge(a, b); }),
              "edge limit rejects append without altering predecessors");
      fault::graph_edge_limit = 0;
      require(graph.seal(), "limit rejection preserves original edge");
      pool_.wait_and_rethrow(graph.run());
      expected_ += 2;
    } else {
      dagflow::TaskGraph::NodeOptions invalid;
      invalid.capacity = 0;
      invalid.overflow = dagflow::TaskGraph::Overflow::Block;
      fault::bypass_graph_early_validation = true;
      graph.emplace([this] { calls_.fetch_add(1); }, invalid);
      fault::bypass_graph_early_validation = false;
      require(throws_as<std::invalid_argument>(
                  [&] { (void)graph.seal(); }),
              "seal independently enforces zero-capacity Block validation");
    }
  }
  void invariant() const {
    require(calls_.load() == expected_, "graph rejects never run tasks");
  }
  void finish() { pool_.wait_idle(); }
};

// The production retire path is intentionally fail-fast. A child process
// verifies termination without aborting the entire PBT executable. Run before
// starting any worker thread; exit(77) also flushes GCC gcov child counters.
void retire_overflow_death_contract() {
#if defined(__linux__) && !defined(__SANITIZE_THREAD__)
  const pid_t child = fork();
  require(child >= 0, "fork for isolated termination check succeeds");
  if (child == 0) {
    std::set_terminate([] { std::exit(77); });
    dagflow::detail::IdleAccounting accounting(1);
    fault::force_worker_retire_overflow = true;
    accounting.retire(0);
    std::_Exit(78); // terminate must never return.
  }
  int status = 0;
  require(waitpid(child, &status, 0) == child, "child reaped");
  require(WIFEXITED(status) && WEXITSTATUS(status) == 77,
          "retire saturation triggers std::terminate");
#endif
}
}  // namespace

int main() {
  retire_overflow_death_contract();
  run<AccountingFaultMachine>("accounting exceptional wait and overflow", 12, 24);
  run<ParkingFaultMachine>("parking condition wait cleanup", 12, 24);
  run<PublicationFaultMachine>("batch prefix publication and admission", 12, 24);
  run<GraphLimitMachine>("graph limit and seal defense", 12, 24);
}
