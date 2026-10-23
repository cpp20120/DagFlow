#include <string_view>

#include "idle_accounting_under_test.hpp"

// One accepted packet. A successful snapshot must acquire its physical
// cleanup, even when the observer does not take the notifier's mutex path.
struct Visibility : rl::test_suite<Visibility, 2> {
  IdleAccounting accounting{1};
  rl::var<int> cleanup;
  void before() {
    cleanup($) = 0;
    accounting.publish_worker(0);
  }
  void thread(unsigned actor) {
    if (actor == 0) {
      cleanup($) = 42;
      accounting.retire(0);
    } else {
      rl::lock_guard<rl::mutex> lock(accounting.mutex_);
      if (accounting.idle_locked()) RL_ASSERT(cleanup($) == 42);
    }
  }
  void after() { RL_ASSERT(accounting.idle_locked()); }
};

// Retirement is per executor, with no shared RMW to collect other workers'
// effects. A successful scan must acquire both lanes independently.
struct Aggregate : rl::test_suite<Aggregate, 3> {
  IdleAccounting accounting{2};
  rl::var<int> cleanup[2];
  void before() {
    for (unsigned worker = 0; worker != 2; ++worker) {
      cleanup[worker]($) = 0;
      accounting.publish_worker(worker);
    }
  }
  void thread(unsigned actor) {
    if (actor < 2) {
      cleanup[actor]($) = 42;
      accounting.retire(actor);
    } else {
      rl::lock_guard<rl::mutex> lock(accounting.mutex_);
      if (accounting.idle_locked()) {
        RL_ASSERT(cleanup[0]($) == 42);
        RL_ASSERT(cleanup[1]($) == 42);
      }
    }
  }
  void after() { RL_ASSERT(accounting.idle_locked()); }
};

// A parent remains live until it publishes a child; the child stays live.
// Therefore there is no idle point at any time in this fixture. With an
// external root, publication and retirement use different kinds of lanes.
template <bool ExternalRoot>
struct Handoff : rl::test_suite<Handoff<ExternalRoot>, 2> {
  IdleAccounting accounting{ExternalRoot ? 2u : 1u};
  static constexpr unsigned executor = ExternalRoot ? 1 : 0;
  void before() {
    if constexpr (ExternalRoot) {
      accounting.producers_ = std::make_unique<IdleAccounting::Producer>(1);
      accounting.publish_external();
    } else {
      accounting.publish_worker(0);
    }
  }
  void thread(unsigned actor) {
    if (actor == 0) {
      accounting.publish_worker(executor);
      accounting.retire(executor);
    } else {
      rl::lock_guard<rl::mutex> lock(accounting.mutex_);
      RL_ASSERT(!accounting.idle_locked());
    }
  }
  void after() { RL_ASSERT(!accounting.idle_locked()); }
};

// Actual untimed wait versus the final packet's retirement and subsequent
// no-work probe. The worker terminates after notifying: a missed notification
// cannot be repaired by another task or another idle probe in this fixture.
struct Wait : rl::test_suite<Wait, 2> {
  IdleAccounting accounting{1};
  rl::var<int> cleanup;
  void before() {
    cleanup($) = 0;
    accounting.publish_worker(0);
  }
  void thread(unsigned actor) {
    if (actor == 0) {
      cleanup($) = 42;
      accounting.retire(0);
      accounting.worker_idle();
    } else {
      accounting.wait();
      RL_ASSERT(cleanup($) == 42);
    }
  }
  void after() {
    RL_ASSERT(accounting.waiters_.load(rl::memory_order_relaxed) == 0);
    RL_ASSERT(accounting.idle_locked());
  }
};

template <unsigned Rounds>
struct MultipleWaiters : rl::test_suite<MultipleWaiters<Rounds>, 3> {
  static constexpr unsigned context_bound = 2;
  IdleAccounting accounting{1};
  rl::var<int> cleanup;
  void before() {
    cleanup($) = 0;
    accounting.publish_worker(0);
  }
  void thread(unsigned actor) {
    if (actor == 0) {
      cleanup($) = 42;
      accounting.retire(0);
      accounting.worker_idle();
    } else {
      for (unsigned round = 0; round != Rounds; ++round) {
        accounting.wait();
        RL_ASSERT(cleanup($) == 42);
      }
    }
  }
  void after() {
    RL_ASSERT(accounting.waiters_.load(rl::memory_order_relaxed) == 0);
    RL_ASSERT(accounting.idle_locked());
  }
};

int main(int argc, char** argv) {
  if (argc != 2) return 2;
  const std::string_view suite(argv[1]);
  if (suite == "visibility") return check_model<Visibility>(argv[1]);
  if (suite == "aggregate") return check_model<Aggregate>(argv[1]);
  if (suite == "handoff") return check_model<Handoff<false>>(argv[1]);
  if (suite == "external-handoff") return check_model<Handoff<true>>(argv[1]);
  if (suite == "wait") return check_model<Wait>(argv[1]);
  if (suite == "multiple-waiters")
    return check_model<MultipleWaiters<1>>(argv[1]);
  if (suite == "repeated-waiters")
    return check_model<MultipleWaiters<2>>(argv[1]);
  return 2;
}
