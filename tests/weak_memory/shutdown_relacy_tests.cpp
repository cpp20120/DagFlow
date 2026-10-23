#include <string_view>

#include "shutdown_under_test.hpp"

// A previously admitted publisher exits without a packet (e.g. rollback).
// Shutdown must still acquire all its effects before stop/wake/join.
struct Admission : rl::test_suite<Admission, 2> {
  IdleAccounting accounting{1};
  ModelParking parking;
  Pool pool{accounting, parking};
  rl::var<int> publisher_cleanup;
  void before() {
    publisher_cleanup($) = 0;
    pool.enter_publication();
    parking.context = this;
    parking.inspect = [](void* context) {
      auto& self = *static_cast<Admission*>(context);
      RL_ASSERT(self.publisher_cleanup($) == 42);
    };
  }
  void thread(unsigned actor) {
    if (actor == 0) {
      publisher_cleanup($) = 42;
      pool.leave_publication();
    } else {
      pool.shutdown();
      RL_ASSERT(pool.closed());
      RL_ASSERT(pool.stop_.load(rl::memory_order_relaxed));
    }
  }
};

// Reserve and close race from an open pool. Accepted publication holds off
// stop until the guard exits; a rejected call never takes a reservation.
struct CloseRace : rl::test_suite<CloseRace, 2> {
  IdleAccounting accounting{1};
  ModelParking parking;
  Pool pool{accounting, parking};
  rl::var<bool> accepted, rejected;
  void before() {
    accepted($) = false;
    rejected($) = false;
  }
  void thread(unsigned actor) {
    if (actor == 0) {
      try {
        Pool::PublicationGuard guard(pool);
        accepted($) = true;
        RL_ASSERT(!pool.stop_.load(rl::memory_order_acquire));
      } catch (const std::logic_error&) {
        rejected($) = true;
      }
    } else {
      pool.shutdown();
    }
  }
  void after() {
    RL_ASSERT(accepted($) != rejected($));
    RL_ASSERT(pool.admission_.load(rl::memory_order_relaxed) ==
              Pool::admission_closed_bit);
    RL_ASSERT(pool.stop_.load(rl::memory_order_relaxed));
  }
};

// One root has been dispatched before the race. Its worker can publish a
// child even after close, and both physical epilogues precede shutdown stop.
struct DescendantDrain : rl::test_suite<DescendantDrain, 2> {
  static constexpr unsigned context_bound = 2;
  IdleAccounting accounting{1};
  ModelParking parking;
  Pool pool{accounting, parking};
  rl::var<int> root_cleanup, child_cleanup;
  void before() {
    root_cleanup($) = child_cleanup($) = 0;
    accounting.producers_ = std::make_unique<IdleAccounting::Producer>(1);
    {
      Pool::PublicationGuard guard(pool);
      accounting.publish_external();
    }
    parking.context = this;
    parking.inspect = [](void* context) {
      auto& self = *static_cast<DescendantDrain*>(context);
      RL_ASSERT(self.root_cleanup($) == 42);
      RL_ASSERT(self.child_cleanup($) == 43);
    };
  }
  void thread(unsigned actor) {
    if (actor == 0) {
      RL_ASSERT(!pool.stop_.load(rl::memory_order_acquire));
      accounting.publish_worker(0);
      root_cleanup($) = 42;
      accounting.retire(0);
      RL_ASSERT(!pool.stop_.load(rl::memory_order_acquire));
      child_cleanup($) = 43;
      accounting.retire(0);
      accounting.worker_idle();
    } else {
      pool.shutdown();
    }
  }
  void after() {
    RL_ASSERT(accounting.waiters_.load(rl::memory_order_relaxed) == 0);
    RL_ASSERT(accounting.idle_locked());
  }
};

// Both barriers in one execution: an admitted publisher has not yet counted
// its packet when the three actors start. A release/acquire mailbox represents
// the queue transfer; there is one packet and no queue arbitration/reuse.
struct PublicationDrain : rl::test_suite<PublicationDrain, 3> {
  static constexpr unsigned context_bound = 1;
  IdleAccounting accounting{1};
  ModelParking parking;
  Pool pool{accounting, parking};
  ModelAtomic<bool> dispatched{false};
  rl::var<int> payload, cleanup;
  void before() {
    payload($) = cleanup($) = 0;
    accounting.producers_ = std::make_unique<IdleAccounting::Producer>(1);
    pool.enter_publication();
    parking.context = this;
    parking.inspect = [](void* context) {
      auto& self = *static_cast<PublicationDrain*>(context);
      RL_ASSERT(self.cleanup($) == 42);
    };
  }
  void thread(unsigned actor) {
    if (actor == 0) {
      payload($) = 42;
      accounting.publish_external();
      dispatched.store(true, rl::memory_order_release);
      dispatched.notify_one();
      pool.leave_publication();
    } else if (actor == 1) {
      dispatched.wait(false, rl::memory_order_acquire);
      RL_ASSERT(!pool.stop_.load(rl::memory_order_acquire));
      cleanup($) = payload($);
      accounting.retire(0);
      accounting.worker_idle();
    } else {
      pool.shutdown();
    }
  }
  void after() {
    RL_ASSERT(accounting.idle_locked());
    RL_ASSERT(pool.admission_.load(rl::memory_order_relaxed) ==
              Pool::admission_closed_bit);
  }
};

// Ordinary wait_idle and shutdown both use the accounting CV. The final
// worker's single notification must let both external callers complete.
struct ShutdownWithWaiter : rl::test_suite<ShutdownWithWaiter, 3> {
  static constexpr unsigned context_bound = 2;
  IdleAccounting accounting{1};
  ModelParking parking;
  Pool pool{accounting, parking};
  rl::var<int> cleanup;
  void before() {
    cleanup($) = 0;
    accounting.producers_ = std::make_unique<IdleAccounting::Producer>(1);
    {
      Pool::PublicationGuard guard(pool);
      accounting.publish_external();
    }
    parking.context = this;
    parking.inspect = [](void* context) {
      auto& self = *static_cast<ShutdownWithWaiter*>(context);
      RL_ASSERT(self.cleanup($) == 42);
    };
  }
  void thread(unsigned actor) {
    if (actor == 0) {
      cleanup($) = 42;
      accounting.retire(0);
      accounting.worker_idle();
    } else if (actor == 1) {
      pool.wait_idle();
      RL_ASSERT(cleanup($) == 42);
    } else {
      pool.shutdown();
    }
  }
  void after() {
    RL_ASSERT(accounting.waiters_.load(rl::memory_order_relaxed) == 0);
    RL_ASSERT(accounting.idle_locked());
    RL_ASSERT(pool.stop_.load(rl::memory_order_relaxed));
  }
};

int main(int argc, char** argv) {
  if (argc != 2) return 2;
  const std::string_view suite(argv[1]);
  if (suite == "admission") return check_model<Admission>(argv[1]);
  if (suite == "close-race") return check_model<CloseRace>(argv[1]);
  if (suite == "descendant-drain") return check_model<DescendantDrain>(argv[1]);
  if (suite == "publication-drain")
    return check_model<PublicationDrain>(argv[1]);
  if (suite == "shutdown-with-waiter")
    return check_model<ShutdownWithWaiter>(argv[1]);
  return 2;
}
