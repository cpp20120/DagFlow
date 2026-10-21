#include <array>
#include <dagflow/detail/scheduler.hpp>
#include "support.hpp"

// Concrete, deterministic scheduler histories. Each worker ID has a unique
// owner and no races are required to exercise periodic ingress and local
// stealing branches; the separate contention test tests their concurrency.
int main() {
  using dagflow::Priority;
  using dagflow::detail::Scheduler;
  using dagflow::detail::ScheduledTask;
  // Exercise the negative outcome of periodic high-priority polling as well
  // as its success path: the old test reached the 32nd probe only after a
  // task had arrived, leaving the empty-ingress transition unobserved.
  {
    Scheduler empty(2, 1, 4);
    for (unsigned i = 0; i < 128; ++i) {
      bool recruit = true;
      CHECK(empty.try_acquire(0, recruit) == nullptr);
      CHECK(!recruit);
    }
  }
  for (auto priority : {Priority::High, Priority::Normal}) {
    Scheduler scheduler(2, 1, 4);
    ScheduledTask high, normal;
    high.prio = Priority::High;
    normal.prio = Priority::Normal;
    for (unsigned i = 0; i < 31; ++i) CHECK(!scheduler.try_acquire(0));
    auto* expected = priority == Priority::High ? &high : &normal;
    expected->prio = priority;
    CHECK(scheduler.try_submit_external(0, expected));
    bool recruit = false;
    CHECK(scheduler.try_acquire(0, recruit) == expected);
    CHECK(recruit);
    CHECK(scheduler.try_acquire(0) == nullptr);
  }
  for (unsigned count : {1u, 2u, 3u, 4u, 8u}) {
    Scheduler scheduler(2, 1, 4);
    std::array<ScheduledTask, 8> packets;
    for (unsigned i = 0; i < count; ++i) {
      packets[i].prio = (i & 1) ? Priority::Normal : Priority::High;
      CHECK(scheduler.try_submit_local(1, &packets[i]));
    }
    std::array<unsigned, 8> hits{};
    bool saw_recruit = false;
    for (unsigned i = 0; i < count; ++i) {
      bool recruit = false;
      auto* task = scheduler.try_acquire(0, recruit);
      CHECK(task);
      // Steal may batch siblings into the thief's local deque; those
      // subsequent local pops correctly report recruit=false.
      saw_recruit |= recruit;
      const auto offset = task - packets.data();
      CHECK(offset >= 0 && static_cast<unsigned>(offset) < count);
      ++hits[static_cast<unsigned>(offset)];
    }
    CHECK(saw_recruit);
    for (unsigned i = 0; i < count; ++i) CHECK(hits[i] == 1);
    CHECK(!scheduler.try_acquire(0));
  }
}
