#include <cstdio>

#include "dagflow/detail/scheduler.hpp"
#include "support.hpp"

using dagflow::Priority;
using dagflow::detail::ScheduledTask;
using dagflow::detail::Scheduler;

// Keep replenishing a local deque: external work must progress without
// requiring it to become empty. No timing, sleeping, or OS scheduling is
// involved.
void external_progress(Priority priority, unsigned shard) {
  Scheduler scheduler(1, 3, 32);
  ScheduledTask local, external;
  local.prio = external.prio = priority;
  CHECK(scheduler.try_submit_local(0, &local));
  CHECK(scheduler.try_submit_external(shard, &external));
  bool acquired = false;
  for (unsigned i = 0; i < 64; ++i) {
    auto* task = scheduler.try_acquire(0);
    CHECK(task != nullptr);
    if (task == &external) {
      acquired = true;
      break;
    }
    CHECK(task == &local);
    CHECK(scheduler.try_submit_local(0, &local));
  }
  CHECK(acquired);
  CHECK(scheduler.try_acquire(0) == &local);
  CHECK(scheduler.try_acquire(0) == nullptr);
}

void shard_rotation() {
  Scheduler scheduler(1, 3, 32);
  ScheduledTask local, external[3];
  bool seen[3]{};
  unsigned acquired = 0;
  CHECK(scheduler.try_submit_local(0, &local));
  for (unsigned i = 0; i < 3; ++i)
    CHECK(scheduler.try_submit_external(i, &external[i]));
  for (unsigned i = 0; i < 160 && acquired != 3; ++i) {
    auto* task = scheduler.try_acquire(0);
    CHECK(task != nullptr);
    if (task == &local) {
      CHECK(scheduler.try_submit_local(0, &local));
      continue;
    }
    const auto shard = static_cast<unsigned>(task - external);
    CHECK(shard < 3);
    if (!seen[shard]) {
      seen[shard] = true;
      ++acquired;
    }
    CHECK(scheduler.try_submit_external(shard, task));
  }
  CHECK(acquired == 3);
}

void high_priority_probe() {
  Scheduler scheduler(1, 2, 32);
  ScheduledTask local, normal, high;
  high.prio = Priority::High;
  CHECK(scheduler.try_submit_local(0, &local));
  CHECK(scheduler.try_submit_external(0, &normal));
  CHECK(scheduler.try_submit_external(1, &high));
  bool acquired = false;
  for (unsigned i = 0; i < 64; ++i) {
    auto* task = scheduler.try_acquire(0);
    if (task == &high) {
      acquired = true;
      break;
    }
    CHECK(task == &local);  // Normal external work cannot precede high probe.
    CHECK(scheduler.try_submit_local(0, &local));
  }
  CHECK(acquired);
}

int main() {
  for (auto priority : {Priority::High, Priority::Normal})
    for (unsigned shard = 0; shard < 3; ++shard)
      external_progress(priority, shard);
  shard_rotation();
  high_priority_probe();
  std::puts("scheduler fairness tests passed");
}
