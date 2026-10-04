#include "dagflow/detail/scheduler.hpp"

#include <algorithm>
#include <exception>
#include <stdexcept>

namespace {
// Constants for the small SplitMix64-style generator used by each worker.
// The per-worker stream is deterministic, but starts at a different point.
constexpr uint64_t worker_random_seed = 0xa0761d6478bd642fULL;
constexpr uint64_t worker_random_stream_stride = 0xd1b54a32d192ed03ULL;
constexpr uint64_t splitmix_increment = 0x9e3779b97f4a7c15ULL;
constexpr uint64_t splitmix_multiplier_1 = 0xbf58476d1ce4e5b9ULL;
constexpr uint64_t splitmix_multiplier_2 = 0x94d049bb133111ebULL;
constexpr uint32_t splitmix_shift_1 = 30;
constexpr uint32_t splitmix_shift_2 = 27;
constexpr uint32_t splitmix_shift_3 = 31;
constexpr uint32_t steal_batch_limit = 4;
constexpr uint32_t external_poll_interval = 32;
}  // namespace

namespace dagflow::detail {
Scheduler::Scheduler(uint32_t workers, uint32_t shards, uint32_t central_batch,
                     std::span<const uint32_t> worker_shards)
    : worker_count_(std::max(DAGFLOW_MIN_WORKER_COUNT, workers)),
      shard_count_(shards ? shards : worker_count_),
      central_batch_(std::max(DAGFLOW_MIN_WORKER_COUNT, central_batch)) {
  if (!worker_shards.empty() && worker_shards.size() != worker_count_)
    throw std::invalid_argument(
        "worker_shards must contain one shard per worker");
  for (auto shard : worker_shards)
    if (shard >= shard_count_)
      throw std::invalid_argument("worker_shards contains an invalid shard");
  locals_ = make_owned_array<Local>(worker_count_);
  shards_ = make_owned_array<Shard>(shard_count_);
  members_ = make_owned_array<uint32_t>(worker_count_);
  for (uint32_t id = 0; id < worker_count_; ++id) {
    auto& local = locals_[id];
    local.home_shard =
        worker_shards.empty()
            ? static_cast<uint32_t>(uint64_t{id} * shard_count_ / worker_count_)
            : worker_shards[id];
    local.next_external_shard = local.home_shard;
    local.random_state =
        worker_random_seed ^
        (worker_random_stream_stride * (uint64_t{id} + 1));
    ++shards_[local.home_shard].member_count;
  }
  uint32_t member_offset = 0;
  for (uint32_t shard_index = 0; shard_index < shard_count_; ++shard_index) {
    auto& shard = shards_[shard_index];
    shard.member_offset = member_offset;
    member_offset += shard.member_count;
    shard.member_count =
        0;  // Reuse as the CSR fill cursor; no scratch allocation.
  }
  for (uint32_t id = 0; id < worker_count_; ++id) {
    auto& shard = shards_[locals_[id].home_shard];
    members_[shard.member_offset + shard.member_count++] = id;
  }
}

uint32_t Scheduler::random_below(Local& local, uint32_t count) noexcept {
  // SplitMix64: one owner-only word instead of a multi-kilobyte mt19937 state.
  auto value = (local.random_state += splitmix_increment);
  value = (value ^ (value >> splitmix_shift_1)) * splitmix_multiplier_1;
  value = (value ^ (value >> splitmix_shift_2)) * splitmix_multiplier_2;
  return static_cast<uint32_t>((value ^ (value >> splitmix_shift_3)) % count);
}

bool Scheduler::try_submit_local(uint32_t id, ScheduledTask* task) {
  auto& local = locals_[id];
  auto& queue = task->prio == Priority::High ? local.high : local.normal;
  if (queue.try_push(task)) {
    runtime_count(RuntimeEvent::local_push);
    return true;
  }
  runtime_count(RuntimeEvent::local_spill);
  return try_submit_external(local.home_shard, task);
}
uint32_t Scheduler::select_shard(std::optional<uint32_t> affinity) {
  if (affinity) return home_shard(*affinity % worker_count_);
  return next_shard_.fetch_add(1, std::memory_order_relaxed) % shard_count_;
}
bool Scheduler::try_submit_external(uint32_t shard, ScheduledTask* task) {
  auto& queues = shards_[shard];
  const bool accepted = (task->prio == Priority::High ? queues.high : queues.normal).try_push(task);
  runtime_count(accepted ? RuntimeEvent::ingress_push : RuntimeEvent::ingress_full);
  return accepted;
}

void Scheduler::OverflowQueue::push(ScheduledTask* task) noexcept {
  // Ownership transfers under the mutex; no new storage is needed after task
  // accounting has committed. A vector/list allocation here could strand an
  // accepted task on bad_alloc. Running it inline instead grows the submit stack.
  std::lock_guard lock(mutex);
  task->overflow_next = nullptr;
  if (tail) tail->overflow_next = task;
  else head = task;
  tail = task;
  nonempty.store(true, std::memory_order_release);
}

ScheduledTask* Scheduler::OverflowQueue::pop(LocalQueue* destination,
                                            uint32_t batch_limit) noexcept {
  // Empty shards pay only a probe, not a lock. The ordinary publish/wake and
  // prepare/final-scan/park handshake covers a concurrent empty -> nonempty.
  if (!nonempty.load(std::memory_order_acquire)) return nullptr;
  std::lock_guard lock(mutex);
  auto* task = head;
  if (!task) return nullptr;  // Another consumer won after our probe.
  head = task->overflow_next;
  task->overflow_next = nullptr;
  // One lock can detach a bounded batch. Publish all siblings before returning
  // its first task: hiding them in an executor-private list would deadlock a
  // nested wait for one of those siblings. After try_push a thief may immediately
  // destroy the packet, so read/unlink everything before handing it over.
  const auto limit = destination
      ? std::min<std::size_t>(batch_limit, destination->free_capacity() + 1)
      : 1;
  std::size_t taken = 1;
  while (head && taken < limit) {
    auto* next = head;
    head = next->overflow_next;
    next->overflow_next = nullptr;
    if (!destination->try_push(next)) std::terminate();
    ++taken;
  }
  if (!head) {
    tail = nullptr;
    nonempty.store(false, std::memory_order_release);
  }
  runtime_count(RuntimeEvent::overflow_acquire, taken);
  return task;
}

void Scheduler::submit_overflow(uint32_t shard, ScheduledTask* task) noexcept {
  auto& queues = shards_[shard];
  (task->prio == Priority::High ? queues.overflow_high : queues.overflow_normal)
      .push(task);
  runtime_count(RuntimeEvent::overflow_push);
}

ScheduledTask* Scheduler::pop_shared(uint32_t id, uint32_t shard,
                                    Priority priority, bool batch,
                                    bool& prefer_overflow) {
  auto& local = locals_[id];
  auto& queues = shards_[shard];
  auto& overflow = priority == Priority::High ? queues.overflow_high
                                             : queues.overflow_normal;
  auto& central = priority == Priority::High ? queues.high : queues.normal;
  auto& destination = priority == Priority::High ? local.high : local.normal;
  // Alternate sources when both stay busy. Always preferring ingress would
  // strand overflow children under sustained external traffic; always preferring
  // overflow would undo the periodic ingress starvation budget.
  const auto take_overflow = [&]() -> ScheduledTask* {
    if (auto* task = overflow.pop(batch ? &destination : nullptr, central_batch_)) {
      prefer_overflow = false;
      return task;
    }
    return nullptr;
  };
  if (prefer_overflow)
    if (auto* task = take_overflow()) return task;
  ScheduledTask* task = nullptr;
  if (central.try_pop(task)) {
    prefer_overflow = true;
    runtime_count(RuntimeEvent::drain_tasks);
    if (shard != local.home_shard) runtime_count(RuntimeEvent::remote_drain_tasks);
    if (batch) {
      const auto limit =
          std::min<std::size_t>(central_batch_, destination.free_capacity() + 1);
      ScheduledTask* next = nullptr;
      for (std::size_t taken = 1; taken < limit && central.try_pop(next); ++taken) {
        runtime_count(RuntimeEvent::drain_tasks);
        if (shard != local.home_shard)
          runtime_count(RuntimeEvent::remote_drain_tasks);
        if (!destination.try_push(next)) std::terminate();
      }
    }
    return task;
  }
  if (!prefer_overflow) return take_overflow();
  return nullptr;
}

ScheduledTask* Scheduler::drain(uint32_t id, uint32_t shard,
                                Priority priority) {
  // Publish the entire batch before user code can help/wait on its siblings.
  return pop_shared(id, shard, priority, true, locals_[id].prefer_overflow);
}

ScheduledTask* Scheduler::steal(uint32_t id, uint32_t shard) {
  const auto group = members(shard);
  if (group.empty()) return nullptr;
  auto& local = locals_[id];
  const auto start = random_below(local, static_cast<uint32_t>(group.size()));
  for (auto priority : {Priority::High, Priority::Normal}) {
    auto& destination = priority == Priority::High ? local.high : local.normal;
    for (std::size_t offset = 0; offset < group.size(); ++offset) {
      const auto victim = group[(start + offset) % group.size()];
      if (victim == id) continue;
      auto& queue = priority == Priority::High ? locals_[victim].high
                                               : locals_[victim].normal;
      constexpr auto batch_size = std::min<std::size_t>(
          steal_batch_limit, LocalQueue::capacity() + 1);
      ScheduledTask* tasks[batch_size];
      uint32_t taken = 0;
      while (taken < batch_size && queue.try_steal(tasks[taken])) ++taken;
      if (!taken) continue;
      for (uint32_t i = 1; i < taken; ++i)
        if (!destination.try_push(tasks[i])) std::terminate();
          return tasks[0];
    }
  }
  return nullptr;
}

ScheduledTask* Scheduler::try_acquire(uint32_t id, bool& recruit) {
  recruit = false;
  auto& local = locals_[id];
  ScheduledTask* task = nullptr;
  if (++local.acquisitions_since_external >= external_poll_interval) {
    local.acquisitions_since_external = 0;
    if (auto* external = poll_external(id, Priority::High)) {
      recruit = true;
      return external;
    }
    if (local.high.try_pop(task)) {
      runtime_count(RuntimeEvent::local_acquire);
      return task;
    }
    if (auto* external = poll_external(id, Priority::Normal)) {
      recruit = true;
      return external;
    }
  }
  if (local.high.try_pop(task) || local.normal.try_pop(task)) {
    runtime_count(RuntimeEvent::local_acquire);
    return task;
  }
  const auto home = local.home_shard;
  for (auto priority : {Priority::High, Priority::Normal})
    if (auto* ready = drain(id, home, priority)) {
      recruit = true;
      return ready;
    }
  if (auto* ready = steal(id, home)) {
    recruit = true;
    return ready;
  }
  const auto start = random_below(local, shard_count_);
  for (uint32_t offset = 0; offset < shard_count_; ++offset) {
    const auto remote =
        static_cast<uint32_t>((uint64_t{start} + offset) % shard_count_);
    if (remote == home) continue;
    for (auto priority : {Priority::High, Priority::Normal})
      if (auto* ready = drain(id, remote, priority)) {
        recruit = true;
        return ready;
      }
    if (auto* ready = steal(id, remote)) {
      recruit = true;
      return ready;
    }
  }
  return nullptr;
}

ScheduledTask* Scheduler::poll_external(uint32_t id, Priority priority) {
  auto& local = locals_[id];
  for (uint32_t offset = 0; offset < shard_count_; ++offset) {
    const auto shard = static_cast<uint32_t>(
        (uint64_t{local.next_external_shard} + offset) % shard_count_);
    // Hold source preference for a complete shard rotation. Toggling after
    // every successful probe can permanently select ingress in shard A and
    // overflow in shard B, starving A's overflow under sustained traffic.
    // Drain preference is separate, so intervening local/remote drains cannot
    // keep resetting the periodic service decision either.
    bool preference = local.external_prefer_overflow;
    if (auto* task = pop_shared(id, shard, priority, false, preference)) {
      const auto next = (shard + 1) % shard_count_;
      if (next <= local.next_external_shard)
        local.external_prefer_overflow = !local.external_prefer_overflow;
      local.next_external_shard = next;
      return task;
    }
  }
  return nullptr;
}
bool Scheduler::has_local_work(uint32_t id) const {
  return !locals_[id].high.empty() || !locals_[id].normal.empty();
}
}  // namespace dagflow::detail
