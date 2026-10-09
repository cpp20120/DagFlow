#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <optional>
#include <span>

#include <dagflow/detail/chase_lev_deque.hpp>
#include <dagflow/detail/ring_mpmc.hpp>
#include <dagflow/detail/runtime_memory.hpp>
#include <dagflow/detail/runtime_diagnostics.hpp>
#include <dagflow/thread_pool.hpp>
namespace dagflow::detail {
/// Immutable placement domains plus owner queues and shared ingress. Every
/// worker has one home shard; shards may have no resident worker. ParkingLot
/// uses this same membership map. CPU/NUMA placement remains a separate
/// concern.
class Scheduler {
 public:
  Scheduler(uint32_t workers, uint32_t shards, uint32_t central_batch,
            std::span<const uint32_t> worker_shards = {});
  bool try_submit_local(uint32_t id, ScheduledTask* task);
  /// Affinity names a worker, mapped to its home shard. Oversized hints wrap
  /// into the worker domain for compatibility; they never index arrays raw.
  uint32_t select_shard(std::optional<uint32_t> affinity);
  bool try_submit_external(uint32_t shard, ScheduledTask* task);
  /// Worker-only escape from full bounded queues: no allocation, no execution,
  /// no wait for capacity. All workers (including helpers) can acquire it.
  void submit_overflow(uint32_t shard, ScheduledTask* task) noexcept;
  /// Owner only. recruit reports work obtained from ingress or another worker.
  /// The executor must relay a wake before user code, even for one task: other
  /// queues can still contain work after the selected source becomes empty.
  ScheduledTask* try_acquire(uint32_t id, bool& recruit);
  ScheduledTask* try_acquire(uint32_t id) {
    bool ignored;
    return try_acquire(id, ignored);
  }
  [[nodiscard]] bool has_local_work(uint32_t id) const;
  [[nodiscard]] uint32_t worker_count() const noexcept { return worker_count_; }
  [[nodiscard]] uint32_t shard_count() const noexcept { return shard_count_; }
  [[nodiscard]] std::span<const uint32_t> worker_order() const noexcept {
    return {members_.get(), worker_count_};
  }
  [[nodiscard]] uint32_t home_shard(uint32_t worker) const noexcept {
    return locals_[worker].home_shard;
  }
  [[nodiscard]] std::span<const uint32_t> members(
      uint32_t shard) const noexcept {
    const auto& group = shards_[shard];
    return {members_.get() + group.member_offset, group.member_count};
  }

 private:
  using LocalQueue = chase_lev_deque<ScheduledTask*>;
  using CentralQueue =
      ring_mpmc<ScheduledTask*, DAGFLOW_CENTRAL_QUEUE_CAPACITY>;
  struct Local {
    LocalQueue high;
    LocalQueue normal;
    uint64_t random_state{};
    uint32_t home_shard{};
    uint32_t acquisitions_since_external{};
    uint32_t next_external_shard{};
    bool prefer_overflow{true};
    bool external_prefer_overflow{true};
  };
  struct OverflowQueue {
    void push(ScheduledTask* task) noexcept;
    ScheduledTask* pop(LocalQueue* destination = nullptr,
                       uint32_t batch_limit = 1) noexcept;
    // Only the probe is atomic. Links are accessed under mutex and removed
    // before executing anything; an executor may immediately free the packet.
    std::atomic<bool> nonempty{false};
    std::mutex mutex;
    ScheduledTask* head{};
    ScheduledTask* tail{};
  };
  struct Shard {
    CentralQueue high;
    CentralQueue normal;
    OverflowQueue overflow_high;
    OverflowQueue overflow_normal;
    uint32_t member_offset{};
    uint32_t member_count{};
  };
  static uint32_t random_below(Local& local, uint32_t count) noexcept;
  ScheduledTask* pop_shared(uint32_t id, uint32_t shard, Priority priority,
                            bool batch, bool& prefer_overflow);
  ScheduledTask* drain(uint32_t id, uint32_t shard, Priority priority);
  ScheduledTask* poll_external(uint32_t id, Priority priority);
  ScheduledTask* steal(uint32_t id, uint32_t shard);

  uint32_t worker_count_, shard_count_, central_batch_;
  OwnedArray<Local> locals_;
  OwnedArray<Shard> shards_;
  OwnedArray<uint32_t> members_;
  alignas(DAGFLOW_CACHE_LINE_SIZE) std::atomic<uint32_t> next_shard_{0};
};
}  // namespace dagflow::detail
