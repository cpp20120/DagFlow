#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <limits>
#include <mutex>

#include <dagflow/config.hpp>
#include <dagflow/detail/runtime_memory.hpp>

namespace dagflow::detail {
// Each lane has exactly one writer. Observers read only while waiting for idle.
// Registration/storage lifetime belongs to the pool, never to producer TLS.
// The publication-before-queue and retirement-after-destruction ordering in
// Pool is required: wait_idle() proves physical packet lifetime, not merely
// completion-credit readiness. A shared fetch_add counter was rejected because
// it makes every task bounce one cache line; a TLS lane pointer cache was also
// measured and rejected, so worker lanes are indexed directly by worker ID.
class IdleAccounting {
 public:
  explicit IdleAccounting(uint32_t workers);
  ~IdleAccounting();
  void publish_worker(uint32_t worker) {
    // Worker ID is stable for the pool lifetime and is the sole writer of this
    // lane. Do not move this to a shared counter or a stealing-side transfer:
    // the executor, not the publisher, owns retirement.
    publish(workers_[worker]);
  }
  void publish_external();
  void retire(uint32_t worker) noexcept {
    auto& retired = workers_[worker].retired;
    const auto previous = retired.load(std::memory_order_relaxed);
    if (previous == std::numeric_limits<uint64_t>::max()) std::terminate();
    retired.store(previous + 1, std::memory_order_release);
  }
  void worker_idle();
  void wait();

 private:
  struct alignas(DAGFLOW_CACHE_LINE_SIZE) Lane {
    std::atomic<uint64_t> published{0}, retired{0};
  };
  struct Producer {
    Lane lane;
    uint64_t id;
    OwnedObject<Producer> next;
    explicit Producer(uint64_t producer) : id(producer) {}
  };
  [[noreturn]] static void publication_exhausted();
  static void publish(Lane& lane) {
    const auto previous = lane.published.load(std::memory_order_relaxed);
    if (previous == std::numeric_limits<uint64_t>::max()) publication_exhausted();
    lane.published.store(previous + 1, std::memory_order_release);
  }
  Lane& producer_lane();
  bool idle_locked() const noexcept;

  const uint64_t identity_;
  OwnedArray<Lane> workers_;
  OwnedObject<Producer> producers_;
  std::mutex mutex_;
  std::condition_variable cv_;
  // Only wait() writes this, under mutex_. No per-task observer traffic.
  std::atomic<uint32_t> waiters_{0};
};
}  // namespace dagflow::detail
