#pragma once
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>

#include <dagflow/detail/runtime_memory.hpp>
#include <dagflow/detail/scheduler.hpp>

namespace dagflow::detail {
/// Shares immutable membership with Scheduler; owns all wait state. Runtime
/// publication must precede wake_one. Owners announce parking before the final
/// acquisition attempt and cancel that announcement when they find work.
///
/// The paired fence/idle-bit/epoch protocol is intentional: a publisher may
/// see no idle worker and return, or claim one announced waiter and signal it.
/// The waiter performs a final acquisition scan after announcing itself. A
/// plain relaxed `need_wake` flag or one global epoch would permit a lost wake
/// unless it grew its own rearm handshake, which would cost the same shared
/// state this layout removes from the common no-idle path.
class ParkingLot {
 public:
  explicit ParkingLot(const Scheduler& topology);
  uint64_t prepare(uint32_t worker) noexcept;
  void cancel(uint32_t worker) noexcept;
  void wait(uint32_t worker, uint64_t epoch, uint32_t timeout_us,
            const std::atomic<bool>& stop);
  void wake_one(uint32_t shard);
  void wake_all();
#if defined(DAGFLOW_FUZZ_HOOKS)
  [[nodiscard]] uint64_t fuzz_epoch(uint32_t worker) const noexcept {
    return waiters_[worker].epoch.load(std::memory_order_seq_cst);
  }
  [[nodiscard]] bool fuzz_sleeping(uint32_t worker) const noexcept {
    return waiters_[worker].sleeping.load(std::memory_order_seq_cst);
  }
#endif

 private:
  struct Waiter {
    std::mutex mutex;
    std::condition_variable cv;
    std::atomic<uint64_t> epoch{0};
    // Only a thread committing to CV sleep needs a mutex/notify handshake.
    std::atomic<bool> sleeping{false};
    uint32_t word_index{};
    uint64_t bit{};
  };
  struct Domain {
    uint32_t first_word{}, word_count{};
    uint64_t first_mask{}, last_mask{};
  };
  bool wake_domain(uint32_t shard);
  bool wake_word(uint32_t word_index, uint64_t mask);
  void signal(uint32_t worker, bool claimed);
  const std::span<const uint32_t> members_;
  OwnedArray<Waiter> waiters_;
  OwnedArray<Domain> domains_;
  OwnedArray<std::atomic<uint64_t>> idle_;
};
}  // namespace dagflow::detail
