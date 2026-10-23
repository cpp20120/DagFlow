#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <dagflow/detail/runtime_memory.hpp>
#include <dagflow/detail/scheduler.hpp>
#include <mutex>

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
  // Completion notification targets the waiting worker, including nested waits.
  void wake_worker(uint32_t worker);
  void wake_all();
#if defined(DAGFLOW_FUZZ_HOOKS)
  [[nodiscard]] uint64_t fuzz_epoch(uint32_t worker) const noexcept {
    return waiters_[worker].epoch.load(std::memory_order_seq_cst);
  }
  [[nodiscard]] bool fuzz_sleeping(uint32_t worker) const noexcept {
    return waiters_[worker].sleeping.load(std::memory_order_seq_cst);
  }
  // Configure before starting the waiter. Untimed waits expose missing
  // notifications instead of allowing an epoch recheck at a timer deadline.
  void fuzz_wait_without_timeout(uint32_t worker) noexcept {
    waiters_[worker].cv.untimed = true;
  }
  [[nodiscard]] uint64_t fuzz_notifications(uint32_t worker) const noexcept {
    return waiters_[worker].cv.notifications.load(std::memory_order_acquire);
  }
#endif

 private:
#if defined(DAGFLOW_FUZZ_HOOKS)
  // Record the primitive invocation itself, not an adjacent source hook.
  // Removing cv.notify_one() must also remove this observable event.
  struct WaitCondition {
    std::condition_variable condition;
    std::atomic<uint64_t> notifications{0};
    bool untimed = false;
    template <class Rep, class Period, class Predicate>
    bool wait_for(std::unique_lock<std::mutex>& lock,
                  std::chrono::duration<Rep, Period> duration,
                  Predicate predicate) {
      if (untimed) {
        const auto before = notifications.load(std::memory_order_acquire);
        bool committed = false;
        condition.wait(lock, [&] {
          const bool signalled = predicate();
          if (!signalled) committed = true;
          // Once a false predicate commits this test waiter to sleep, a
          // spurious wake must not clear sleeping before the publisher checks
          // its notification obligation. An initially true predicate still
          // takes the ordinary no-notification path.
          return signalled &&
                 (!committed ||
                  notifications.load(std::memory_order_acquire) != before);
        });
        return true;
      }
      return condition.wait_for(lock, duration, predicate);
    }
    void notify_one() noexcept {
      notifications.fetch_add(1, std::memory_order_release);
      condition.notify_one();
    }
  };
#else
  using WaitCondition = std::condition_variable;
#endif
  struct Waiter {
    std::mutex mutex;
    WaitCondition cv;
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
#if defined(DAGFLOW_FUZZ_HOOKS)
  void signal(uint32_t worker, bool claimed, bool checkpoint = true);
#else
  void signal(uint32_t worker, bool claimed);
#endif
  const std::span<const uint32_t> members_;
  OwnedArray<Waiter> waiters_;
  OwnedArray<Domain> domains_;
  OwnedArray<std::atomic<uint64_t>> idle_;
};
}  // namespace dagflow::detail
