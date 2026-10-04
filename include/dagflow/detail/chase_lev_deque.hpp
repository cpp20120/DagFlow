#pragma once
/**
 * @file chase_lev_deque.hpp
 * @brief Bounded single-owner, multiple-thief Chase-Lev deque.
 *
 * Storage is fixed for the lifetime of the deque; operations never allocate.
 * Only the owner calls try_push(), try_pop(), and free_capacity().
 * Other threads may try_steal(). A failed steal may mean contention, not
 * emptiness. Destroy only after the owner and all thieves have stopped
 * accessing the deque.
 *
 * Ordering follows the portable C11 algorithm in Figure 1 of Le et al.,
 * "Correct and Efficient Work-Stealing for Weak Memory Models" (PPoPP 2013):
 * https://fzn.fr/readings/ppopp13.pdf
 * All bottom stores use release ordering, including owner-pop updates, so
 * every bottom value observed by a thief explicitly publishes prior writes.
 * This also lets race detectors track publication without release fences.
 * Atomic slots also protect speculative reads by delayed thieves when a slot
 * is reused. Values must not be dereferenced until the operation succeeds.
 * Indices must not overflow ptrdiff_t during the lifetime of the deque.
 */

#include <atomic>
#include <cstddef>
#include <limits>
#include <type_traits>

#include "dagflow/config.hpp"
#include "dagflow/detail/runtime_diagnostics.hpp"

namespace dagflow::detail {

template <typename T, std::size_t N = DAGFLOW_LOCAL_QUEUE_CAPACITY>
class chase_lev_deque {
  static_assert(N >= 2 && (N & (N - 1)) == 0,
                "Capacity must be a power of two >= 2");
  static_assert(N <= static_cast<std::size_t>(
                         std::numeric_limits<std::ptrdiff_t>::max()));
  static_assert(std::is_trivially_copyable_v<T>);
  static_assert(std::atomic<T>::is_always_lock_free,
                "Deque slots require lock-free atomics");
  static_assert(std::atomic<std::ptrdiff_t>::is_always_lock_free);

 public:
  using value_type = T;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;

  chase_lev_deque() noexcept = default;
  chase_lev_deque(const chase_lev_deque&) = delete;
  chase_lev_deque& operator=(const chase_lev_deque&) = delete;

  [[nodiscard]] static constexpr size_type capacity() noexcept { return N; }

  /// Owner only. Failure leaves the deque unchanged; caller retains the value.
  [[nodiscard]] bool try_push(T value) noexcept {
    const auto b = bottom_.load(std::memory_order_relaxed);
    const auto t = top_.load(std::memory_order_acquire);
    if (b - t >= static_cast<std::ptrdiff_t>(N)) return false;

    buffer_[static_cast<std::size_t>(b) & (N - 1)].store(
        value, std::memory_order_relaxed);
    bottom_.store(b + 1, std::memory_order_release);
    return true;
  }

  /// Owner only. Does not modify out when empty or when a thief wins.
  [[nodiscard]] bool try_pop(T& out) noexcept {
    const auto b = bottom_.load(std::memory_order_relaxed) - 1;
    bottom_.store(b, std::memory_order_release);
    std::atomic_thread_fence(std::memory_order_seq_cst);
    auto t = top_.load(std::memory_order_relaxed);

    if (t > b) {
      bottom_.store(b + 1, std::memory_order_release);
      return false;
    }

    const T value = buffer_[static_cast<std::size_t>(b) & (N - 1)].load(
        std::memory_order_relaxed);
    if (t == b) {
      const bool won = top_.compare_exchange_strong(
          t, t + 1, std::memory_order_seq_cst, std::memory_order_relaxed);
      bottom_.store(b + 1, std::memory_order_release);
      if (!won) return false;
    }
    out = value;
    return true;
  }

  /// Any thief. Does not modify out on failure; the caller may retry.
  [[nodiscard]] bool try_steal(T& out) noexcept {
    runtime_count(RuntimeEvent::steal_probe);
    auto t = top_.load(std::memory_order_acquire);
    // A failed empty probe does not compete for a slot. Avoid the expensive
    // arbitration fence in that common case; a possible success still uses
    // the original fence, bottom recheck and last-item CAS protocol below.
    if (t >= bottom_.load(std::memory_order_acquire)) {
      runtime_count(RuntimeEvent::steal_empty);
      return false;
    }
    std::atomic_thread_fence(std::memory_order_seq_cst);
    const auto b = bottom_.load(std::memory_order_acquire);
    if (t >= b) {
      runtime_count(RuntimeEvent::steal_empty);
      return false;
    }

    const T value = buffer_[static_cast<std::size_t>(t) & (N - 1)].load(
        std::memory_order_relaxed);
    if (!top_.compare_exchange_strong(t, t + 1, std::memory_order_seq_cst,
                                      std::memory_order_relaxed)) {
      runtime_count(RuntimeEvent::steal_lost);
      return false;
    }
    runtime_count(RuntimeEvent::stolen_tasks);
    out = value;
    return true;
  }

  /// Owner-only conservative capacity estimate. Thieves can only free slots.
  [[nodiscard]] std::size_t free_capacity() const noexcept {
    const auto b = bottom_.load(std::memory_order_relaxed);
    const auto t = top_.load(std::memory_order_acquire);
    return N - static_cast<std::size_t>(b - t);
  }

  /// Snapshot only; this is not a termination/completion test.
  [[nodiscard]] bool empty() const noexcept {
    const auto t = top_.load(std::memory_order_acquire);
    const auto b = bottom_.load(std::memory_order_acquire);
    return t >= b;
  }

 private:
  alignas(DAGFLOW_CACHE_LINE_SIZE) std::atomic<std::ptrdiff_t> bottom_{0};
  alignas(DAGFLOW_CACHE_LINE_SIZE) std::atomic<std::ptrdiff_t> top_{0};
  alignas(DAGFLOW_CACHE_LINE_SIZE) std::atomic<T> buffer_[N]{};
};

}  // namespace dagflow::detail
