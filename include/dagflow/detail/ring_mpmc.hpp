#pragma once

#include <atomic>
#include <cstddef>
#include <type_traits>
#include <utility>

#include "dagflow/config.hpp"

namespace dagflow::detail {

/// Fixed-capacity Vyukov MPMC queue. Operations do not allocate or wait for
/// space. A producer paused after reserving a slot can obstruct consumers; this
/// is not a strictly lock-free queue. Destruction requires all users to have
/// stopped.
template <typename T, std::size_t N>
class ring_mpmc {
  static_assert(N >= 2 && (N & (N - 1)) == 0,
                "Capacity must be a power of two >= 2");
  // A reserved slot must always be published/recycled, even for generic T.
  static_assert(std::is_nothrow_move_assignable_v<T>);

 public:
  using value_type = T;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;
  using reference = value_type&;
  using const_reference = const value_type&;

  ring_mpmc() noexcept(std::is_nothrow_default_constructible_v<T>) {
    for (size_type i = 0; i < N; ++i)
      buffer_[i].sequence.store(i, std::memory_order_relaxed);
  }

  ring_mpmc(const ring_mpmc&) = delete;
  ring_mpmc& operator=(const ring_mpmc&) = delete;

  [[nodiscard]] static constexpr size_type capacity() noexcept { return N; }

  /// Returns false without copying/moving from value if the next slot is busy.
  [[nodiscard]] bool try_push(const_reference value) noexcept
    requires std::is_nothrow_copy_assignable_v<T>
  {
    return push_impl(value);
  }

  [[nodiscard]] bool try_push(value_type&& value) noexcept {
    return push_impl(std::move(value));
  }

  /// Returns false without changing value when the next slot is unpublished.
  /// This can also happen while a producer is still publishing that slot.
  [[nodiscard]] bool try_pop(reference value) noexcept {
    size_type position = head_.load(std::memory_order_relaxed);
    for (;;) {
      auto& cell = buffer_[position & mask];
      const auto sequence = cell.sequence.load(std::memory_order_acquire);
      const auto difference = static_cast<difference_type>(sequence) -
                              static_cast<difference_type>(position + 1);
      if (difference == 0) {
        if (head_.compare_exchange_weak(position, position + 1,
                                        std::memory_order_relaxed))
          break;
      } else if (difference < 0) {
        return false;
      } else {
        position = head_.load(std::memory_order_relaxed);
      }
    }
    auto& cell = buffer_[position & mask];
    value = std::move(cell.value);
    cell.sequence.store(position + N, std::memory_order_release);
    return true;
  }

  /// Snapshot only: an unpublished head is not proof that a run is finished.
  [[nodiscard]] bool empty() const noexcept {
    const auto position = head_.load(std::memory_order_acquire);
    return buffer_[position & mask].sequence.load(std::memory_order_acquire) !=
           position + 1;
  }

 private:
  template <class U>
  bool push_impl(U&& value) noexcept {
    size_type position = tail_.load(std::memory_order_relaxed);
    for (;;) {
      auto& cell = buffer_[position & mask];
      const auto sequence = cell.sequence.load(std::memory_order_acquire);
      const auto difference = static_cast<difference_type>(sequence) -
                              static_cast<difference_type>(position);
      if (difference == 0) {
        if (tail_.compare_exchange_weak(position, position + 1,
                                        std::memory_order_relaxed))
          break;
      } else if (difference < 0) {
        return false;
      } else {
        position = tail_.load(std::memory_order_relaxed);
      }
    }
    auto& cell = buffer_[position & mask];
    cell.value = std::forward<U>(value);
    cell.sequence.store(position + 1, std::memory_order_release);
    return true;
  }

  struct Cell {
    std::atomic<size_type> sequence{};
    value_type value{};
  };
  static constexpr size_type mask = N - 1;

  alignas(DAGFLOW_CACHE_LINE_SIZE) Cell buffer_[N];
  alignas(DAGFLOW_CACHE_LINE_SIZE) std::atomic<size_type> head_{0};
  alignas(DAGFLOW_CACHE_LINE_SIZE) std::atomic<size_type> tail_{0};
};

}  // namespace dagflow::detail
