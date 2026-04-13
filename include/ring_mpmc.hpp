#pragma once
/**
 * @file ring_mpmc.hpp
 * @brief Bounded MPMC ring-buffer queue (Vyukov's algorithm).
 *
 * @details
 * Zero per-operation heap allocations; no QSBR/hazard-pointer reclamation
 * needed (storage is preallocated at construction).
 *
 * Capacity @c N must be a compile-time power of two. When the buffer is full,
 * push() spins with yield back-off until a consumer makes room.
 *
 * Algorithm: each cell carries a `sequence` counter.
 *  - Producer claims a slot by CAS-incrementing tail_; writes data; marks the
 *    cell ready by storing seq = pos + 1.
 *  - Consumer claims a slot by CAS-incrementing head_; reads data; marks the
 *    cell recyclable by storing seq = pos + N.
 */

#include <atomic>
#include <cstddef>
#include <optional>
#include <thread>

#include "config.hpp"

namespace dagflow::detail {

template <typename T, std::size_t N>
class ring_mpmc {
  static_assert((N & (N - 1)) == 0, "N must be a power of two");
  static constexpr std::size_t MASK = N - 1;

  struct Cell {
    std::atomic<std::size_t> seq{};
    T data{};
  };

 public:
  ring_mpmc() noexcept {
    for (std::size_t i = 0; i < N; ++i)
      buf_[i].seq.store(i, std::memory_order_relaxed);
  }

  ring_mpmc(const ring_mpmc&) = delete;
  ring_mpmc& operator=(const ring_mpmc&) = delete;

  /**
   * @brief Push an element. Spins (yielding) when the buffer is full.
   */
  void push(T v) {
    std::size_t pos = tail_.load(std::memory_order_relaxed);
    for (;;) {
      Cell& cell = buf_[pos & MASK];
      std::size_t seq = cell.seq.load(std::memory_order_acquire);
      auto diff =
          static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos);
      if (diff == 0) {
        if (tail_.compare_exchange_weak(pos, pos + 1,
                                        std::memory_order_relaxed))
          break;
      } else if (diff < 0) {
        // Full — yield and retry.
        std::this_thread::yield();
        pos = tail_.load(std::memory_order_relaxed);
      } else {
        pos = tail_.load(std::memory_order_relaxed);
      }
    }
    buf_[pos & MASK].data = std::move(v);
    buf_[pos & MASK].seq.store(pos + 1, std::memory_order_release);
  }

  /**
   * @brief Try to pop; returns std::nullopt if empty.
   */
  std::optional<T> pop() {
    std::size_t pos = head_.load(std::memory_order_relaxed);
    for (;;) {
      Cell& cell = buf_[pos & MASK];
      std::size_t seq = cell.seq.load(std::memory_order_acquire);
      auto diff = static_cast<std::ptrdiff_t>(seq) -
                  static_cast<std::ptrdiff_t>(pos + 1);
      if (diff == 0) {
        if (head_.compare_exchange_weak(pos, pos + 1,
                                        std::memory_order_relaxed))
          break;
      } else if (diff < 0) {
        return std::nullopt;
      } else {
        pos = head_.load(std::memory_order_relaxed);
      }
    }
    T v = std::move(buf_[pos & MASK].data);
    buf_[pos & MASK].seq.store(pos + MASK + 1, std::memory_order_release);
    return v;
  }

  /**
   * @brief Best-effort empty check (no lock).
   */
  [[nodiscard]] bool empty() const noexcept {
    std::size_t pos = head_.load(std::memory_order_acquire);
    return buf_[pos & MASK].seq.load(std::memory_order_acquire) != pos + 1;
  }

 private:
  alignas(CACHE_LINE_SIZE) Cell buf_[N];
  alignas(CACHE_LINE_SIZE) std::atomic<std::size_t> head_{0};
  alignas(CACHE_LINE_SIZE) std::atomic<std::size_t> tail_{0};
};

}  // namespace dagflow::detail
