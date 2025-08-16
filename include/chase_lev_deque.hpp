#pragma once
/**
 * @file chase_lev_deque.hpp
 * @brief Lock-free Chase–Lev work-stealing deque implementation.
 *
 * @details
 * This structure provides a single-owner, multiple-thief work-stealing deque:
 *  - The owner thread pushes to and pops from the bottom.
 *  - Thief threads attempt to steal from the top.
 *
 * Memory model and correctness closely follow the original Chase–Lev algorithm:
 *  - The ring buffer capacity is a power of two; indexing uses mask `index &
 * (cap - 1)`.
 *  - The `bottom_` index is modified only by the owner; `top_` is used by
 * thieves.
 *  - Special care is required when popping the last element (races with
 * thieves).
 *
 * Progress guarantees:
 *  - Owner operations are wait-free in the common case.
 *  - Thief operations are lock-free.
 *
 * Thread-safety:
 *  - Exactly one owner thread must use push_bottom() and pop_bottom().
 *  - Any number of thief threads may call steal().
 */

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

namespace dagflow::detail {

template <typename T>
class chase_lev_deque {
 public:
  /**
   * @brief Construct a deque with initial power-of-two capacity.
   *
   * The capacity grows automatically on owner push when the buffer would
   * overflow.
   */
  chase_lev_deque()
	  : bottom_(0), top_(0), cap_(initial_capacity_), buf_(new T[cap_]) {}
  ~chase_lev_deque() = default;

  chase_lev_deque(const chase_lev_deque&) = delete;
  chase_lev_deque& operator=(const chase_lev_deque&) = delete;
  /**
   * @brief Push a value at the bottom. Only the owner may call this.
   * @param v Value to push. It is moved into the ring buffer.
   *
   * If the ring is full (modulo capacity), it grows by a factor of two.
   */
  void push_bottom(T v) {
	auto b = bottom_.load(std::memory_order_relaxed);
	auto t = top_.load(std::memory_order_acquire);
	if (b - t >= static_cast<ptrdiff_t>(cap_ - 1)) grow(b, t);

	buf_[b & (cap_ - 1)] = std::move(v);
	bottom_.store(b + 1, std::memory_order_release);
  }
  /**
   * @brief Pop a value from the bottom. Only the owner may call this.
   * @param out Reference to store the popped value on success.
   * @return true if an element was popped; false if the deque was empty or a
   * thief won the race for the last element.
   */
  bool pop_bottom(T& out) {
	auto b = bottom_.load(std::memory_order_relaxed) - 1;
	bottom_.store(b, std::memory_order_relaxed);

	std::atomic_thread_fence(std::memory_order_seq_cst);

	auto t = top_.load(std::memory_order_relaxed);

	if (t > b) {
	  bottom_.store(b + 1, std::memory_order_relaxed);
	  return false;
	}

	out = std::move(buf_[b & (cap_ - 1)]);

	if (t == b) {
	  if (!top_.compare_exchange_strong(t, t + 1,
										std::memory_order_seq_cst,
										std::memory_order_relaxed)) {
		bottom_.store(b + 1, std::memory_order_relaxed);
		return false;
	  }
	  bottom_.store(b + 1, std::memory_order_relaxed);
	}
	return true;
  }

  /**
   * @brief Attempt to steal a value from the top. Any thief thread may call
   * this.
   * @param out Reference to store the stolen value on success.
   * @return true if a value was stolen, false if the deque was empty or a race
   * was lost.
   */
  bool steal(T& out) {
	auto t = top_.load(std::memory_order_acquire);
	auto b = bottom_.load(std::memory_order_acquire);
	if (t >= b) return false;

	out = std::move(buf_[t & (cap_ - 1)]);
	return top_.compare_exchange_strong(t, t + 1, std::memory_order_acq_rel,
										std::memory_order_relaxed);
  }
  /**
   * @brief Snapshot emptiness check.
   * @return true if the deque is observed empty (top >= bottom).
   */
  bool empty() const {
	auto t = top_.load(std::memory_order_acquire);
	auto b = bottom_.load(std::memory_order_acquire);
	return t >= b;
  }

 private:
  /**
   * @brief Grow the ring buffer to the next power-of-two capacity and move
   * elements.
   * @param b Current bottom index.
   * @param t Current top index.
   *
   * Elements are moved into the new buffer preserving their logical positions.
   */
  void grow(ptrdiff_t b, ptrdiff_t t) {
	size_t new_cap = cap_ * 2;
	std::unique_ptr<T[]> nb(new T[new_cap]);
	for (ptrdiff_t i = t; i < b; ++i)
	  nb[i & (new_cap - 1)] = std::move(buf_[i & (cap_ - 1)]);
	buf_.swap(nb);
	cap_ = new_cap;
  }
  /// Initial capacity for the ring buffer; must be a power of two.
  static constexpr size_t initial_capacity_ = 1024;

  /// Bottom index (owner thread). 64-byte aligned to avoid false sharing.
  alignas(64) std::atomic<ptrdiff_t> bottom_;
  /// Top index (thieves). 64-byte aligned to avoid false sharing.
  alignas(64) std::atomic<ptrdiff_t> top_;
  /// Current ring capacity (power of two).
  size_t cap_;
  /// Storage for elements.
  std::unique_ptr<T[]> buf_;
};

}  // namespace tp::detail
