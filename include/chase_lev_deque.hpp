#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

/**
 * @file chase_lev_deque.hpp
 * @brief Implementation of Chase-Lev work-stealing deque
 */
namespace tp::detail {
/**
 * @brief Chase-Lev work-stealing deque implementation
 * @tparam T Type of elements stored in the deque
 *
 * This is a lock-free deque supporting efficient work stealing from other
 * threads. The implementation follows the algorithm described in: "Dynamic
 * Circular Work-Stealing Deque" by David Chase and Yossi Lev
 */
template <typename T>
class chase_lev_deque {
 public:
  /**
   * @brief Constructs an empty deque with initial capacity
   */
  chase_lev_deque()
	  : bottom_(0), top_(0), cap_(initial_capacity_), buf_(new T[cap_]) {}

  ~chase_lev_deque() = default;
  // Non-copyable
  chase_lev_deque(const chase_lev_deque&) = delete;
  chase_lev_deque& operator=(const chase_lev_deque&) = delete;
  /**
   * @brief Pushes an element to the bottom of the deque (owner thread only)
   * @param v Value to push
   */
  void push_bottom(T v) {
	auto b = bottom_.load(std::memory_order_relaxed);
	auto t = top_.load(std::memory_order_acquire);
	if (b - t >= static_cast<ptrdiff_t>(cap_ - 1)) grow(b, t);
	buf_[b & (cap_ - 1)] = std::move(v);
	std::atomic_thread_fence(std::memory_order_release);
	bottom_.store(b + 1, std::memory_order_relaxed);
  }
  /**
   * @brief Pops an element from the bottom of the deque (owner thread only)
   * @param[out] out Reference to store popped value
   * @return True if element was popped, false if deque was empty
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
	  if (!top_.compare_exchange_strong(t, t + 1, std::memory_order_seq_cst,
										std::memory_order_relaxed)) {
		bottom_.store(b + 1, std::memory_order_relaxed);
		return false;
	  }
	  bottom_.store(b + 1, std::memory_order_relaxed);
	}
	return true;
  }
  /**
   * @brief Steals an element from the top of the deque (other threads)
   * @param[out] out Reference to store stolen value
   * @return True if element was stolen, false if deque was empty
   */
  bool steal(T& out) {
	auto t = top_.load(std::memory_order_acquire);
	std::atomic_thread_fence(std::memory_order_seq_cst);
	auto b = bottom_.load(std::memory_order_acquire);
	if (t >= b) return false;
	out = std::move(buf_[t & (cap_ - 1)]);
	return top_.compare_exchange_strong(t, t + 1, std::memory_order_seq_cst,
										std::memory_order_relaxed);
  }
  /**
   * @brief Checks if deque is empty
   * @return True if deque is empty
   */
  bool empty() const {
	auto t = top_.load(std::memory_order_acquire);
	auto b = bottom_.load(std::memory_order_acquire);
	return t >= b;
  }

 private:
  /**
   * @brief Grows the deque capacity when full
   * @param b Current bottom index
   * @param t Current top index
   */
  void grow(ptrdiff_t b, ptrdiff_t t) {
	size_t new_cap = cap_ * 2;
	std::unique_ptr<T[]> nb(new T[new_cap]);
	for (ptrdiff_t i = t; i < b; ++i) {
	  nb[i & (new_cap - 1)] = std::move(buf_[i & (cap_ - 1)]);
	}
	buf_.swap(nb);
	cap_ = new_cap;
  }

  static constexpr size_t initial_capacity_ = 1024;	 ///< Initial deque capacity
  // Align to cache lines to prevent false sharing
  alignas(64) std::atomic<ptrdiff_t> bottom_;  ///< Bottom index (owner thread)
  alignas(64) std::atomic<ptrdiff_t> top_;	   ///< Top index (for stealing)
  size_t cap_;								   ///< Current capacity
  std::unique_ptr<T[]> buf_;				   ///< Underlying buffer
};

}  // namespace tp::detail
