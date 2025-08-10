#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

namespace tp::detail {

template <typename T>
class chase_lev_deque {
 public:
  chase_lev_deque()
	  : bottom_(0), top_(0), cap_(initial_capacity_), buf_(new T[cap_]) {}

  ~chase_lev_deque() = default;
  chase_lev_deque(const chase_lev_deque&) = delete;
  chase_lev_deque& operator=(const chase_lev_deque&) = delete;

  void push_bottom(T v) {
	auto b = bottom_.load(std::memory_order_relaxed);
	auto t = top_.load(std::memory_order_acquire);
	if (b - t >= static_cast<ptrdiff_t>(cap_ - 1)) grow(b, t);
	buf_[b & (cap_ - 1)] = std::move(v);
	std::atomic_thread_fence(std::memory_order_release);
	bottom_.store(b + 1, std::memory_order_relaxed);
  }

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

  bool steal(T& out) {
	auto t = top_.load(std::memory_order_acquire);
	std::atomic_thread_fence(std::memory_order_seq_cst);
	auto b = bottom_.load(std::memory_order_acquire);
	if (t >= b) return false;
	out = std::move(buf_[t & (cap_ - 1)]);
	return top_.compare_exchange_strong(t, t + 1, std::memory_order_seq_cst,
										std::memory_order_relaxed);
  }

  bool empty() const {
	auto t = top_.load(std::memory_order_acquire);
	auto b = bottom_.load(std::memory_order_acquire);
	return t >= b;
  }

 private:
  void grow(ptrdiff_t b, ptrdiff_t t) {
	size_t new_cap = cap_ * 2;
	std::unique_ptr<T[]> nb(new T[new_cap]);
	for (ptrdiff_t i = t; i < b; ++i) {
	  nb[i & (new_cap - 1)] = std::move(buf_[i & (cap_ - 1)]);
	}
	buf_.swap(nb);
	cap_ = new_cap;
  }

  static constexpr size_t initial_capacity_ = 1024;

  alignas(64) std::atomic<ptrdiff_t> bottom_;
  alignas(64) std::atomic<ptrdiff_t> top_;
  size_t cap_;
  std::unique_ptr<T[]> buf_;
};

}  // namespace tp::detail
