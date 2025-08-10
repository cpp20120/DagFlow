#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <span>
#include <thread>
#include <type_traits>
#include <vector>

#include "chase_lev_deque.hpp"
#include "hazard_ptr.hpp"
#include "mpmc_queue.hpp"
#include "small_function.hpp"

namespace tp {

enum class Priority : uint8_t { High = 0, Normal = 1 };

struct Config {
  uint32_t threads = std::thread::hardware_concurrency();
  uint32_t shards = 0;	// 0 => = threads
  uint32_t central_batch = 64;
  uint32_t idle_us_min = 50;
  uint32_t idle_us_max = 200;
  bool pin_threads = false;	 // enable pinning
};

struct SubmitOptions {
  std::optional<uint32_t> affinity;
  Priority priority = Priority::Normal;
  bool owned = true;
};

class Handle {
 public:
  struct Counter {
	std::atomic<int> count{0};
	std::mutex mu;
	std::condition_variable cv;
  };

  Handle() noexcept = default;
  explicit Handle(std::shared_ptr<Counter> c) noexcept : ctr_(std::move(c)) {}

  bool valid() const noexcept { return static_cast<bool>(ctr_); }
  std::shared_ptr<Counter> get() const noexcept { return ctr_; }

 private:
  std::shared_ptr<Counter> ctr_{};
  friend class Pool;
};

using Fn = void (*)(void*);

class Pool {
 public:
  explicit Pool(const Config& cfg = {});
  ~Pool();

  Pool(const Pool&) = delete;
  Pool& operator=(const Pool&) = delete;

  Handle submit(Fn fn, void* arg = nullptr, SubmitOptions opt = {});
  template <class F>
  Handle submit(F f, SubmitOptions opt = {}) {
	return submit_impl(small_function<void()>{[g = std::move(f)] { g(); }},
					   std::move(opt));
  }

  template <class It, class F>
  Handle for_each(It begin, It end, F f, SubmitOptions opt = {}) {
	const std::size_t n = static_cast<std::size_t>(std::distance(begin, end));
	if (n == 0) return Handle{};
	const std::size_t target = 1 << 14;
	std::size_t chunks = (n + target - 1) / target;
	if (chunks == 0) chunks = 1;

	auto ctr = std::make_shared<Handle::Counter>();
	ctr->count.store(static_cast<int>(chunks), std::memory_order_relaxed);

	using Cat = typename std::iterator_traits<It>::iterator_category;
	for (std::size_t c = 0; c < chunks; ++c) {
	  std::size_t lo = c * n / chunks;
	  std::size_t hi = (c + 1) * n / chunks;

	  if constexpr (std::is_base_of_v<std::random_access_iterator_tag, Cat>) {
		It base = begin + static_cast<long>(lo);  // локальная копия base
		auto task =
			small_function<void()>{[base, count = hi - lo, &f, ctr]() mutable {
			  auto it = base;
			  for (std::size_t i = 0; i < count; ++i, ++it) f(*it);
			  complete_counter(ctr.get());
			}};
		submit_impl(std::move(task), opt);
	  } else {
		It base = begin;
		std::advance(base, static_cast<long>(lo));
		auto task =
			small_function<void()>{[base, count = hi - lo, &f, ctr]() mutable {
			  auto it = base;
			  for (std::size_t i = 0; i < count; ++i, ++it) f(*it);
			  complete_counter(ctr.get());
			}};
		submit_impl(std::move(task), opt);
	  }
	}
	return Handle{std::move(ctr)};
  }

  Handle combine(std::span<const Handle> hs, SubmitOptions opt = {});
  Handle combine(std::initializer_list<Handle> hs, SubmitOptions opt = {}) {
	return combine(std::span<const Handle>(hs.begin(), hs.size()),
				   std::move(opt));
  }
  void wait(const Handle& h);
  void wait_idle();

 private:
  struct Task {
	small_function<void()> fn;
	Priority prio{Priority::Normal};
	bool owned{true};
	std::shared_ptr<Handle::Counter> done{};
  };

  struct Worker {
	detail::chase_lev_deque<Task*> deq_hi;
	detail::chase_lev_deque<Task*> deq_lo;
	std::mt19937 rng;
	std::mutex mu;
	std::condition_variable cv;
	uint32_t backoff_us{};
  };

  struct CentralShard {
	detail::mpmc_queue<Task*> hi;
	detail::mpmc_queue<Task*> lo;
  };

  Handle submit_impl(small_function<void()> job, SubmitOptions opt);
  void dispatch(Task* t, std::optional<uint32_t> affinity);
  void worker_loop(uint32_t id);
  bool try_help_one(uint32_t id);

  static void complete_counter(Handle::Counter* ctr);

  bool all_empty() const;
  void notify_worker(uint32_t id);
  uint32_t pick_queue();

  static thread_local uint32_t tls_id_;
  static thread_local bool tls_in_pool_;

  Config cfg_;
  std::vector<std::thread> threads_;
  std::vector<std::unique_ptr<Worker>> workers_;
  std::vector<std::unique_ptr<CentralShard>> centrals_;

  alignas(64) std::atomic<bool> stop_{false};
  alignas(64) std::atomic<uint64_t> submitted_{0};
  alignas(64) std::atomic<uint64_t> executed_{0};
  alignas(64) std::atomic<uint64_t> stolen_{0};
  alignas(64) std::atomic<uint32_t> rr_{0};
  alignas(64) std::atomic<uint32_t> inflight_{0};

  mutable std::mutex wait_mu_;
  std::condition_variable wait_cv_;
};

}  // namespace tp
