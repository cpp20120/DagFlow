#pragma once
/**
 * @file pool.hpp
 * @brief Work-stealing thread pool with per-thread deques, central MPMC queues,
 * priorities and affinity.
 *
 * @details
 * Architecture:
 *  - N worker threads, each owns two Chase–Lev deques: High and Normal
 * priority.
 *  - M central shards (MPMC queues) for external submissions and balancing.
 *  - Work policy: owner pushes/pops bottom; thieves steal from top; workers
 * help neighbors.
 *  - Priority: High is preferred over Normal across owner and central
 * structures.
 *
 * API:
 *  - `submit(F, SubmitOptions)` -> @ref Handle (countdown of work units).
 *  - `for_each` — range chunking (~16K elements per chunk).
 *  - `combine` — wait-all aggregation for multiple handles.
 *  - `wait(Handle)` and `wait_idle()`.
 *
 * Thread-safety:
 *  - `submit` is safe from any thread. Internals are MPMC and lock-free where
 * possible.
 *
 * Performance:
 *  - Idle backoff between `idle_us_min..idle_us_max`.
 *  - RNG for random victim selection while stealing.
 */

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
/**
 * @enum Priority
 * @brief Task priority: @c High served before @c Normal.
 */
enum class Priority : uint8_t { High = 0, Normal = 1 };
/**
 * @struct Config
 * @brief Pool configuration parameters.
 *
 * @var threads Number of worker threads (default: hardware concurrency).
 * @var shards  Number of central shards (default: =threads).
 * @var central_batch Preferred batch size when draining central queues.
 * @var idle_us_min Minimal backoff delay on idle.
 * @var idle_us_max Maximal backoff delay on idle.
 * @var pin_threads Whether to pin OS threads to CPUs (implementation-defined).
 */
struct Config {
  uint32_t threads = std::thread::hardware_concurrency();
  uint32_t shards = 0;
  uint32_t central_batch = 1024;
  uint32_t idle_us_min = 50;
  uint32_t idle_us_max = 200;
  bool pin_threads = true;
};
/**
 * @struct SubmitOptions
 * @brief Submission hints.
 *
 * @var affinity Optional worker id affinity.
 * @var priority Task priority.
 * @var owned    Internal ownership flag (true for pool-owned tasks).
 */
struct SubmitOptions {
  std::optional<uint32_t> affinity;
  Priority priority = Priority::Normal;
  bool owned = true;
};
/**
 * @class Handle
 * @brief Waitable completion handle with an internal countdown.
 *
 * @details
 * Each completed unit of work decrements the shared counter. Waiting threads
 * block on a condition variable until the counter reaches zero.
 */
class Handle {
 public:
  /// Internal shared counter state.
  struct Counter {
	std::atomic<int> count{0};
	std::mutex mu;
	std::condition_variable cv;
  };
  /// Invalid/empty handle.
  Handle() noexcept = default;
  /// Construct from a shared counter.
  explicit Handle(std::shared_ptr<Counter> c) noexcept : ctr_(std::move(c)) {}
  /// @return true if the handle refers to a valid counter.
  bool valid() const noexcept { return static_cast<bool>(ctr_); }
  /// @return shared pointer to the underlying counter.
  std::shared_ptr<Counter> get() const noexcept { return ctr_; }

 private:
  std::shared_ptr<Counter> ctr_{};
  friend class Pool;
};
/// Raw function signature for low-level submission.
using Fn = void (*)(void*);
/**
 * @class Pool
 * @brief Work-stealing thread pool with central queues and priorities.
 */
class Pool {
 public:
  /**
   * @brief Construct a pool with the given config.
   */
  explicit Pool(const Config& cfg = {});
  /**
   * @brief Join worker threads and destroy the pool.
   * @warning Ensure no external references to enqueued tasks remain.
   */
  ~Pool();

  Pool(const Pool&) = delete;
  Pool& operator=(const Pool&) = delete;
  /**
   * @brief Submit a raw function pointer with an optional argument.
   * @param fn  Function pointer `void(void*)`.
   * @param arg Opaque pointer passed to @p fn.
   * @param opt Submission options (priority, affinity).
   * @return Handle that completes when this task's unit of work is done.
   */
  Handle submit(Fn fn, void* arg = nullptr, SubmitOptions opt = {});
  /**
   * @brief Submit a callable (moved into internal small_function).
   * @tparam F Callable type invocable as `void()`.
   * @param f   Callable (moved).
   * @param opt Options (priority, affinity).
   */
  template <class F>
  Handle submit(F f, SubmitOptions opt = {}) {
	return submit_impl(small_function<void()>{[g = std::move(f)] { g(); }},
					   std::move(opt));
  }
  /**
   * @brief Parallel for-each over [begin,end), chunked to approx 16K elements.
   * @tparam It Iterator type (input or random-access).
   * @tparam F  Functor type (invocable as `void(T&)`).
   * @return Handle whose count equals the number of chunks.
   */
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
		It base = begin + static_cast<long>(lo);
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
  /// Helper for cound threads
  uint32_t thread_count() const noexcept {
	if (!workers_.empty()) return static_cast<uint32_t>(workers_.size());
	return cfg_.threads ? cfg_.threads : 1u;
  }
  /**
   * @brief Parallel range processing with dynamic range stealing (help-first).
   *
   * @tparam It Random-access iterator type.
   * @tparam F  Callable type, invocable as `void(T&)` for `*It`.
   * @param begin  Iterator to the start of the range.
   * @param end    Iterator to the end of the range.
   * @param f      Functor to apply to each element in the range.
   * @param opt    Submission options (priority, affinity).
   * @param min_grain_hint Minimal grain size hint (default: 16K elements).
   *        The algorithm may pick a smaller grain if the range is small.
   *
   * @details
   * Implements *help-first* lazy binary splitting:
   * - The current range is processed by repeatedly splitting off the upper half
   *   into a new task if it is at least @c min_grain elements long.
   * - The "leaf" range (≤ 2 × min_grain) is processed sequentially.
   * - Tasks are submitted to the pool with optional affinity to improve NUMA
   * locality.
   *
   * Compared to @ref for_each:
   * - Balances load better for skewed workloads.
   * - Reduces tail-latency by allowing idle workers to steal large chunks.
   * - Requires random-access iterators for O(1) splitting.
   *
   * Example:
   * @code
   * std::vector<int> data(50'000'000, 1);
   * tp::Pool pool;
   * auto h = pool.for_each_ws(data.begin(), data.end(),
   *     [](int& x) { x += 1; },
   *     {.priority = tp::Priority::High});
   * pool.wait(h);
   * @endcode
   *
   * @note This overload requires @c std::random_access_iterator_tag.
   *       Use @ref for_each for generic iterators.
   *
   * @return Handle that completes when all subranges are processed.
   */
  template <class It, class F>
  Handle for_each_ws(It begin, It end, F f, SubmitOptions opt = {},
					 std::size_t min_grain_hint = (1u << 14)) {
	using Cat = typename std::iterator_traits<It>::iterator_category;
	static_assert(std::is_base_of_v<std::random_access_iterator_tag, Cat>,
				  "for_each_ws requires random-access iterators");

	const std::size_t n = static_cast<std::size_t>(end - begin);
	if (n == 0) return Handle{};

	const uint32_t T = this->thread_count();
	std::size_t min_grain = std::max<std::size_t>(n / (T * 8u), min_grain_hint);
	min_grain = std::min<std::size_t>(min_grain, n);

	struct Range {
	  std::size_t lo, hi;
	};

	struct ProcState {
	  Pool* pool;
	  It begin;
	  F func;
	  SubmitOptions opt;
	  std::size_t min_grain;
	  std::shared_ptr<Handle::Counter> ctr;
	};

	auto ctr = std::make_shared<Handle::Counter>();
	ctr->count.store(1, std::memory_order_relaxed);

	auto st = std::make_shared<ProcState>(
		ProcState{this, begin, std::move(f), opt, min_grain, ctr});

	struct Exec {
	  std::shared_ptr<ProcState> st;

	  void operator()(Range r) const {
		while ((r.hi - r.lo) > (st->min_grain * 2)) {
		  const std::size_t mid = r.lo + ((r.hi - r.lo) >> 1);
		  Range upper{mid, r.hi};
		  r.hi = mid;

		  st->ctr->count.fetch_add(1, std::memory_order_relaxed);

		  Exec ex{st};
		  st->pool->submit_impl(
			  small_function<void()>{[ex, upper]() mutable { ex(upper); }},
			  st->opt);
		}

		auto* base = &st->begin[r.lo];
		const std::size_t cnt = r.hi - r.lo;
		for (std::size_t i = 0; i < cnt; ++i) st->func(base[i]);

		Pool::complete_counter(st->ctr.get());
	  }
	};

	Exec ex{st};

	if (n >= min_grain * 4 && T > 1) {
	  const std::size_t tiles = T;
	  ctr->count.store(static_cast<int>(tiles), std::memory_order_relaxed);

	  for (std::size_t t = 0; t < tiles; ++t) {
		const std::size_t lo = (n * t) / tiles;
		const std::size_t hi = (n * (t + 1)) / tiles;

		auto opt_local = st->opt;
		opt_local.affinity = static_cast<uint32_t>(t % T);

		Exec ex2{st}; 
		this->submit_impl(small_function<void()>{[ex2, lo, hi]() mutable {
							ex2(Range{lo, hi});
						  }},
						  opt_local);
	  }
	  return Handle{std::move(ctr)};
	}

	this->submit_impl(
		small_function<void()>{[ex, n]() mutable { ex(Range{0, n}); }},
		st->opt);
	return Handle{std::move(ctr)};
  }


  /**
   * @brief Combine multiple handles; returns a handle that completes when all
   * do.
   */
  Handle combine(std::span<const Handle> hs, SubmitOptions opt = {});
  /// Convenience overload from initializer_list.
  Handle combine(std::initializer_list<Handle> hs, SubmitOptions opt = {}) {
	return combine(std::span<const Handle>(hs.begin(), hs.size()),
				   std::move(opt));
  }
  /**
   * @brief Block the caller until @p h completes.
   */
  void wait(const Handle& h);
  /**
   * @brief Block until the pool has no in-flight work (best effort).
   */
  void wait_idle();

 private:
  /**
   * @struct Task
   * @brief Scheduled unit of work kept in worker deques or central queues.
   *
   * @var fn Small-function `void()`.
   * @var prio Priority of the task.
   * @var owned Whether the pool owns the task object lifetime.
   * @var done Optional shared counter to decrement on completion.
   */
  struct Task {
	small_function<void()> fn;
	Priority prio{Priority::Normal};
	bool owned{true};
	std::shared_ptr<Handle::Counter> done{};
  };
  /**
   * @struct Worker
   * @brief Per-thread worker state with two deques and idle backoff.
   *
   * @var deq_hi High-priority owner deque.
   * @var deq_lo Normal-priority owner deque.
   * @var rng RNG used for selecting steal victims.
   * @var mu/cv Synchronization primitives for parking/notification.
   * @var backoff_us Current idle backoff duration.
   */
  struct Worker {
	detail::chase_lev_deque<Task*> deq_hi;
	detail::chase_lev_deque<Task*> deq_lo;
	std::mt19937 rng;
	std::mutex mu;
	std::condition_variable cv;
	uint32_t backoff_us{};
  };
  /**
   * @struct CentralShard
   * @brief Central MPMC queues for high/low priority tasks.
   */
  struct CentralShard {
	detail::mpmc_queue<Task*> hi;
	detail::mpmc_queue<Task*> lo;
  };

  /**
   * @brief Core submit implementation taking a small_function job.
   */
  Handle submit_impl(small_function<void()> job, SubmitOptions opt);

  /**
   * @brief Dispatch a task either to a specific worker (affinity) or centrally.
   */
  void dispatch(Task* t, std::optional<uint32_t> affinity, 
				bool rate_limit_notify);

  /**
   * @brief Main loop for worker @p id.
   */
  void worker_loop(uint32_t id);

  /**
   * @brief Attempt to help execute one task from other queues (steal).
   * @return true if a task was helped/executed.
   */
  bool try_help_one(uint32_t id);

  /**
   * @brief Decrement a handle counter and notify waiting threads if it reaches
   * zero.
   */
  static void complete_counter(Handle::Counter* ctr);

  /**
   * @brief Check whether all queues are empty (best effort).
   */
  bool all_empty() const;

  /**
   * @brief Wake up worker @p id.
   */
  void notify_worker(uint32_t id);

  /**
   * @brief Pick a central shard index in round-robin fashion.
   */
  uint32_t pick_queue();

  void notify_all_workers();


  /// TLS: current worker id (UINT32_MAX if not in pool thread).
  static thread_local uint32_t tls_id_;

  /// TLS: whether the current thread is a pool worker.
  static thread_local bool tls_in_pool_;

  /// Configuration.
  Config cfg_;

  /// Worker threads.
  std::vector<std::thread> threads_;

  /// Per-worker state.
  std::vector<std::unique_ptr<Worker>> workers_;

  /// Central shards (MPMC queues).
  std::vector<std::unique_ptr<CentralShard>> centrals_;

  /// Stop flag shared by workers.
  alignas(64) std::atomic<bool> stop_{false};

  /// Counters for diagnostics.
  alignas(64) std::atomic<uint64_t> submitted_{0};
  alignas(64) std::atomic<uint32_t> submit_tick_{0};
  alignas(64) std::atomic<uint64_t> executed_{0};
  alignas(64) std::atomic<uint64_t> stolen_{0};

  /// Round-robin counter for central shards.
  alignas(64) std::atomic<uint32_t> rr_{0};

  /// Number of in-flight tasks (for wait_idle).
  alignas(64) std::atomic<uint32_t> inflight_{0};

  /// Global wait condition for `wait_idle`.
  mutable std::mutex wait_mu_;
  std::condition_variable wait_cv_;
};

}  // namespace tp
