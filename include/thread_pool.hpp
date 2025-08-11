/** \file pool.hpp
 * \brief Thread pool implementation with work-stealing and task prioritization.
 *
 * This file defines a thread pool that supports task submission with
 * priorities, work-stealing, and thread affinity.
 */

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

/** \brief Task priority for scheduling. */
enum class Priority : uint8_t {
  High = 0,	 /**< \brief High priority tasks. */
  Normal = 1 /**< \brief Normal priority tasks. */
};

/** \struct Config
 * \brief Configuration for the thread pool.
 */
struct Config {
  uint32_t threads = std::thread::hardware_concurrency(); /**< \brief Number of
															 worker threads. */
  uint32_t shards =
	  0; /**< \brief Number of central shards (0 => equals threads). */
  uint32_t central_batch = 64; /**< \brief Max tasks pulled per batch. */
  uint32_t idle_us_min = 50;   /**< \brief Idle backoff floor (microseconds). */
  uint32_t idle_us_max =
	  200;					/**< \brief Idle backoff ceiling (microseconds). */
  bool pin_threads = false; /**< \brief Pin workers to cores if true. */
};

/** \struct SubmitOptions
 * \brief Options for task submission.
 */
struct SubmitOptions {
  std::optional<uint32_t>
	  affinity; /**< \brief Optional worker affinity (worker ID). */
  Priority priority = Priority::Normal; /**< \brief Task priority. */
  bool owned = true; /**< \brief Whether the pool owns the task object. */
};

/** \class Handle
 * \brief Handle for tracking task completion.
 */
class Handle {
 public:
  /** \struct Counter
   * \brief Counter for tracking task completion.
   */
  struct Counter {
	std::atomic<int> count{0};	/**< \brief Number of tasks remaining. */
	std::mutex mu;				/**< \brief Mutex for condition variable. */
	std::condition_variable cv; /**< \brief Condition variable for waiting. */
  };

  /** \brief Default constructor, initializes an invalid handle. */
  Handle() noexcept = default;

  /** \brief Constructs a handle with a counter.
   *
   * \param c The counter to track.
   */
  explicit Handle(std::shared_ptr<Counter> c) noexcept : ctr_(std::move(c)) {}

  /** \brief Checks if the handle is valid.
   *
   * \return True if the handle is valid, false otherwise.
   */
  bool valid() const noexcept { return static_cast<bool>(ctr_); }

  /** \brief Gets the underlying counter.
   *
   * \return The counter pointer.
   */
  std::shared_ptr<Counter> get() const noexcept { return ctr_; }

 private:
  std::shared_ptr<Counter>
	  ctr_{}; /**< \brief The counter for task completion. */
  friend class Pool;
};

using Fn = void (*)(void*); /**< \brief Function pointer type for tasks. */

/** \class Pool
 * \brief A thread pool with work-stealing and task prioritization.
 */
class Pool {
 public:
  /** \brief Constructs a thread pool with the specified configuration.
   *
   * \param cfg The configuration for the thread pool.
   */
  explicit Pool(const Config& cfg = {});

  /** \brief Destroys the thread pool, stopping all workers. */
  ~Pool();

  /** \brief Deleted copy constructor to prevent copying. */
  Pool(const Pool&) = delete;

  /** \brief Deleted copy assignment operator to prevent copying. */
  Pool& operator=(const Pool&) = delete;

  /** \brief Submits a task to the thread pool.
   *
   * \param fn The function to execute.
   * \param arg The argument to pass to the function.
   * \param opt Submission options.
   * \return A handle to track the task.
   */
  Handle submit(Fn fn, void* arg = nullptr, SubmitOptions opt = {});

  /** \brief Submits a callable task to the thread pool.
   *
   * \tparam F The type of the callable.
   * \param f The callable to execute.
   * \param opt Submission options.
   * \return A handle to track the task.
   */
  template <class F>
  Handle submit(F f, SubmitOptions opt = {}) {
	return submit_impl(small_function<void()>{[g = std::move(f)] { g(); }},
					   std::move(opt));
  }

  /** \brief Submits tasks for each element in a range.
   *
   * \tparam It The iterator type.
   * \tparam F The callable type.
   * \param begin The start of the range.
   * \param end The end of the range.
   * \param f The callable to apply to each element.
   * \param opt Submission options.
   * \return A handle to track the tasks.
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

  /** \brief Combines multiple handles into a single handle.
   *
   * \param hs The span of handles to combine.
   * \param opt Submission options.
   * \return A handle to track the combined tasks.
   */
  Handle combine(std::span<const Handle> hs, SubmitOptions opt = {});

  /** \brief Combines multiple handles into a single handle (initializer list).
   *
   * \param hs The initializer list of handles.
   * \param opt Submission options.
   * \return A handle to track the combined tasks.
   */
  Handle combine(std::initializer_list<Handle> hs, SubmitOptions opt = {}) {
	return combine(std::span<const Handle>(hs.begin(), hs.size()),
				   std::move(opt));
  }

  /** \brief Waits for a handle to complete.
   *
   * \param h The handle to wait for.
   */
  void wait(const Handle& h);

  /** \brief Waits for the pool to become idle. */
  void wait_idle();

 private:
  /** \struct Task
   * \brief Represents a task in the thread pool.
   */
  struct Task {
	small_function<void()> fn;		 /**< \brief The callable to execute. */
	Priority prio{Priority::Normal}; /**< \brief Task priority. */
	bool owned{true}; /**< \brief Whether the pool owns the task. */
	std::shared_ptr<Handle::Counter> done{}; /**< \brief Completion counter. */
  };

  /** \struct Worker
   * \brief Represents a worker thread's state.
   */
  struct Worker {
	detail::chase_lev_deque<Task*>
		deq_hi; /**< \brief High-priority task deque. */
	detail::chase_lev_deque<Task*>
		deq_lo;		  /**< \brief Low-priority task deque. */
	std::mt19937 rng; /**< \brief Random number generator for work-stealing. */
	std::mutex mu;	  /**< \brief Mutex for condition variable. */
	std::condition_variable cv; /**< \brief Condition variable for worker. */
	uint32_t backoff_us{};		/**< \brief Backoff time in microseconds. */
  };

  /** \struct CentralShard
   * \brief Central queue shard for tasks.
   */
  struct CentralShard {
	detail::mpmc_queue<Task*> hi; /**< \brief High-priority task queue. */
	detail::mpmc_queue<Task*> lo; /**< \brief Low-priority task queue. */
  };

  /** \brief Submits a task implementation.
   *
   * \param job The callable to execute.
   * \param opt Submission options.
   * \return A handle to track the task.
   */
  Handle submit_impl(small_function<void()> job, SubmitOptions opt);

  /** \brief Dispatches a task to a worker.
   *
   * \param t The task to dispatch.
   * \param affinity Optional worker affinity.
   */
  void dispatch(Task* t, std::optional<uint32_t> affinity);

  /** \brief Main loop for a worker thread.
   *
   * \param id The worker's ID.
   */
  void worker_loop(uint32_t id);

  /** \brief Attempts to steal and execute one task.
   *
   * \param id The worker's ID.
   * \return True if a task was executed, false otherwise.
   */
  bool try_help_one(uint32_t id);

  /** \brief Completes a counter when a task finishes.
   *
   * \param ctr The counter to update.
   */
  static void complete_counter(Handle::Counter* ctr);

  /** \brief Checks if all queues are empty.
   *
   * \return True if all queues are empty, false otherwise.
   */
  bool all_empty() const;

  /** \brief Notifies a worker to check for tasks.
   *
   * \param id The worker's ID.
   */
  void notify_worker(uint32_t id);

  /** \brief Picks a central queue for work-stealing.
   *
   * \return The index of the selected queue.
   */
  uint32_t pick_queue();

  static thread_local uint32_t tls_id_; /**< \brief Thread-local worker ID. */
  static thread_local bool
	  tls_in_pool_; /**< \brief Whether the thread is in the pool. */

  Config cfg_;						 /**< \brief Thread pool configuration. */
  std::vector<std::thread> threads_; /**< \brief Worker threads. */
  std::vector<std::unique_ptr<Worker>> workers_; /**< \brief Worker states. */
  std::vector<std::unique_ptr<CentralShard>>
	  centrals_; /**< \brief Central queue shards. */
  alignas(64) std::atomic<bool> stop_{false}; /**< \brief Stop flag. */
  alignas(64) std::atomic<uint64_t> submitted_{
	  0}; /**< \brief Number of submitted tasks. */
  alignas(64) std::atomic<uint64_t> executed_{
	  0}; /**< \brief Number of executed tasks. */
  alignas(64) std::atomic<uint64_t> stolen_{
	  0}; /**< \brief Number of stolen tasks. */
  alignas(64) std::atomic<uint32_t> rr_{0}; /**< \brief Round-robin counter. */
  alignas(64) std::atomic<uint32_t> inflight_{
	  0};					   /**< \brief Number of inflight tasks. */
  mutable std::mutex wait_mu_; /**< \brief Mutex for waiting. */
  std::condition_variable
	  wait_cv_; /**< \brief Condition variable for waiting. */
};

}  // namespace tp