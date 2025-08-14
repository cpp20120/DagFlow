#pragma once
/**
 * @file task_scope.hpp
 * @brief High-level task scope that builds small dependency graphs and runs
 * them on a thread pool.
 *
 * @details
 * The TaskScope class provides a convenient, high-level façade over the
 * lower-level TaskGraph and Pool components. It lets you:
 *  - submit standalone nodes (`submit`);
 *  - declare a dependency where B runs after A (`then`);
 *  - fan-in multiple prerequisites into a continuation (`when_all`);
 *  - run a tokenized parallel loop over an iterator range (`parallel_for` and
 * `parallel_for_after`);
 *  - run and wait the graph (`run`, `wait`, `run_and_wait`);
 *  - clear the scope (`clear`).
 *
 * Execution model:
 *  - Each submitted node is backed by an internal TaskGraph node that is fed
 * with tokens. One token == one execution of the node's body. By default a
 * submitted node is primed with one token (so it executes once).
 *  - Dependencies are edges in the graph; a dependent node is primed with its
 * tokens only when all of its predecessors complete.
 *
 * Error and cancellation model:
 *  - When a task throws, the first exception is captured and the run is
 * cancelled (no new work is scheduled). `run_and_wait()` will rethrow that
 * first exception.
 *
 * Thread-safety:
 *  - A single TaskScope instance is intended to be built from one thread.
 *    The underlying execution is multi-threaded in the Pool.
 */

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <memory>
#include <numeric>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "task_graph.hpp"
#include "thread_pool.hpp"

namespace tp {

/**
 * @struct ScheduleOptions
 * @brief Per-node scheduling options used when creating TaskGraph nodes via
 * TaskScope.
 *
 * @var ScheduleOptions::priority
 * Priority of the task. High priority tasks tend to be scheduled sooner.
 *
 * @var ScheduleOptions::affinity
 * Optional worker affinity. When present, the pool tries to dispatch work to
 * the specified worker. The interpretation of the worker identifier is
 * implementation-defined.
 *
 * @var ScheduleOptions::concurrency
 * Maximum number of concurrent executions of this node. Values <= 0 are treated
 * as unlimited (INT_MAX).
 *
 * @var ScheduleOptions::capacity
 * Maximum number of enqueued tokens for the node's inbox. This imposes
 * back-pressure.
 *
 * @var ScheduleOptions::overflow
 * Behavior when the inbox capacity would be exceeded. See TaskGraph::Overflow.
 */
struct ScheduleOptions {
  Priority priority = Priority::Normal;
  std::optional<uint32_t> affinity{};
  int concurrency = 1;
  std::size_t capacity = SIZE_MAX;
  TaskGraph::Overflow overflow = TaskGraph::Overflow::Block;
};

/**
 * @class JobHandle
 * @brief Lightweight identifier for a node submitted through TaskScope.
 *
 * @details
 * This handle is used to express dependencies between nodes by passing it to
 * `then` and `when_all`. It is not a synchronization primitive; for
 * synchronization use TaskScope::wait() or the Handle returned by run().
 */
class JobHandle {
 public:
  /// Constructs an invalid handle.
  JobHandle() = default;

  /// Constructs a handle referencing the internal node index @p id.
  explicit JobHandle(std::size_t id) : id_(id) {}

  /// @return true if this handle refers to a real node.
  [[nodiscard]] bool valid() const noexcept { return id_ != npos; }

  /// @return underlying node identifier.
  [[nodiscard]] std::size_t id() const noexcept { return id_; }

 private:
  /// Internal node id or npos if invalid.
  std::size_t id_ = npos;

  /// Sentinel for an invalid handle.
  static constexpr std::size_t npos = static_cast<std::size_t>(-1);

  /// TaskScope needs access to internal id to wire dependencies.
  friend class TaskScope;
};

/**
 * @class TaskScope
 * @brief Builder and runner for a single execution of a dependency graph on top
 * of a Pool.
 *
 * @details
 * Usage workflow:
 *  1. Create TaskScope with a reference to an existing Pool.
 *  2. Submit nodes and dependencies using `submit`, `then`, `when_all`, or
 * `parallel_for`.
 *  3. Call `run()` to kick off execution. Optionally call `wait()` to block
 * until done, or use `run_and_wait()` to both run and rethrow the first
 * captured exception, if any.
 *  4. Optionally `clear()` to reuse the scope.
 *
 * Invariants:
 *  - Mutating operations on the underlying graph are not allowed while it is
 * running.
 *  - The destructor attempts to run and wait any pending graph to avoid
 * dropping work silently.
 */
class TaskScope {
 public:
  /// Construct a TaskScope bound to a Pool.
  explicit TaskScope(Pool& pool) : pool_(pool), graph_(pool) {}

  /**
   * @brief Submit a single independent node that runs once.
   * @tparam F Callable type, invoked as `void()`.
   * @param f   Task body to execute. It will be moved into an internal
   * small_function.
   * @param opt Scheduling options for the created node.
   * @return    JobHandle identifying the created node.
   *
   * @note The node is primed with one token (thus runs once) unless you
   * explicitly modify tokens later in TaskGraph.
   */
  template <class F>
  JobHandle submit(F&& f, const ScheduleOptions& opt = {}) {
	TaskGraph::NodeOptions nopt;
	nopt.priority = opt.priority;
	nopt.affinity = opt.affinity;
	nopt.concurrency = (opt.concurrency <= 0 ? INT_MAX : opt.concurrency);
	nopt.capacity = opt.capacity;
	nopt.overflow = opt.overflow;

	auto nid = graph_.add_node(std::forward<F>(f), nopt);
	graph_.set_tokens(nid, 1);
	return JobHandle{nid.idx};
  }

  /**
   * @brief Create a node that depends on @p dep and runs after it.
   *
   * @tparam F Callable type, invoked as `void()`.
   * @param dep Dependency handle returned by a previous submit/then/when_all.
   * @param f   Task body to execute after @p dep completes.
   * @param opt Scheduling options for the created node.
   * @return    JobHandle of the new node.
   */
  template <class F>
  JobHandle then(JobHandle dep, F&& f, const ScheduleOptions& opt = {}) {
	auto h = submit(std::forward<F>(f), opt);
	graph_.add_edge({dep.id()}, {h.id()});
	return h;
  }

  /**
   * @brief Fan-in: create a node that runs after all @p deps complete.
   *
   * @tparam F Callable type, invoked as `void()`.
   * @param deps Span of dependency handles.
   * @param f    Continuation task body.
   * @param opt  Scheduling options for the created node.
   * @return     JobHandle of the new node.
   */
  template <class F>
  JobHandle when_all(std::span<const JobHandle> deps, F&& f,
					 const ScheduleOptions& opt = {}) {
	auto h = submit(std::forward<F>(f), opt);
	for (auto& d : deps) graph_.add_edge({d.id()}, {h.id()});
	return h;
  }

  /**
   * @brief Convenience overload of when_all() taking an initializer list.
   *
   * @tparam F Callable type, invoked as `void()`.
   * @param deps Initializer list of dependency handles.
   * @param f    Continuation task body.
   * @param opt  Scheduling options for the created node.
   * @return     JobHandle of the new node.
   */
  template <class F>
  JobHandle when_all(std::initializer_list<JobHandle> deps, F&& f,
					 const ScheduleOptions& opt = {}) {
	return when_all(std::span<const JobHandle>(deps.begin(), deps.size()),
					std::forward<F>(f), opt);
  }

  /**
   * @brief Tokenized parallel-for over [begin, end), chunked and dispatched as
   * tokens.
   *
   * @tparam It Iterator type. Both input and random-access iterators are
   * supported.
   * @tparam F  Callable type invoked for each element as `void(T&)`.
   * @param begin Iterator to the first element.
   * @param end   Iterator to one past the last element.
   * @param f     Element processing function; captured by move into tasks.
   * @param opt   Scheduling options. Concurrency is clamped to at least 1;
   * capacity is set to number of chunks.
   * @return      JobHandle of the created node. Its tokens equal the number of
   * chunks computed.
   *
   * @details
   * The input range is split into contiguous chunks (target chunk size ~ 16K
   * elements). Each token processes exactly one chunk by sequentially calling
   * `f(*it)` for its subrange. The node's inbox capacity is set to the number
   * of chunks and the overflow behavior applies when enqueuing tokens.
   */
  template <class It, class F>
  JobHandle parallel_for(It begin, It end, F&& f,
						 const ScheduleOptions& opt = {}) {
	using FnType = std::decay_t<F>;
	struct Block {
	  It b, e;
	  FnType fn;
	};
	struct Parts {
	  std::vector<Block> blocks;
	  std::atomic<std::size_t> next{0};
	};
	auto parts = std::make_shared<Parts>();

	const std::size_t n = static_cast<std::size_t>(std::distance(begin, end));
	if (n == 0) return submit([] {}, opt);

	const std::size_t target = 1 << 14;
	const std::size_t chunks =
		std::max<std::size_t>(1, (n + target - 1) / target);

	parts->blocks.reserve(chunks);
	for (std::size_t c = 0; c < chunks; ++c) {
	  std::size_t lo = c * n / chunks;
	  std::size_t hi = (c + 1) * n / chunks;

	  It bb = begin;
	  std::advance(bb, static_cast<long>(lo));
	  It ee = begin;
	  std::advance(ee, static_cast<long>(hi));

	  parts->blocks.push_back(Block{bb, ee, std::forward<F>(f)});
	}

	TaskGraph::NodeOptions nopt;
	nopt.priority = opt.priority;
	nopt.affinity = opt.affinity;
	nopt.concurrency = std::max(1, opt.concurrency);
	nopt.capacity = chunks;
	nopt.overflow = opt.overflow;

	auto nid = graph_.add_node(
		[parts]() {
		  const auto i = parts->next.fetch_add(1, std::memory_order_relaxed);
		  if (i >= parts->blocks.size()) return;
		  auto& p = parts->blocks[i];
		  for (auto it = p.b; it != p.e; ++it) p.fn(*it);
		},
		nopt);

	graph_.set_tokens(nid, parts->blocks.size());
	return JobHandle{nid.idx};
  }

  template <class F>
  inline Handle for_each_index_ws(Pool& p, std::size_t n, F f,
								  SubmitOptions opt = {},
								  std::size_t min_grain = (1u << 14)) {
	struct It {
	  using iterator_category = std::random_access_iterator_tag;
	  using value_type = std::size_t;
	  using difference_type = std::ptrdiff_t;
	  std::size_t i;
	  std::size_t operator*() const { return i; }
	  It& operator++() {
		++i;
		return *this;
	  }
	  It operator+(std::ptrdiff_t d) const {
		return It{i + static_cast<std::size_t>(d)};
	  }
	  std::ptrdiff_t operator-(It o) const {
		return static_cast<std::ptrdiff_t>(i) -
			   static_cast<std::ptrdiff_t>(o.i);
	  }
	  bool operator!=(It o) const { return i != o.i; }
	};
	return p.for_each_ws(
		It{0}, It{n}, [g = std::move(f)](std::size_t i) { g(i); }, opt,
		min_grain);
  }


  /**
   * @brief Tokenized parallel-for that depends on @p dep.
   *
   * @tparam It Iterator type.
   * @tparam F  Callable `void(T&)`.
   * @param dep  Dependency that must complete before this loop runs.
   * @param begin Range begin.
   * @param end   Range end.
   * @param f     Element functor.
   * @param opt   Scheduling options.
   * @return      JobHandle of the created node.
   */
  template <class It, class F>
  JobHandle parallel_for_after(JobHandle dep, It begin, It end, F&& f,
							   const ScheduleOptions& opt = {}) {
	auto h = parallel_for(begin, end, std::forward<F>(f), opt);
	graph_.add_edge({dep.id()}, {h.id()});
	return h;
  }

  /**
   * @brief Run the graph once. If already run and not yet waited, returns the
   * same handle.
   * @return A Handle that can be waited on with Pool::wait().
   *
   * @throws std::logic_error if a cycle is detected (via TaskGraph::run()).
   */
  Handle run() {
	if (ran_) return handle_;
	handle_ = graph_.run();
	ran_ = true;
	return handle_;
  }

  /**
   * @brief Wait for the last run to complete. Captures the last error from
   * TaskGraph.
   *
   * If `run()` was not called yet but there is pending work, it is called
   * implicitly. The method does not rethrow exceptions; use `run_and_wait()` if
   * you want the first captured exception to be rethrown.
   */
  void wait() {
	if (!ran_) run();
	if (handle_.valid()) pool_.wait(handle_);
	last_error_ = graph_.last_error();
	ran_ = false;
	handle_ = {};
  }

  /**
   * @brief Convenience method that runs, waits, and rethrows the first captured
   * exception.
   * @throws the first exception thrown in any task during the last run, if any.
   */
  void run_and_wait() {
	run();
	wait();
	if (auto ep = last_error_) std::rethrow_exception(ep);
  }

  /// @return The first captured exception from the last execution, or nullptr
  /// if none.
  [[nodiscard]] std::exception_ptr last_error() const noexcept {
	return last_error_;
  }

  /**
   * @brief Clear the current DAG so the scope can be reused.
   *
   * Requires that the graph is not running; if a previous run is in progress
   * this function waits for it to finish. It also resets the last recorded
   * error.
   */
  void clear() {
	ensure_not_running_ended();
	graph_.clear();
	last_error_ = nullptr;
  }

  /**
   * @brief Destructor attempts to gracefully finish any pending run.
   *
   * If the graph has nodes but was not run, it will be run and waited. All
   * exceptions are swallowed here to avoid throwing from a destructor.
   */
  ~TaskScope() {
	try {
	  if (!ran_ && graph_.size() > 0) run();
	  if (ran_) wait();
	} catch (...) {
	}
  }

 private:
  /// Ensure no pending run is in progress; wait it out and collect last_error_.
  void ensure_not_running_ended() {
	if (ran_) {
	  if (handle_.valid()) pool_.wait(handle_);
	  last_error_ = graph_.last_error();
	  ran_ = false;
	  handle_ = Handle{};
	}
  }

  /// Reference to the underlying pool used for execution.
  Pool& pool_;

  /// The underlying TaskGraph managed by this scope.
  TaskGraph graph_;

  /// The handle of the last run, if any.
  Handle handle_{};

  /// Whether a run is currently active and has not been waited yet.
  bool ran_{false};

  /// Last exception captured by the underlying TaskGraph during the last run.
  std::exception_ptr last_error_{};
};

}  // namespace tp
