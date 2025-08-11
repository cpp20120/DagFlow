/** \file task_scope.hpp
 * \brief Task scope for managing and scheduling tasks in a thread pool.
 *
 * This file defines the TaskScope class, which provides a high-level interface
 * for submitting tasks with dependencies and scheduling them in a thread pool
 * using a task graph.
 */

#pragma once
#include <functional>
#include <initializer_list>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "task_graph.hpp"
#include "thread_pool.hpp"

namespace tp {

/** \struct ScheduleOptions
 * \brief Options for scheduling tasks in the task scope.
 */
struct ScheduleOptions {
  Priority priority = Priority::Normal; /**< \brief Task priority. */
  std::optional<uint32_t> affinity{};	/**< \brief Optional worker affinity. */
  int concurrency = 1;					/**< \brief Maximum concurrency. */
  std::size_t capacity = SIZE_MAX;		/**< \brief Inbox capacity. */
  TaskGraph::Overflow overflow =
	  TaskGraph::Overflow::Block; /**< \brief Overflow policy. */
};

/** \class JobHandle
 * \brief Handle for identifying a job in the task scope.
 */
class JobHandle {
 public:
  /** \brief Default constructor, initializes an invalid handle. */
  JobHandle() = default;

  /** \brief Constructs a job handle with a specific ID.
   *
   * \param id The job ID.
   */
  explicit JobHandle(std::size_t id) : id_(id) {}

  /** \brief Checks if the handle is valid.
   *
   * \return True if the handle is valid, false otherwise.
   */
  [[nodiscard]] bool valid() const noexcept { return id_ != npos; }

  /** \brief Gets the job ID.
   *
   * \return The job ID.
   */
  [[nodiscard]] size_t id() const noexcept { return id_; }

 private:
  size_t id_ = npos; /**< \brief The job ID. */
  static constexpr size_t npos =
	  static_cast<size_t>(-1); /**< \brief Invalid job ID. */
  friend class TaskScope;
};

/** \class TaskScope
 * \brief Manages a scope for submitting and scheduling tasks with dependencies.
 */
class TaskScope {
 public:
  /** \brief Constructs a task scope associated with a thread pool.
   *
   * \param pool The thread pool to use for execution.
   */
  explicit TaskScope(Pool& pool) : pool_(pool), graph_(pool) {}

  /** \brief Submits a task to the scope.
   *
   * \tparam F The type of the callable.
   * \param f The callable to execute.
   * \param opt Scheduling options.
   * \return A handle to the submitted job.
   */
  template <class F>
  JobHandle submit(F&& f, const ScheduleOptions& opt = {}) {
	tp::TaskGraph::NodeOptions nopt;
	nopt.priority = opt.priority;
	nopt.affinity = opt.affinity;
	nopt.concurrency = opt.concurrency <= 0 ? INT_MAX : opt.concurrency;
	nopt.capacity = opt.capacity;
	nopt.overflow = opt.overflow;

	auto nid = graph_.add_node(std::forward<F>(f), nopt);
	return JobHandle{nid.idx};
  }

  /** \brief Submits a task that depends on another job.
   *
   * \tparam F The type of the callable.
   * \param dep The dependency job handle.
   * \param f The callable to execute.
   * \param opt Scheduling options.
   * \return A handle to the submitted job.
   */
  template <class F>
  JobHandle then(JobHandle dep, F&& f, const ScheduleOptions& opt = {}) {
	auto h = submit(std::forward<F>(f), opt);
	graph_.add_edge({dep.id()}, {h.id()});
	return h;
  }

  /** \brief Submits a task that depends on all specified jobs.
   *
   * \tparam F The type of the callable.
   * \param deps The span of dependency job handles.
   * \param f The callable to execute.
   * \param opt Scheduling options.
   * \return A handle to the submitted job.
   */
  template <class F>
  JobHandle when_all(std::span<const JobHandle> deps, F&& f,
					 const ScheduleOptions& opt = {}) {
	auto h = submit(std::forward<F>(f), opt);
	for (auto& d : deps) graph_.add_edge({d.id()}, {h.id()});
	return h;
  }

  /** \brief Submits a task that depends on all specified jobs (initializer
   * list).
   *
   * \tparam F The type of the callable.
   * \param deps The initializer list of dependency job handles.
   * \param f The callable to execute.
   * \param opt Scheduling options.
   * \return A handle to the submitted job.
   */
  template <class F>
  JobHandle when_all(std::initializer_list<JobHandle> deps, F&& f,
					 const ScheduleOptions& opt = {}) {
	return when_all(std::span<const JobHandle>(deps.begin(), deps.size()),
					std::forward<F>(f), opt);
  }

  /** \brief Submits a parallel-for task over a range.
   *
   * \tparam It The iterator type.
   * \tparam F The callable type.
   * \param begin The start of the range.
   * \param end The end of the range.
   * \param f The callable to apply to each element.
   * \param opt Scheduling options.
   * \return A handle to the submitted job.
   */
  template <class It, class F>
  JobHandle parallel_for(It begin, It end, F&& f,
						 const ScheduleOptions& opt = {}) {
	struct Block {
	  It begin, end;
	  std::decay_t<F> fn;
	};
	auto blk = std::make_shared<Block>(Block{begin, end, std::forward<F>(f)});

	return submit(
		[this, blk]() {
		  auto h = pool_.for_each(blk->begin, blk->end, blk->fn);
		  pool_.wait(h);
		},
		opt);
  }

  /** \brief Runs the task graph.
   *
   * \return A handle to track the execution.
   */
  Handle run() {
	if (ran_) return handle_;
	handle_ = graph_.run();
	ran_ = true;
	return handle_;
  }

  /** \brief Waits for all tasks in the scope to complete. */
  void wait() {
	if (!ran_) run();
	if (handle_.valid()) pool_.wait(handle_);
	last_error_ = graph_.last_error();

	ran_ = false;
	handle_ = {};
  }

  /** \brief Gets the last error encountered during execution.
   *
   * \return The exception pointer or nullptr if no error.
   */
  [[nodiscard]] std::exception_ptr last_error() const noexcept {
	return last_error_;
  }

  /** \brief Runs the task graph and waits for completion, rethrowing errors. */
  void run_and_wait() {
	run();
	wait();
	if (auto ep = last_error_) std::rethrow_exception(ep);
  }

  /** \brief Clears the task scope. */
  void clear() {
	ensure_not_running_ended();
	graph_.clear();
	last_error_ = nullptr;
  }

  /** \brief Destroys the task scope, running and waiting if necessary. */
  ~TaskScope() {
	try {
	  if (!ran_ && graph_.size() > 0) run();
	  if (ran_) wait();
	} catch (...) {
	}
  }

 private:
  /** \brief Ensures the scope is not in a running state. */
  void ensure_not_running_ended() {
	if (ran_) {
	  if (handle_.valid()) pool_.wait(handle_);
	  last_error_ = graph_.last_error();
	  ran_ = false;
	  handle_ = Handle{};
	}
  }

  Pool& pool_;		/**< \brief The associated thread pool. */
  TaskGraph graph_; /**< \brief The underlying task graph. */
  Handle handle_{}; /**< \brief The execution handle. */
  bool ran_{false}; /**< \brief Whether the graph has been run. */
  std::exception_ptr last_error_{}; /**< \brief The last execution error. */
};

}  // namespace tp