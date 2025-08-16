#pragma once
#include <atomic>
#include <functional>
#include <type_traits>
#include <vector>

namespace dagflow {
/** \class ThreadPool
 * \brief Forward declaration of the ThreadPool class.
 */
class ThreadPool;  // fwd
/** \class Task
 * \brief Represents a task with dependencies for execution in a thread pool.
 */
class Task {
 public:
  /** \brief Default constructor, initializes an empty task. */
  Task() = default;
  /** \brief Constructs a task from a callable.
   *
   * \tparam F The type of the callable.
   * \param f The callable to execute.
   */
  template <typename F,
			typename = std::enable_if_t<std::is_invocable_r_v<void, F>>>
  explicit Task(F&& f) : func_(std::forward<F>(f)) {}
  /** \brief Deleted copy constructor to prevent copying. */
  Task(const Task&) = delete;
  /** \brief Deleted copy assignment operator to prevent copying. */
  Task& operator=(const Task&) = delete;
  /** \brief Move constructor. */
  Task(Task&&) = default;
  /** \brief Move assignment operator. */
  Task& operator=(Task&&) = default;

  /** \brief Specifies that this task must precede other tasks.
   *
   * \tparam Tasks The types of the tasks.
   * \param ts The tasks that depend on this task.
   */
  template <typename... Tasks>
  void precede(Tasks*... ts) {
	(precede_one(ts), ...);
  }
  /** \brief Specifies that this task must succeed other tasks.
   *
   * \tparam Tasks The types of the tasks.
   * \param ts The tasks that this task depends on.
   */
  template <typename... Tasks>
  void succeed(Tasks*... ts) {
	(succeed_one(ts), ...);
  }
  /** \brief Attempts to cancel the task.
   *
   * \return True if the task was cancelled, false if it was already invoked.
   */
  bool cancel() {
	return (flags_.fetch_or(kCancelled, std::memory_order_acq_rel) &
			kInvoked) == 0;
  }
  /** \brief Resets the task's state. */
  void reset() { flags_.store(0, std::memory_order_relaxed); }

 private:
  friend class ThreadPool;
  /** \brief Adds a single task as a successor.
   *
   * \param other The task to add as a successor.
   */
  void precede_one(Task* other) {
	next_.push_back(other);
	other->total_preds_++;
	other->remain_preds_.fetch_add(1, std::memory_order_relaxed);
  }
  /** \brief Adds a single task as a predecessor.
   *
   * \param other The task to add as a predecessor.
   */
  void succeed_one(Task* other) {
	other->next_.push_back(this);
	total_preds_++;
	remain_preds_.fetch_add(1, std::memory_order_relaxed);
  }

  static constexpr int kCancelled = 1; /**< \brief Flag for cancelled state. */
  static constexpr int kInvoked = 1 << 1; /**< \brief Flag for invoked state. */

  std::function<void()> func_{}; /**< \brief The callable to execute. */
  std::vector<Task*> next_{};	 /**< \brief Successor tasks. */
  int total_preds_{0};			 /**< \brief Total number of predecessors. */
  alignas(64) std::atomic<int> remain_preds_{
	  0};								  /**< \brief Remaining predecessors. */
  alignas(64) std::atomic<int> flags_{0}; /**< \brief Task state flags. */
};

}  // namespace tp
