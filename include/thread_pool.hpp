#pragma once
#include <atomic>
#include <cassert>
#include <functional>
#include <new>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

/**
 * @brief Namespace for thread pool and work-stealing deque implementation.
 */
namespace thread_pool {

namespace internal {

/**
 * @brief Constant indicating a task has been cancelled.
 */
constexpr auto k_cancelled = 1;

/**
 * @brief Constant indicating a task has been invoked.
 */
constexpr auto k_invoked = 1 << 1;

/**
 * @brief A thread-safe array for storing pointers with atomic operations.
 * @tparam T The pointer type stored in the array.
 * @note Requires T to be a pointer type.
 */
template <typename T>
  requires std::is_pointer_v<T>
class array {
 public:
  /**
   * @brief Constructs an array with the specified capacity.
   * @param capacity The size of the array (must be a power of 2).
   */
  explicit array(const int capacity)
	  : capacity_{capacity},
		mask_{capacity - 1},
		buffer_{new std::atomic<T>[capacity]} {}

  array(const array&) = delete;
  array(array&&) = delete;
  array& operator=(const array&) = delete;
  array& operator=(array&&) = delete;

  /**
   * @brief Destructor that deallocates the buffer.
   */
  ~array() noexcept { delete[] buffer_; }

  /**
   * @brief Stores an item at the specified index in the array.
   * @param index The index where the item will be stored.
   * @param item The pointer to store.
   * @note Uses relaxed memory ordering for performance.
   */
  void put(const size_t index, T item) noexcept {
	buffer_[index & mask_].store(item, std::memory_order_relaxed);
  }

  /**
   * @brief Retrieves an item from the specified index in the array.
   * @param index The index from which to retrieve the item.
   * @return The pointer stored at the index.
   * @note Uses relaxed memory ordering for performance.
   */
  [[nodiscard]] T get(const size_t index) noexcept {
	return buffer_[index & mask_].load(std::memory_order_relaxed);
  }

  /**
   * @brief Resizes the array, copying elements from the old array.
   * @param bottom The starting index for copying.
   * @param top The ending index for copying.
   * @return A pointer to the new resized array.
   */
  [[nodiscard]] array* resize(const size_t bottom, const size_t top) {
	auto* new_array = new array{2 * capacity_};
	for (auto i = top; i != bottom; ++i) {
	  new_array->put(i, get(i));
	}
	return new_array;
  }

  /**
   * @brief Returns the capacity of the array.
   * @return The number of elements the array can hold.
   */
  [[nodiscard]] int capacity() const { return capacity_; }

 private:
  const int capacity_;	   /**< The capacity of the array. */
  const int mask_;		   /**< Bitmask for indexing (capacity - 1). */
  std::atomic<T>* buffer_; /**< The underlying buffer of atomic pointers. */
};

/**
 * @brief A work-stealing deque for task management in a thread pool.
 * @tparam T The pointer type stored in the deque.
 * @note Requires T to be a pointer type.
 */
template <typename T>
  requires std::is_pointer_v<T>
class work_stealing_deque {
 public:
  /**
   * @brief Constructs a work-stealing deque with the specified capacity.
   * @param capacity The initial capacity (must be a power of 2).
   */
  explicit work_stealing_deque(const int capacity = 1024)
	  : top_{0}, bottom_{0}, array_{new array<T>{capacity}} {
	assert(capacity && (capacity & (capacity - 1)) == 0);
	garbage_.reserve(64);
  }

  work_stealing_deque(const work_stealing_deque&) = delete;
  work_stealing_deque(work_stealing_deque&&) = delete;
  work_stealing_deque& operator=(const work_stealing_deque&) = delete;
  work_stealing_deque& operator=(work_stealing_deque&&) = delete;

  /**
   * @brief Destructor that cleans up the array and garbage-collected arrays.
   */
  ~work_stealing_deque() noexcept {
	for (auto* arr : garbage_) {
	  delete arr;
	}
	delete array_.load();
  }

  /**
   * @brief Pushes an item onto the deque.
   * @param item The pointer to push.
   * @note Resizes the underlying array if necessary.
   */
  void push(T item) {
	const auto bottom = bottom_.load(std::memory_order_relaxed);
	const auto top = top_.load(std::memory_order_acquire);
	auto* arr = array_.load(std::memory_order_relaxed);
	if (arr->capacity() - 1 < bottom - top) {
	  arr = resize(arr, bottom, top);
	}
	arr->put(bottom, item);
	bottom_.store(bottom + 1, std::memory_order_release);
  }

  /**
   * @brief Pops an item from the deque (owner thread).
   * @return The popped pointer, or nullptr if the deque is empty or a race
   * condition occurs.
   */
  [[nodiscard]] T pop() {
	const auto bottom = bottom_.fetch_sub(1, std::memory_order_seq_cst) - 1;
	assert(bottom >= -1);
	auto* arr = array_.load(std::memory_order_relaxed);
	auto top = top_.load(std::memory_order_seq_cst);
	if (top < bottom) {
	  return arr->get(bottom);
	}
	T item{nullptr};
	if (top == bottom) {
	  item = arr->get(bottom);
	  if (top_.compare_exchange_strong(top, top + 1, std::memory_order_seq_cst,
									   std::memory_order_relaxed)) {
		++top;
	  } else {
		item = nullptr;
	  }
	} else {
	  assert(top - bottom == 1);
	}
	bottom_.store(top, std::memory_order_relaxed);
	return item;
  }

  /**
   * @brief Steals an item from the deque (non-owner thread).
   * @return The stolen pointer, or nullptr if the deque is empty or a race
   * condition occurs.
   */
  [[nodiscard]] T steal() {
	auto top = top_.load(std::memory_order_seq_cst);
	if (const auto bottom = bottom_.load(std::memory_order_seq_cst);
		top >= bottom) {
	  return nullptr;
	}
	auto* arr = array_.load(std::memory_order_acquire);
	const auto item = arr->get(top);
	if (!top_.compare_exchange_strong(top, top + 1, std::memory_order_seq_cst,
									  std::memory_order_relaxed)) {
	  return nullptr;
	}
	return item;
  }

 private:
  /**
   * @brief Resizes the underlying array, moving items to a new array.
   * @param arr The current array.
   * @param bottom The bottom index of the deque.
   * @param top The top index of the deque.
   * @return A pointer to the new array.
   */
  [[nodiscard]] array<T>* resize(array<T>* arr, const size_t bottom,
								 const size_t top) {
	auto* tmp = arr->resize(bottom, top);
	garbage_.push_back(arr);
	std::swap(arr, tmp);
	array_.store(arr, std::memory_order_release);
	return arr;
  }

#ifdef __cpp_lib_hardware_interference_size
  alignas(std::hardware_destructive_interference_size) std::atomic<int> top_,
	  bottom_;
#else
  std::atomic<int> top_;	/**< The top index of the deque. */
  std::atomic<int> bottom_; /**< The bottom index of the deque. */
#endif
  std::atomic<array<T>*> array_; /**< The underlying array for storing items. */
  std::vector<array<T>*> garbage_; /**< Garbage-collected old arrays. */
};
}  // namespace internal

/**
 * @brief Represents a task with dependencies for execution in a thread pool.
 */
class task {
 public:
  /**
   * @brief Default constructor.
   */
  task() = default;

  /**
   * @brief Constructs a task with a callable function.
   * @tparam task_type The type of the callable (must be convertible to
   * std::function<void()>).
   * @param func The callable to execute.
   */
  template <typename task_type, typename = std::enable_if_t<std::convertible_to<
									task_type, std::function<void()>>>>
  explicit task(task_type&& func) : func_{std::forward<task_type>(func)} {}

  /**
   * @brief Copy constructor.
   * @param other The task to copy.
   */
  task(const task& other)
	  : total_predecessors_{other.total_predecessors_},
		func_{other.func_},
		next_{other.next_} {
	remaining_predecessors_.store(other.remaining_predecessors_);
	cancellation_flags_.store(other.cancellation_flags_);
  }

  /**
   * @brief Move constructor.
   * @param other The task to move.
   */
  task(task&& other) noexcept
	  : total_predecessors_{other.total_predecessors_},
		func_{std::move(other.func_)},
		next_{std::move(other.next_)} {
	remaining_predecessors_.store(other.remaining_predecessors_);
	cancellation_flags_.store(other.cancellation_flags_);
  }

  /**
   * @brief Copy assignment operator.
   * @param other The task to copy.
   * @return Reference to this task.
   */
  task& operator=(const task& other) {
	total_predecessors_ = other.total_predecessors_;
	remaining_predecessors_.store(other.remaining_predecessors_);
	cancellation_flags_.store(other.cancellation_flags_);
	func_ = other.func_;
	next_ = other.next_;
	return *this;
  }

  /**
   * @brief Move assignment operator.
   * @param other The task to move.
   * @return Reference to this task.
   */
  task& operator=(task&& other) noexcept {
	total_predecessors_ = other.total_predecessors_;
	remaining_predecessors_.store(other.remaining_predecessors_);
	cancellation_flags_.store(other.cancellation_flags_);
	func_ = std::move(other.func_);
	next_ = std::move(other.next_);
	return *this;
  }

  /**
   * @brief Destructor.
   */
  ~task() = default;

  /**
   * @brief Marks this task as a successor of another task.
   * @param other_task The predecessor task.
   */
  void succeed(task* other_task) {
	other_task->next_.push_back(this);
	++total_predecessors_;
	remaining_predecessors_.fetch_add(1);
  }

  /**
   * @brief Marks this task as a successor of multiple tasks.
   * @tparam tasks_type The types of the predecessor tasks.
   * @param other_task The first predecessor task.
   * @param tasks The remaining predecessor tasks.
   */
  template <typename... tasks_type>
  void succeed(task* other_task, const tasks_type&... tasks) {
	other_task->next_.push_back(this);
	++total_predecessors_;
	remaining_predecessors_.fetch_add(1);
	succeed(tasks...);
  }

  /**
   * @brief Marks this task as a predecessor of another task.
   * @param other_task The successor task.
   */
  void precede(task* other_task) {
	next_.push_back(other_task);
	++other_task->total_predecessors_;
	other_task->remaining_predecessors_.fetch_add(1);
  }

  /**
   * @brief Marks this task as a predecessor of multiple tasks.
   * @tparam tasks_type The types of the successor tasks.
   * @param other_task The first successor task.
   * @param tasks The remaining successor tasks.
   */
  template <typename... tasks_type>
  void precede(task* other_task, const tasks_type&... tasks) {
	next_.push_back(other_task);
	++other_task->total_predecessors_;
	other_task->remaining_predecessors_.fetch_add(1);
	precede(tasks...);
  }

  /**
   * @brief Attempts to cancel the task.
   * @return True if the task was cancelled, false if it was already invoked.
   */
  bool cancel() {
	return (cancellation_flags_.fetch_or(internal::k_cancelled) &
			internal::k_invoked) == 0;
  }

  /**
   * @brief Resets the cancellation flags of the task.
   */
  void reset() { cancellation_flags_.store(0); }

 private:
  friend class thread_pool;
  bool delete_{
	  false}; /**< Whether the task should be deleted after execution. */
  bool is_root_{
	  false}; /**< Whether the task is a root task (no predecessors). */
  int total_predecessors_{0}; /**< Total number of predecessor tasks. */
  std::atomic<int> remaining_predecessors_{
	  0}; /**< Number of remaining predecessors. */
  std::atomic<int> cancellation_flags_{
	  0}; /**< Flags for cancellation and invocation status. */
  std::function<void()> func_; /**< The callable to execute. */
  std::vector<task*> next_;	   /**< List of successor tasks. */
};

/**
 * @brief A thread pool with work-stealing capabilities for task execution.
 */
class thread_pool {
 public:
  /**
   * @brief Constructs a thread pool with the specified number of threads.
   * @param threads_count Number of worker threads (defaults to hardware
   * concurrency).
   */
  explicit thread_pool(
	  const unsigned threads_count = std::thread::hardware_concurrency())
	  : queues_count_{threads_count + 1}, queues_{threads_count + 1} {
	threads_.reserve(threads_count);
	for (unsigned i = 0; i != threads_count; ++i) {
	  threads_.emplace_back([this, i] { run(i + 1); });
	}
  }

  thread_pool(const thread_pool&) = delete;
  thread_pool(thread_pool&&) = delete;
  thread_pool& operator=(const thread_pool&) = delete;
  thread_pool& operator=(thread_pool&&) = delete;

  /**
   * @brief Destructor that waits for tasks to complete and joins threads.
   */
  ~thread_pool() noexcept {
	wait();
	stop_.test_and_set();
	tasks_count_ += queues_count_;
	tasks_count_.notify_all();
	for (auto& thread : threads_) {
	  thread.join();
	}
  }

  /**
   * @brief Submits a callable as a task to the thread pool.
   * @tparam func_type The type of the callable (must be convertible to
   * std::function<void()>).
   * @param func The callable to execute.
   */
  template <typename func_type, typename = std::enable_if_t<std::convertible_to<
									func_type, std::function<void()>>>>
  void submit(func_type&& func) {
	auto* new_task = new task(std::forward<func_type>(func));
	new_task->delete_ = true;
	submit(new_task);
  }

  /**
   * @brief Submits a task to the thread pool with optional affinity.
   * @param task The task to submit.
   * @param affinity The queue index to submit to (defaults to -1 for
   * round-robin).
   */
  void submit(task* task, unsigned affinity = -1) {
	++tasks_count_;
	if (affinity != -1 && affinity < queues_count_) {
	  queues_[affinity].push(task);
	} else {
	  queues_[index_].push(task);
	}
	tasks_count_.notify_one();
  }

  /**
   * @brief Submits a batch of tasks to the thread pool.
   * @tparam tasks_type The type of the task container.
   * @param tasks The container of tasks to submit.
   * @param affinity The queue index to submit to (defaults to -1 for
   * round-robin).
   */
  template <typename tasks_type>
  void submit_batch(tasks_type& tasks, unsigned affinity = -1) {
	for (auto& t : tasks) {
	  t.is_root_ = t.total_predecessors_ == 0;
	}
	for (auto& t : tasks) {
	  if (t.is_root_) {
		submit(&t, affinity);
	  }
	}
  }

  /**
   * @brief Waits for a condition to be met, executing tasks in the meantime.
   * @tparam predicate_type The type of the predicate.
   * @param predicate The condition to wait for.
   */
  template <typename predicate_type>
  void wait(const predicate_type& predicate) {
	while (!predicate()) {
	  if (auto* t = get_task()) {
		execute(t);
	  }
	}
  }

  /**
   * @brief Waits for all tasks to complete.
   */
  void wait() const {
	while (const auto count = tasks_count_.load()) {
	  tasks_count_.wait(count);
	}
  }

 private:
  /**
   * @brief Main loop for a worker thread.
   * @param i The index of the thread's queue.
   */
  void run(const unsigned i) {
	index_ = i;
	for (auto attempts = 0;;) {
	  if (constexpr auto max_attempts = 100; ++attempts > max_attempts) {
		tasks_count_.wait(0);
	  }
	  if (auto* t = get_task()) {
		execute(t);
		attempts = 0;
	  } else if (stop_.test()) {
		return;
	  }
	}
  }

  /**
   * @brief Executes a task and schedules its successors.
   * @param t The task to execute.
   */
  void execute(task* t) {
	for (task* next = nullptr; t; next = nullptr) {
	  t->remaining_predecessors_.store(t->total_predecessors_);
	  if (t->cancellation_flags_.fetch_or(internal::k_invoked) &
		  internal::k_cancelled) {
		break;
	  }
	  if (t->func_) {
		t->func_();
	  }
	  auto it = t->next_.begin();
	  for (; it != t->next_.end(); ++it) {
		if ((*it)->remaining_predecessors_.fetch_sub(1) == 1) {
		  next = *it++;
		  break;
		}
	  }
	  for (; it != t->next_.end(); ++it) {
		if ((*it)->remaining_predecessors_.fetch_sub(1) == 1) {
		  submit(*it);
		}
	  }
	  if (t->delete_) {
		delete t;
	  }
	  t = next;
	}
	if (tasks_count_.fetch_sub(1) == 1) {
	  tasks_count_.notify_all();
	}
  }

  /**
   * @brief Retrieves a task from the thread pool's queues.
   * @return A task pointer, or nullptr if no task is available.
   */
  task* get_task() {
	const auto i = index_;
	auto* t = queues_[i].pop();
	if (t) {
	  return t;
	}
	for (unsigned j = 1; j != queues_count_; ++j) {
	  t = queues_[(i + j) % queues_count_].steal();
	  if (t) {
		return t;
	  }
	}
	return nullptr;
  }

  static thread_local unsigned
	  index_; /**< Thread-local index of the current thread's queue. */
  const unsigned queues_count_;		  /**< Number of work-stealing queues. */
  std::atomic_flag stop_;			  /**< Flag to stop worker threads. */
  std::atomic<unsigned> tasks_count_; /**< Count of active tasks. */
  std::vector<std::thread> threads_;  /**< Worker threads. */
  std::vector<internal::work_stealing_deque<task*>>
	  queues_; /**< Work-stealing deques for tasks. */
};

/**
 * @brief Thread-local index for the current thread's queue.
 */
inline thread_local unsigned thread_pool::index_{0};
}  // namespace thread_pool