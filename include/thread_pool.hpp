#pragma once
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

namespace thread_pool {

namespace internal {

// Tagged pointer to solve ABA problem
template <typename T>
struct tagged_ptr {
  T* ptr;
  uintptr_t tag;

  tagged_ptr() : ptr(nullptr), tag(0) {}
  tagged_ptr(T* p, uintptr_t t) : ptr(p), tag(t) {}

  bool operator==(const tagged_ptr& other) const {
	return ptr == other.ptr && tag == other.tag;
  }
};

// Lock-free queue based on Michael & Scott algorithm with ABA protection
template <typename T>
class lockfree_queue {
  struct node {
	std::atomic<tagged_ptr<node>> next;
	T data;

	template <typename... Args>
	explicit node(Args&&... args)
		: next(tagged_ptr<node>(nullptr, 0)),
		  data(std::forward<Args>(args)...) {}
  };

  alignas(64) std::atomic<tagged_ptr<node>> head;
  alignas(64) std::atomic<tagged_ptr<node>> tail;

 public:
  lockfree_queue() {
	node* dummy = new node();
	tagged_ptr<node> dummy_ptr(dummy, 0);
	head.store(dummy_ptr);
	tail.store(dummy_ptr);
  }

  ~lockfree_queue() {
	while (dequeue()) {
	}
	delete head.load().ptr;
  }

  // No copying or moving
  lockfree_queue(const lockfree_queue&) = delete;
  lockfree_queue& operator=(const lockfree_queue&) = delete;

  void enqueue(T item) {
	node* new_node = new node(std::move(item));
	tagged_ptr<node> new_tail(new_node, 0);
	tagged_ptr<node> curr_tail;

	while (true) {
	  curr_tail = tail.load(std::memory_order_acquire);
	  tagged_ptr<node> curr_next =
		  curr_tail.ptr->next.load(std::memory_order_acquire);

	  if (curr_tail == tail.load(std::memory_order_relaxed)) {
		if (curr_next.ptr == nullptr) {
		  tagged_ptr<node> new_next(new_node, curr_next.tag + 1);
		  if (curr_tail.ptr->next.compare_exchange_weak(
				  curr_next, new_next, std::memory_order_release)) {
			tagged_ptr<node> new_tail(new_node, curr_tail.tag + 1);
			tail.compare_exchange_strong(curr_tail, new_tail,
										 std::memory_order_release);
			break;
		  }
		} else {
		  tagged_ptr<node> new_tail(curr_next.ptr, curr_tail.tag + 1);
		  tail.compare_exchange_strong(curr_tail, new_tail,
									   std::memory_order_release);
		}
	  }
	}
  }

  std::optional<T> dequeue() {
	tagged_ptr<node> curr_head;
	tagged_ptr<node> curr_tail;
	tagged_ptr<node> next;

	while (true) {
	  curr_head = head.load(std::memory_order_acquire);
	  curr_tail = tail.load(std::memory_order_acquire);
	  next = curr_head.ptr->next.load(std::memory_order_acquire);

	  if (curr_head == head.load(std::memory_order_relaxed)) {
		if (curr_head.ptr == curr_tail.ptr) {
		  if (next.ptr == nullptr) {
			return std::nullopt;
		  }
		  tagged_ptr<node> new_tail(next.ptr, curr_tail.tag + 1);
		  tail.compare_exchange_strong(curr_tail, new_tail,
									   std::memory_order_release);
		} else {
		  T val = std::move(next.ptr->data);
		  tagged_ptr<node> new_head(next.ptr, curr_head.tag + 1);
		  if (head.compare_exchange_strong(curr_head, new_head,
										   std::memory_order_release)) {
			delete curr_head.ptr;
			return val;
		  }
		}
	  }
	}
  }

  bool empty() const {
	tagged_ptr<node> curr_head = head.load(std::memory_order_acquire);
	tagged_ptr<node> curr_tail = tail.load(std::memory_order_acquire);
	tagged_ptr<node> next = curr_head.ptr->next.load(std::memory_order_acquire);

	return curr_head.ptr == curr_tail.ptr && next.ptr == nullptr;
  }
};

// Constants for task state
constexpr auto k_cancelled = 1;
constexpr auto k_invoked = 1 << 1;

}  // namespace internal

class task {
 public:
  task() = default;

  template <typename TaskType,
			typename = std::enable_if_t<std::is_invocable_r_v<void, TaskType>>>
  explicit task(TaskType&& func) : func_(std::forward<TaskType>(func)) {}

  // Copy/move operations
  task(const task& other)
	  : total_predecessors_(other.total_predecessors_),
		func_(other.func_),
		next_(other.next_) {
	remaining_predecessors_.store(other.remaining_predecessors_);
	cancellation_flags_.store(other.cancellation_flags_);
  }

  task(task&& other) noexcept
	  : total_predecessors_(other.total_predecessors_),
		func_(std::move(other.func_)),
		next_(std::move(other.next_)) {
	remaining_predecessors_.store(other.remaining_predecessors_);
	cancellation_flags_.store(other.cancellation_flags_);
  }

  task& operator=(const task& other) {
	if (this != &other) {
	  total_predecessors_ = other.total_predecessors_;
	  remaining_predecessors_.store(other.remaining_predecessors_);
	  cancellation_flags_.store(other.cancellation_flags_);
	  func_ = other.func_;
	  next_ = other.next_;
	}
	return *this;
  }

  task& operator=(task&& other) noexcept {
	if (this != &other) {
	  total_predecessors_ = other.total_predecessors_;
	  remaining_predecessors_.store(other.remaining_predecessors_);
	  cancellation_flags_.store(other.cancellation_flags_);
	  func_ = std::move(other.func_);
	  next_ = std::move(other.next_);
	}
	return *this;
  }

  void succeed(task* other_task) {
	other_task->next_.push_back(this);
	++total_predecessors_;
	remaining_predecessors_.fetch_add(1, std::memory_order_relaxed);
  }

  template <typename... TasksType>
  void succeed(task* other_task, const TasksType&... tasks) {
	succeed(other_task);
	succeed(tasks...);
  }

  void precede(task* other_task) {
	next_.push_back(other_task);
	++other_task->total_predecessors_;
	other_task->remaining_predecessors_.fetch_add(1, std::memory_order_relaxed);
  }

  template <typename... TasksType>
  void precede(task* other_task, const TasksType&... tasks) {
	precede(other_task);
	precede(tasks...);
  }

  bool cancel() {
	return (cancellation_flags_.fetch_or(internal::k_cancelled,
										 std::memory_order_relaxed) &
			internal::k_invoked) == 0;
  }

  void reset() { cancellation_flags_.store(0, std::memory_order_relaxed); }

 private:
  friend class thread_pool;
  bool delete_{false};
  bool is_root_{false};
  int total_predecessors_{0};
  std::atomic<int> remaining_predecessors_{0};
  std::atomic<int> cancellation_flags_{0};
  std::function<void()> func_;
  std::vector<task*> next_;
};

class thread_pool {
 public:
  explicit thread_pool(
	  unsigned threads_count = std::thread::hardware_concurrency())
	  : queues_(threads_count), stop_(false) {
	threads_.reserve(threads_count);
	for (unsigned i = 0; i < threads_count; ++i) {
	  threads_.emplace_back([this, i] { worker_loop(i); });
	}
  }

  ~thread_pool() {
	stop_.store(true, std::memory_order_release);
	for (auto& q : queues_) {
	  q.notify();
	}
	for (auto& t : threads_) {
	  if (t.joinable()) {
		t.join();
	  }
	}
  }

  // No copying or moving
  thread_pool(const thread_pool&) = delete;
  thread_pool& operator=(const thread_pool&) = delete;

  template <typename FuncType,
			typename = std::enable_if_t<std::is_invocable_r_v<void, FuncType>>>
  void submit(FuncType&& func, unsigned affinity = -1) {
	auto* new_task = new task(std::forward<FuncType>(func));
	new_task->delete_ = true;
	submit(new_task, affinity);
  }

  void submit(task* t, unsigned affinity = -1) {
	if (affinity == static_cast<unsigned>(-1)) {
	  affinity = next_queue_++ % queues_.size();
	}
	queues_[affinity].enqueue(t);
  }

  template <typename TasksType>
  void submit_batch(TasksType& tasks, unsigned affinity = -1) {
	for (auto& t : tasks) {
	  t.is_root_ = t.total_predecessors_ == 0;
	}
	for (auto& t : tasks) {
	  if (t.is_root_) {
		submit(&t, affinity);
	  }
	}
  }

  void wait() {
	while (!all_queues_empty()) {
	  if (auto t = try_steal_task()) {
		execute(t);
	  } else {
		std::this_thread::yield();
	  }
	}
  }

 private:
  struct queue {
	internal::lockfree_queue<task*> tasks;
	std::atomic<bool> has_tasks{false};
	std::condition_variable cv;
	std::mutex cv_mutex;

	void enqueue(task* t) {
	  tasks.enqueue(t);
	  notify();
	}

	std::optional<task*> dequeue() {
	  auto task = tasks.dequeue();
	  if (!task && !tasks.empty()) {
		// Try again if queue isn't empty but we got nothing
		return tasks.dequeue();
	  }
	  return task;
	}

	void notify() {
	  {
		std::lock_guard<std::mutex> lock(cv_mutex);
		has_tasks.store(true, std::memory_order_release);
	  }
	  cv.notify_one();
	}

	bool wait_for_task(std::atomic<bool>& stop) {
	  std::unique_lock<std::mutex> lock(cv_mutex);
	  cv.wait(lock, [this, &stop] {
		return has_tasks.load(std::memory_order_acquire) ||
			   stop.load(std::memory_order_acquire);
	  });
	  bool has_work = has_tasks.load(std::memory_order_acquire);
	  has_tasks.store(false, std::memory_order_relaxed);
	  return has_work;
	}

	bool empty() const { return tasks.empty(); }
  };

  void worker_loop(unsigned thread_idx) {
	while (!stop_.load(std::memory_order_acquire)) {
	  if (auto t = queues_[thread_idx].dequeue()) {
		execute(*t);
	  } else if (auto t = try_steal_task()) {
		execute(t);
	  } else if (!queues_[thread_idx].wait_for_task(stop_)) {
		break;
	  }
	}
  }

  task* try_steal_task() {
	const unsigned num_queues = queues_.size();
	for (unsigned i = 1; i < num_queues; ++i) {
	  unsigned idx = (next_queue_ + i) % num_queues;
	  if (auto t = queues_[idx].dequeue()) {
		return *t;
	  }
	}
	return nullptr;
  }

  void execute(task* t) {
	while (t) {
	  task* next = nullptr;

	  // Reset predecessors count for this execution
	  t->remaining_predecessors_.store(t->total_predecessors_,
									   std::memory_order_relaxed);

	  // Check if task was cancelled
	  if (t->cancellation_flags_.fetch_or(internal::k_invoked,
										  std::memory_order_relaxed) &
		  internal::k_cancelled) {
		break;
	  }

	  // Execute the task
	  if (t->func_) {
		t->func_();
	  }

	  // Schedule successors
	  auto it = t->next_.begin();
	  for (; it != t->next_.end(); ++it) {
		if ((*it)->remaining_predecessors_.fetch_sub(
				1, std::memory_order_relaxed) == 1) {
		  next = *it++;
		  break;
		}
	  }

	  for (; it != t->next_.end(); ++it) {
		if ((*it)->remaining_predecessors_.fetch_sub(
				1, std::memory_order_relaxed) == 1) {
		  submit(*it);
		}
	  }

	  if (t->delete_) {
		delete t;
	  }

	  t = next;
	}
  }

  bool all_queues_empty() const {
	for (const auto& q : queues_) {
	  if (!q.empty()) {
		return false;
	  }
	}
	return true;
  }

  std::vector<std::thread> threads_;
  std::vector<queue> queues_;
  std::atomic<bool> stop_{false};
  std::atomic<unsigned> next_queue_{0};
};

}  // namespace thread_pool