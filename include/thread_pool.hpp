#pragma once
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
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

// Lock-free queue based on Michael-Scott algorithm with ABA protection
template <typename T>
class lockfree_queue {
  struct node {
	alignas(64) std::atomic<tagged_ptr<node>> next;
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
	tagged_ptr<node> curr_head = head.load(std::memory_order_acquire);
	node* curr = curr_head.ptr;
	while (curr) {
	  node* next = curr->next.load(std::memory_order_acquire).ptr;
	  delete curr;
	  curr = next;
	}
  }

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
	while (true) {
	  tagged_ptr<node> curr_head = head.load(std::memory_order_acquire);
	  tagged_ptr<node> curr_tail = tail.load(std::memory_order_acquire);
	  tagged_ptr<node> next =
		  curr_head.ptr->next.load(std::memory_order_acquire);

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

  bool empty() const {
	tagged_ptr<node> curr_head = head.load(std::memory_order_acquire);
	tagged_ptr<node> curr_tail = tail.load(std::memory_order_acquire);
	tagged_ptr<node> next = curr_head.ptr->next.load(std::memory_order_acquire);
	return curr_head.ptr == curr_tail.ptr && next.ptr == nullptr;
  }
};

constexpr auto k_cancelled = 1;
constexpr auto k_invoked = 1 << 1;

}  // namespace internal

class task {
 public:
  task() = default;

  template <typename TaskType,
			typename = std::enable_if_t<std::is_invocable_r_v<void, TaskType>>>
  explicit task(TaskType&& func) : func_(std::forward<TaskType>(func)) {}

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

  template <typename... TasksType>
  constexpr void succeed(TasksType*... tasks) {
	if constexpr (sizeof...(tasks) > 0) {
	  (succeed_one(tasks), ...);
	}
  }

  template <typename... TasksType>
  constexpr void precede(TasksType*... tasks) {
	if constexpr (sizeof...(tasks) > 0) {
	  (precede_one(tasks), ...);
	}
  }

  bool cancel() {
	return (cancellation_flags_.fetch_or(internal::k_cancelled,
										 std::memory_order_release) &
			internal::k_invoked) == 0;
  }

  void reset() { cancellation_flags_.store(0, std::memory_order_relaxed); }

 private:
  void succeed_one(task* other_task) {
	other_task->next_.push_back(this);
	++total_predecessors_;
	remaining_predecessors_.fetch_add(1, std::memory_order_relaxed);
  }

  void precede_one(task* other_task) {
	next_.push_back(other_task);
	++other_task->total_predecessors_;
	other_task->remaining_predecessors_.fetch_add(1, std::memory_order_relaxed);
  }

  friend class thread_pool;
  bool delete_{false};
  bool is_root_{false};
  int total_predecessors_{0};
  alignas(64) std::atomic<int> remaining_predecessors_{0};
  alignas(64) std::atomic<int> cancellation_flags_{0};
  std::function<void()> func_;
  std::vector<task*> next_;
};

class thread_pool {
 public:
  explicit thread_pool(
	  unsigned threads_count = std::thread::hardware_concurrency())
	  : queues_(threads_count > 0 ? threads_count : 1), stop_(false) {
	threads_.reserve(queues_.size());
	for (unsigned i = 0; i < queues_.size(); ++i) {
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
	alignas(64) std::atomic_flag has_tasks = ATOMIC_FLAG_INIT;

	void enqueue(task* t) {
	  tasks.enqueue(t);
	  notify();
	}

	std::optional<task*> dequeue() {
	  auto task = tasks.dequeue();
	  if (!task && !tasks.empty()) {
		return tasks.dequeue();
	  }
	  return task;
	}

	void notify() { has_tasks.test_and_set(std::memory_order_release); }

	bool wait_for_task(std::atomic<bool>& stop) {
	  while (!has_tasks.test(std::memory_order_acquire) &&
			 !stop.load(std::memory_order_acquire)) {
		std::this_thread::yield();
	  }
	  bool has_work = has_tasks.test_and_set(std::memory_order_acquire);
	  has_tasks.clear(std::memory_order_relaxed);
	  return has_work;
	}

	bool empty() const { return tasks.empty(); }
  };

  void worker_loop(unsigned thread_idx) {
	thread_local std::random_device rd;
	thread_local std::mt19937 gen(rd());
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
	thread_local std::random_device rd;
	thread_local std::mt19937 gen(rd());
	const unsigned num_queues = queues_.size();
	std::uniform_int_distribution<unsigned> dist(0, num_queues - 1);
	unsigned start_idx = dist(gen);
	for (unsigned i = 0; i < num_queues; ++i) {
	  unsigned idx = (start_idx + i) % num_queues;
	  if (auto t = queues_[idx].dequeue()) {
		return *t;
	  }
	}
	return nullptr;
  }

  void execute(task* t) {
	while (t) {
	  task* next = nullptr;
	  t->remaining_predecessors_.store(t->total_predecessors_,
									   std::memory_order_relaxed);

	  if (t->cancellation_flags_.fetch_or(internal::k_invoked,
										  std::memory_order_release) &
		  internal::k_cancelled) {
		break;
	  }

	  if (t->func_) {
		t->func_();
	  }

	  auto it = t->next_.begin();
	  for (; it != t->next_.end(); ++it) {
		if ((*it)->remaining_predecessors_.fetch_sub(
				1, std::memory_order_release) == 1) {
		  next = *it++;
		  break;
		}
	  }

	  for (; it != t->next_.end(); ++it) {
		if ((*it)->remaining_predecessors_.fetch_sub(
				1, std::memory_order_release) == 1) {
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
  alignas(64) std::atomic<bool> stop_{false};
  alignas(64) std::atomic<unsigned> next_queue_{0};
};

}  // namespace thread_pool