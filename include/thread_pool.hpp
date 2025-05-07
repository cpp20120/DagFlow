#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#if defined(_WIN32) || defined(_WIN64)
#define THREADPOOL_WINDOWS 1
#include <windows.h>
#undef max
#elif defined(__linux__) || defined(__APPLE__)
#define THREADPOOL_POSIX 1
#include <pthread.h>
#include <unistd.h>
#endif

namespace thread_pool {
template <typename T>
class lock_free_queue {
  struct Node {
    T data;
    Node *next;

    explicit Node(T value) : data(std::move(value)), next(nullptr) {}
  };

  std::atomic<Node *> head;
  std::atomic<Node *> tail;

 public:
  lock_free_queue() {
    Node *dummy = new Node(T{});
    head.store(dummy);
    tail.store(dummy);
  }

  ~lock_free_queue() {
    Node *current = head.load();
    while (current) {
      Node *next = current->next;
      delete current;
      current = next;
    }
  }

  void push(T value) {
    Node *new_node = new Node(std::move(value));
    Node *old_tail = tail.exchange(new_node);
    old_tail->next = new_node;
  }

  std::optional<T> pop() {
    Node *old_head = head.load();
    Node *next = old_head->next;

    if (!next) return std::nullopt;

    head.store(next);
    T value = std::move(next->data);
    delete old_head;
    return value;
  }

  [[nodiscard]] bool empty() const { return head.load()->next == nullptr; }
};

class task_cancelled_error final : public std::runtime_error {
 public:
  task_cancelled_error() : std::runtime_error("Task was cancelled") {}
};

struct TaskMetadata {
  std::string name;
  std::chrono::time_point<std::chrono::steady_clock> submission_time;
  std::optional<std::chrono::time_point<std::chrono::steady_clock>> start_time;
  std::optional<std::chrono::time_point<std::chrono::steady_clock>>
      completion_time;
  std::thread::id worker_thread_id;
  size_t memory_usage_estimate = 0;
  std::unordered_set<std::string> tags;
  std::optional<std::chrono::steady_clock::duration> timeout;
  std::optional<std::chrono::steady_clock::duration> execution_time;
  std::optional<std::chrono::steady_clock::duration> wait_time;
  std::optional<std::chrono::steady_clock::duration> queue_time;
  std::optional<std::chrono::steady_clock::duration> priority;
  std::vector<size_t> dependencies;
};

struct TaskHandle {
  std::shared_ptr<std::atomic<bool>> cancelled;
  std::shared_ptr<TaskMetadata> metadata;
  size_t task_id;

  [[nodiscard]] bool is_ready() const {
    return !cancelled || !cancelled->load();
  }
};

thread_local int lock_level = 0;

class hierarchical_mutex {
  std::mutex mtx;
  int level;

 public:
  explicit hierarchical_mutex(int l) : level(l) {}

  void lock() {
    if (lock_level >= level) {
      throw std::logic_error("Lock hierarchy violation");
    }
    mtx.lock();
    lock_level = level;
  }

  void unlock() {
    if (!mtx.try_lock()) {
      throw std::logic_error(
          "Attempt to unlock a mutex that is not locked by the current thread");
    }
    mtx.unlock();
  }
};
template <typename PriorityType>
class thread_pool;

class DependencyGraph {
  struct Node {
    std::shared_ptr<std::packaged_task<void(size_t)>> task;
    std::shared_ptr<std::future<void>> future;
    std::unordered_set<size_t> dependents;
    std::atomic<int> dependency_count{0};
    TaskMetadata metadata;
    std::shared_ptr<std::atomic<bool>> cancelled =
        std::make_shared<std::atomic<bool>>(false);
    std::atomic<bool> dependencies_processed{false};
    mutable std::mutex mutex;

    bool is_ready() const {
      return !cancelled->load() &&
             (dependency_count.load() == 0 || dependencies_processed.load());
    }
  };

  mutable std::mutex graph_mutex;
  mutable std::mutex queue_mutex;
  std::condition_variable tasks_completed_condition;
  std::unordered_map<size_t, Node> nodes;
  std::atomic<uint64_t> next_task_id{0};

  bool has_cycle_util(const size_t node_id, std::unordered_set<size_t> &visited,
                      std::unordered_set<size_t> &recursion_stack) const {
    if (!recursion_stack.contains(node_id)) {
      visited.insert(node_id);
      recursion_stack.insert(node_id);

      if (const auto it = nodes.find(node_id); it != nodes.end()) {
        for (size_t dependent_id : it->second.dependents) {
          if (!nodes.contains(dependent_id)) continue;
          if (!visited.contains(dependent_id)) {
            if (has_cycle_util(dependent_id, visited, recursion_stack)) {
              return true;
            }
          } else if (recursion_stack.contains(dependent_id)) {
            return true;
          }
        }
      }
    }
    recursion_stack.erase(node_id);
    return false;
  }

  void notify_dependents(size_t completed_id,
                         const std::unordered_set<size_t> &dependents) {
    if (dependents.empty()) {
      std::cout << "No dependents to notify for task " << completed_id
                << std::endl;
      return;
    }

    std::unique_lock lock(graph_mutex);
    for (size_t dependent_id : dependents) {
      if (auto dep_it = nodes.find(dependent_id); dep_it != nodes.end()) {
        int prev_count = dep_it->second.dependency_count.fetch_sub(1);
        if (prev_count <= 0) {
          std::cerr << "Invalid dependency count for task " << dependent_id
                    << ": " << prev_count << std::endl;
          continue;
        }
        std::cout << "Notified dependent task " << dependent_id
                  << ", new dependency_count: " << (prev_count - 1)
                  << std::endl;
        if (prev_count == 1) {
          dep_it->second.dependencies_processed.store(true);
          std::cout << "Task " << dependent_id << " is now ready" << std::endl;
        }
      } else {
        std::cerr << "Dependent task " << dependent_id << " not found in graph"
                  << std::endl;
      }
    }
  }

 public:
  std::mutex &get_graph_mutex() const { return graph_mutex; }
  std::unordered_map<size_t, Node> &get_nodes() { return nodes; }

  void verify_lock_state() {
    if (!graph_mutex.try_lock()) {
      std::cerr << "Graph mutex is locked!\n";
      return;
    }
    graph_mutex.unlock();

    for (auto &[id, node] : nodes) {
      if (!node.mutex.try_lock()) {
        std::cerr << "Node " << id << " mutex is locked!\n";
        continue;
      }
      node.mutex.unlock();
    }
  }

  void debug_print() const {
    std::scoped_lock lock(graph_mutex);
    std::cout << "Dependency Graph:\n";
    std::cout << "Total nodes: " << nodes.size() << "\n";

    for (const auto &[id, node] : nodes) {
      std::scoped_lock node_lock(node.mutex);
      std::cout << "  Task " << id << ": "
                << "deps=" << node.dependency_count.load()
                << ", dependents=" << node.dependents.size()
                << ", cancelled=" << node.cancelled->load() << "\n";
    }
  }

  bool has_cycle() const {
    std::unordered_set<size_t> visited;
    std::unordered_set<size_t> recursion_stack;

    for (const auto &[id, node] : nodes) {
      if (!visited.contains(id)) {
        if (has_cycle_util(id, visited, recursion_stack)) {
          return true;
        }
      }
    }
    return false;
  }

  TaskHandle add_task(std::packaged_task<void(size_t)> task,
                      TaskMetadata metadata,
                      const std::vector<TaskHandle> &dependencies, size_t id) {
    auto task_ptr =
        std::make_shared<std::packaged_task<void(size_t)>>(std::move(task));
    auto cancelled = std::make_shared<std::atomic<bool>>(false);
    auto meta_ptr = std::make_shared<TaskMetadata>(std::move(metadata));
    auto future = std::make_shared<std::future<void>>((*task_ptr).get_future());

    {
      std::scoped_lock lock(graph_mutex);

      for (const auto &dep : dependencies) {
        if (dep.task_id == id) {
          throw std::runtime_error("Self-dependency detected");
        }
      }

      Node &node = nodes[id];
      node.task = task_ptr;
      node.future = future;
      node.metadata = *meta_ptr;
      node.cancelled = cancelled;
      node.dependency_count.store(static_cast<int>(dependencies.size()));

      for (const auto &dep : dependencies) {
        nodes[dep.task_id].dependents.insert(id);
        meta_ptr->dependencies.push_back(dep.task_id);
      }

      if (has_cycle()) {
        nodes.erase(id);
        throw std::runtime_error("Circular dependency detected");
      }

      std::cout << "Added task " << id
                << " to dependency graph, dependency_count: "
                << node.dependency_count.load() << ", dependents: [";
      for (const auto &dep : node.dependents) {
        std::cout << dep << " ";
      }
      std::cout << "]" << std::endl;
    }

    return TaskHandle{cancelled, meta_ptr, id};
  }

  struct ReadyTask {
    std::packaged_task<void(size_t)> task;
    size_t task_id;
  };

  std::optional<ReadyTask> try_get_ready_task() {
    std::optional<ReadyTask> result;
    std::unordered_set<size_t> dependents_to_notify;
    size_t completed_id = 0;

    {
      std::unique_lock graph_lock(graph_mutex);
      for (auto it = nodes.begin(); it != nodes.end();) {
        auto &node = it->second;
        if (node.is_ready() && node.task && !node.metadata.completion_time) {
          result = ReadyTask{std::move(*node.task), it->first};
          node.task.reset();
          completed_id = it->first;
          dependents_to_notify = node.dependents;
          it = nodes.erase(it);
          break;
        } else {
          ++it;
        }
      }
    }

    if (result) {
      notify_dependents(completed_id, dependents_to_notify);
    }

    return result;
  }

  void mark_completed(size_t task_id) {
    std::unordered_set<size_t> dependents;
    {
      std::unique_lock lock(graph_mutex);
      auto it = nodes.find(task_id);
      if (it == nodes.end()) {
        std::cerr << "Warning: Attempt to mark non-existent task " << task_id
                  << " as completed" << std::endl;
        return;
      }
      if (it->second.metadata.completion_time) {
        std::cerr << "Warning: Task " << task_id << " already completed"
                  << std::endl;
        return;
      }
      dependents = it->second.dependents;
      it->second.metadata.completion_time = std::chrono::steady_clock::now();
      nodes.erase(it);
    }
    notify_dependents(task_id, dependents);
  }

  bool has_ready_task() const {
    std::scoped_lock lock(graph_mutex);
    for (const auto &[id, node] : nodes) {
      std::scoped_lock node_lock(node.mutex);
      if (node.dependency_count.load() == 0 && !node.cancelled->load() &&
          node.task) {
        return true;
      }
    }
    return false;
  }

  size_t pending_count() const {
    std::scoped_lock lock(graph_mutex);
    return nodes.size();
  }

  void clear_all() {
    std::scoped_lock lock(graph_mutex);
    nodes.clear();
  }
};

template <typename PriorityType = int>
class thread_pool {
  friend class DependencyGraph;

  struct PrioritizedTask {
    PriorityType priority;
    std::packaged_task<void()> task;
    std::shared_ptr<TaskMetadata> metadata;
    std::shared_ptr<std::atomic<bool>> cancelled;

    PrioritizedTask() = default;

    PrioritizedTask(PriorityType priority, std::packaged_task<void()> task,
                    std::shared_ptr<TaskMetadata> metadata,
                    std::shared_ptr<std::atomic<bool>> cancelled)
        : priority(priority),
          task(std::move(task)),
          metadata(std::move(metadata)),
          cancelled(std::move(cancelled)) {}

    PrioritizedTask(const PrioritizedTask &) = delete;
    PrioritizedTask &operator=(const PrioritizedTask &) = delete;

    PrioritizedTask(PrioritizedTask &&other) noexcept
        : priority(other.priority),
          task(std::move(other.task)),
          metadata(std::move(other.metadata)),
          cancelled(std::move(other.cancelled)) {}

    PrioritizedTask &operator=(PrioritizedTask &&other) noexcept {
      if (this != &other) {
        priority = other.priority;
        task = std::move(other.task);
        metadata = std::move(other.metadata);
        cancelled = std::move(other.cancelled);
      }
      return *this;
    }

    bool operator<(const PrioritizedTask &other) const {
      return priority < other.priority;
    }
  };

  struct TaskComparator {
    bool operator()(const std::shared_ptr<PrioritizedTask> &lhs,
                    const std::shared_ptr<PrioritizedTask> &rhs) const {
      return *rhs < *lhs;
    }
  };

  struct ThreadContext {
    std::deque<PrioritizedTask> queue;
    mutable std::mutex mutex;
    std::condition_variable cv;
    std::atomic<bool> should_stop{false};
    std::thread::id thread_id;
    std::atomic<int> cpu_core{-1};
    std::function<void()> initialization;
    std::function<void()> teardown;
    std::atomic<int> os_priority{0};
  };

  std::vector<ThreadContext> thread_contexts;
  std::vector<std::jthread> workers;
  std::priority_queue<std::shared_ptr<PrioritizedTask>,
                      std::vector<std::shared_ptr<PrioritizedTask>>,
                      TaskComparator>
      global_queue;
  mutable std::mutex queue_mutex;
  std::condition_variable queue_condition;
  std::vector<std::deque<PrioritizedTask>> worker_queues;
  std::vector<std::mutex> worker_mutexes;
  DependencyGraph dependency_graph;
  std::atomic<bool> is_active;
  mutable std::mutex thread_names_mutex;
  std::unordered_map<std::thread::id, std::string> thread_names;
  std::atomic<size_t> total_tasks_executed{0};
  std::atomic<size_t> idle_thread_count{0};
  std::atomic<size_t> pending_task_count{0};
  std::atomic<uint64_t> total_execution_time_us{0};
  std::condition_variable tasks_completed_condition;
  std::function<void(const TaskMetadata &)> on_task_complete;
  std::function<void(std::exception_ptr, const TaskMetadata &)> on_task_error;
  std::atomic<uint64_t> next_task_id{0};
  std::chrono::steady_clock::time_point last_task_submission_time;
  std::atomic<bool> submission_in_progress{false};

  static bool set_thread_affinity(int cpu_core) {
    if (cpu_core < 0) return true;

#if THREADPOOL_WINDOWS
    DWORD_PTR mask = 1ull << cpu_core;
    return SetThreadAffinityMask(GetCurrentThread(), mask) != 0;
#elif THREADPOOL_POSIX
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_core, &cpuset);
    return pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) ==
           0;
#else
    return false;
#endif
  }

  static int get_processor_count() {
#if THREADPOOL_WINDOWS
    SYSTEM_INFO sysinfo;
    GetSystemInfo(&sysinfo);
    return sysinfo.dwNumberOfProcessors;
#elif THREADPOOL_POSIX
    return sysconf(_SC_NPROCESSORS_ONLN);
#else
    return std::thread::hardware_concurrency();
#endif
  }

#if defined(_WIN32) || defined(_WIN64)
  bool set_os_thread_priority(int priority) {
    return SetThreadPriority(GetCurrentThread(), priority) != 0;
  }
#elif defined(__linux__) || defined(__APPLE__)
  bool set_os_thread_priority(int priority) {
    errno = 0;
    int current_nice = nice(0);
    if (errno != 0) return false;
    int new_nice = current_nice + priority;
    if (new_nice < -20) new_nice = -20;
    if (new_nice > 19) new_nice = 19;
    return nice(new_nice - current_nice) != -1;
  }
#else
  bool set_os_thread_priority(int) { return false; }
#endif

  void apply_thread_settings(size_t worker_index) {
    auto &context = thread_contexts[worker_index];
    const int cpu_core = context.cpu_core.load();
    const int os_priority = context.os_priority.load();

    if (cpu_core >= 0) {
      set_thread_affinity(cpu_core);
    }

    if (!set_os_thread_priority(os_priority)) {
      log_error(
          "Failed to set thread priority to " + std::to_string(os_priority),
          TaskMetadata{});
    }
  }

  bool try_get_local_task(PrioritizedTask &task, size_t worker_id) {
    std::scoped_lock lock(worker_mutexes[worker_id]);
    if (!worker_queues[worker_id].empty()) {
      task = std::move(worker_queues[worker_id].front());
      worker_queues[worker_id].pop_front();
      return true;
    }
    return false;
  }

  bool try_get_global_task(PrioritizedTask &task) {
    std::scoped_lock lock(queue_mutex);
    if (!global_queue.empty()) {
      auto task_ptr = global_queue.top();
      global_queue.pop();
      task = PrioritizedTask(task_ptr->priority, std::move(task_ptr->task),
                             std::move(task_ptr->metadata),
                             std::move(task_ptr->cancelled));
      return true;
    }
    return false;
  }

  bool try_steal_work(PrioritizedTask &task, size_t worker_id) {
    if (auto ready_task = dependency_graph.try_get_ready_task()) {
      auto metadata = std::make_shared<TaskMetadata>();
      metadata->submission_time = std::chrono::steady_clock::now();
      metadata->name = "task_" + std::to_string(ready_task->task_id);
      task = PrioritizedTask{
          PriorityType{std::numeric_limits<PriorityType>::max()},
          std::packaged_task<void()>([task = std::move(ready_task->task),
                                      task_id = ready_task->task_id, metadata,
                                      this]() mutable {
            std::cout << "Executing dependency task " << metadata->name
                      << " with task_id " << task_id << std::endl;
            task(task_id);
            dependency_graph.mark_completed(task_id);
          }),
          metadata, std::make_shared<std::atomic<bool>>(false)};
      return true;
    }

    size_t max_queue_size = 0;
    size_t victim = worker_id;
    for (size_t i = 0; i < worker_queues.size(); ++i) {
      if (i == worker_id) continue;
      std::unique_lock lock(worker_mutexes[i], std::try_to_lock);
      if (!lock.owns_lock()) continue;
      if (worker_queues[i].size() > max_queue_size) {
        max_queue_size = worker_queues[i].size();
        victim = i;
      }
    }

    if (max_queue_size > 1) {
      std::unique_lock lock(worker_mutexes[victim]);
      if (!worker_queues[victim].empty()) {
        task = std::move(worker_queues[victim].back());
        worker_queues[victim].pop_back();
        return true;
      }
    }

    std::unique_lock global_lock(queue_mutex, std::try_to_lock);
    if (global_lock.owns_lock() && !global_queue.empty()) {
      auto task_ptr = global_queue.top();
      global_queue.pop();
      task = PrioritizedTask{task_ptr->priority, std::move(task_ptr->task),
                             std::move(task_ptr->metadata),
                             std::move(task_ptr->cancelled)};
      return true;
    }

    return false;
  }

  void execute_task(PrioritizedTask &task, size_t worker_id) {
    idle_thread_count.fetch_sub(1, std::memory_order_relaxed);
    auto start_time = std::chrono::steady_clock::now();

    try {
      if (task.metadata && task.metadata->completion_time) {
        std::cerr << "Warning: Attempt to execute already completed task "
                  << task.metadata->name << std::endl;
        idle_thread_count.fetch_add(1, std::memory_order_relaxed);
        return;
      }
      if (task.metadata) {
        task.metadata->worker_thread_id = thread_contexts[worker_id].thread_id;
        task.metadata->start_time = start_time;
        std::cout << "Executing task " << task.metadata->name << " on thread "
                  << task.metadata->worker_thread_id << std::endl;
      }
      task.task();
      if (task.metadata) {
        std::cout << "Completed task " << task.metadata->name << std::endl;
        if (on_task_complete) {
          on_task_complete(*task.metadata);
        }
      }
    } catch (...) {
      if (task.metadata) {
        std::cerr << "Error in task " << task.metadata->name << std::endl;
        if (on_task_error) {
          on_task_error(std::current_exception(), *task.metadata);
        }
      }
    }

    const auto end_time = std::chrono::steady_clock::now();
    const auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        end_time - start_time);
    total_execution_time_us.fetch_add(duration.count());
    total_tasks_executed.fetch_add(1);
    idle_thread_count.fetch_add(1, std::memory_order_relaxed);

    if (task.metadata && pending_task_count.load() == 0) {
      std::scoped_lock lock(queue_mutex);
      tasks_completed_condition.notify_all();
    }
  }

  void worker_loop(const std::stop_token &stop_token, size_t worker_id) {
    auto &context = thread_contexts[worker_id];
    context.thread_id = std::this_thread::get_id();
    set_thread_name(context.thread_id, "worker_" + std::to_string(worker_id));
    apply_thread_settings(worker_id);
    if (context.initialization) {
      try {
        context.initialization();
      } catch (...) {
        log_error("Thread initialization failed", TaskMetadata{});
      }
    }

    int idle_attempts = 0;
    while (!stop_token.stop_requested() && is_active.load()) {
      PrioritizedTask task;
      bool task_executed = false;

      for (int attempt = 0; attempt < 4 && !task_executed; ++attempt) {
        if (try_get_local_task(task, worker_id)) {
          execute_task(task, worker_id);
          task_executed = true;
          idle_attempts = 0;
          break;
        }

        if (try_steal_work(task, worker_id)) {
          execute_task(task, worker_id);
          task_executed = true;
          idle_attempts = 0;
          break;
        }

        if (!task_executed && attempt < 3) {
          std::this_thread::sleep_for(
              std::chrono::microseconds(100 << attempt));
        }
      }

      if (!task_executed) {
        idle_attempts++;
        if (idle_attempts > 10) {
          if (emergency_process_blocked_tasks(1)) {
            idle_attempts = 0;
            continue;
          }
        }

        std::unique_lock lock(thread_contexts[worker_id].mutex);
        idle_thread_count.fetch_add(1, std::memory_order_relaxed);
        thread_contexts[worker_id].cv.wait_for(
            lock, std::chrono::milliseconds(100), [&] {
              return !thread_contexts[worker_id].queue.empty() ||
                     stop_token.stop_requested() ||
                     dependency_graph.has_ready_task() || !global_queue.empty();
            });
        idle_thread_count.fetch_sub(1, std::memory_order_relaxed);
      }
    }

    if (context.teardown) {
      try {
        context.teardown();
      } catch (...) {
        log_error("Thread teardown failed", TaskMetadata{});
      }
    }
  }

  void dispatch_to_thread(size_t worker_index, std::function<void()> f) {
    if (auto &context = thread_contexts[worker_index];
        std::this_thread::get_id() == context.thread_id) {
      f();
    } else {
      std::packaged_task<void()> task(f);
      {
        std::scoped_lock lock(context.mutex);
        context.queue.push_front(
            PrioritizedTask{std::numeric_limits<PriorityType>::max(),
                            std::move(task), nullptr, nullptr});
      }
      context.cv.notify_one();
    }
  }

  static void log_error(const std::string &message,
                        const TaskMetadata &metadata) {
    std::cerr << "[ThreadPool Error] Thread " << std::this_thread::get_id()
              << " in task " << metadata.name << ": " << message << std::endl;
  }

 public:
  thread_pool(const thread_pool &) = delete;
  thread_pool &operator=(const thread_pool &) = delete;

  explicit thread_pool(size_t thread_count)
      : thread_contexts(thread_count),
        worker_queues(thread_count),
        worker_mutexes(thread_count),
        is_active(true),
        idle_thread_count(thread_count) {
    for (auto &context : thread_contexts) {
      context.should_stop.store(false);
    }

    for (size_t i = 0; i < thread_count; ++i) {
      workers.emplace_back([this, i](std::stop_token stop_token) {
        worker_loop(stop_token, i);
      });
    }
  }

  ~thread_pool() {
    shutdown();
    wait_for_tasks();
  }

  template <typename F, typename... Args>
  auto submit(PriorityType priority,
              std::chrono::steady_clock::duration timeout, F &&f,
              Args &&...args)
      -> std::pair<std::future<typename std::invoke_result<F, Args...>::type>,
                   TaskHandle> {
    using return_type = typename std::invoke_result<F, Args...>::type;

    submission_in_progress.store(true);
    last_task_submission_time = std::chrono::steady_clock::now();
    auto cancelled = std::make_shared<std::atomic<bool>>(false);
    auto metadata = std::make_shared<TaskMetadata>();
    metadata->submission_time = std::chrono::steady_clock::now();
    size_t task_id = next_task_id.fetch_add(1);
    metadata->name = "task_" + std::to_string(task_id);
    metadata->timeout = timeout;

    std::cout << "Submitting task " << metadata->name << " with task_id "
              << task_id << ", dependencies: []" << std::endl;

    auto packaged_task = std::make_shared<std::packaged_task<return_type()>>(
        [cancelled, f = std::forward<F>(f), args...]() {
          if (cancelled->load()) throw task_cancelled_error();
          return f(args...);
        });

    std::future<return_type> result = packaged_task->get_future();

    auto wrapped_task = std::packaged_task<void()>(
        [packaged_task, metadata, task_id, this]() mutable {
          metadata->start_time = std::chrono::steady_clock::now();
          metadata->worker_thread_id = std::this_thread::get_id();
          std::cout << "Executing task " << metadata->name << " with task_id "
                    << task_id << " on thread " << metadata->worker_thread_id
                    << std::endl;
          try {
            (*packaged_task)();
            metadata->completion_time = std::chrono::steady_clock::now();
            std::cout << "Completed task " << metadata->name << std::endl;
            dependency_graph.mark_completed(task_id);
            std::cout << "Decremented pending_task_count to "
                      << pending_task_count.load() - 1 << " for task "
                      << metadata->name << std::endl;
            pending_task_count.fetch_sub(1, std::memory_order_relaxed);
          } catch (...) {
            metadata->completion_time = std::chrono::steady_clock::now();
            std::cerr << "Error in task " << metadata->name << std::endl;
            dependency_graph.mark_completed(task_id);
            std::cout << "Decremented pending_task_count to "
                      << pending_task_count.load() - 1 << " for task "
                      << metadata->name << std::endl;
            pending_task_count.fetch_sub(1, std::memory_order_relaxed);
            throw;
          }
        });

    TaskHandle handle = dependency_graph.add_task(
        std::packaged_task<void(size_t)>(
            [task = std::move(wrapped_task)](size_t) mutable { task(); }),
        *metadata, {}, task_id);

    {
      std::scoped_lock lock(queue_mutex);
      auto task_ptr = std::make_shared<PrioritizedTask>(
          priority, std::move(wrapped_task), metadata, cancelled);
      global_queue.push(task_ptr);
      pending_task_count.fetch_add(1, std::memory_order_relaxed);
      std::cout << "Incremented pending_task_count to "
                << pending_task_count.load() << " for task " << metadata->name
                << std::endl;
    }

    queue_condition.notify_one();
    submission_in_progress.store(false);
    return {std::move(result), std::move(handle)};
  }

  template <typename F, typename... Args>
  auto submit(PriorityType priority, F &&f, Args &&...args)
      -> std::pair<std::future<std::invoke_result_t<F, Args...>>, TaskHandle> {
    return submit(priority, std::chrono::steady_clock::duration::max(),
                  std::forward<F>(f), std::forward<Args>(args)...);
  }

  template <typename F, typename... Args>
  auto submit_with_dependencies(PriorityType priority,
                                std::chrono::steady_clock::duration timeout,
                                const std::vector<TaskHandle> &dependencies,
                                F &&f, Args &&...args)
      -> std::pair<std::future<std::invoke_result_t<F, Args...>>, TaskHandle> {
    using return_type = std::invoke_result_t<F, Args...>;

    verify_dependencies(dependencies);

    auto cancelled = std::make_shared<std::atomic<bool>>();
    auto metadata = std::make_shared<TaskMetadata>();
    metadata->submission_time = std::chrono::steady_clock::now();
    size_t task_id = next_task_id.fetch_add(1);
    metadata->name = "task_" + std::to_string(task_id);
    metadata->timeout = timeout;

    std::cout << "Submitting task " << metadata->name << " with task_id "
              << task_id << ", dependencies: [";
    for (const auto &dep : dependencies) {
      std::cout << dep.task_id << " ";
    }
    std::cout << "]" << std::endl;

    auto task = std::make_shared<std::packaged_task<return_type()>>(
        [cancelled, func = std::forward<F>(f),
         args_tuple = std::make_tuple(std::forward<Args>(args)...)]() {
          if (cancelled->load()) throw task_cancelled_error();
          return std::apply(func, args_tuple);
        });

    std::future<return_type> result = task->get_future();

    auto packaged_task = std::packaged_task<void(size_t)>(
        [task, metadata, task_id, this](size_t id) mutable {
          if (id != task_id) {
            std::cerr << "Task ID mismatch in task " << metadata->name
                      << ": expected " << task_id << ", got " << id
                      << std::endl;
            throw std::runtime_error("Task ID mismatch");
          }
          metadata->start_time = std::chrono::steady_clock::now();
          metadata->worker_thread_id = std::this_thread::get_id();
          std::cout << "Executing task " << metadata->name << " with task_id "
                    << task_id << " on thread " << metadata->worker_thread_id
                    << std::endl;
          try {
            (*task)();
            metadata->completion_time = std::chrono::steady_clock::now();
            dependency_graph.mark_completed(task_id);
            std::cout << "Decremented pending_task_count to "
                      << pending_task_count.load() - 1 << " for task "
                      << metadata->name << std::endl;
            pending_task_count.fetch_sub(1, std::memory_order_relaxed);
          } catch (...) {
            metadata->completion_time = std::chrono::steady_clock::now();
            dependency_graph.mark_completed(task_id);
            std::cout << "Decremented pending_task_count to "
                      << pending_task_count.load() - 1 << " for task "
                      << metadata->name << std::endl;
            pending_task_count.fetch_sub(1, std::memory_order_relaxed);
            throw;
          }
        });

    TaskHandle handle = dependency_graph.add_task(
        std::move(packaged_task), *metadata, dependencies, task_id);

    {
      std::scoped_lock lock(queue_mutex);
      pending_task_count.fetch_add(1, std::memory_order_relaxed);
      std::cout << "Incremented pending_task_count to "
                << pending_task_count.load() << " for task " << metadata->name
                << std::endl;
    }

    queue_condition.notify_one();
    return {std::move(result), std::move(handle)};
  }

  template <typename F, typename... Args>
  auto submit_with_dependencies(PriorityType priority,
                                const std::vector<TaskHandle> &dependencies,
                                F &&f, Args &&...args)
      -> std::pair<std::future<std::invoke_result_t<F, Args...>>, TaskHandle> {
    return submit_with_dependencies(
        priority, std::chrono::steady_clock::duration::max(), dependencies,
        std::forward<F>(f), std::forward<Args>(args)...);
  }

  template <typename Func>
  auto submit_batch(PriorityType priority,
                    std::chrono::steady_clock::duration timeout,
                    std::vector<Func> tasks)
      -> std::vector<
          std::pair<std::future<std::invoke_result_t<Func>>, TaskHandle>> {
    std::vector<std::pair<std::future<std::invoke_result_t<Func>>, TaskHandle>>
        handles;
    for (auto &task_func : tasks) {
      handles.push_back(submit(priority, timeout, std::move(task_func)));
    }
    return handles;
  }

  template <typename Func>
  auto submit_batch(PriorityType priority, std::vector<Func> tasks)
      -> std::vector<
          std::pair<std::future<std::invoke_result_t<Func>>, TaskHandle>> {
    std::vector<std::pair<std::future<std::invoke_result_t<Func>>, TaskHandle>>
        handles;
    handles.reserve(tasks.size());
    for (auto &task_func : tasks) {
      handles.push_back(submit(priority, std::move(task_func)));
    }
    return handles;
  }

  bool emergency_process_blocked_tasks(bool force) {
    std::vector<size_t> to_process;
    {
      std::unique_lock graph_lock(dependency_graph.get_graph_mutex());
      auto &nodes = dependency_graph.get_nodes();

      for (auto &[id, node] : nodes) {
        bool all_deps_completed = true;
        for (auto dep_id : node.metadata.dependencies) {
          if (nodes.contains(dep_id)) {
            all_deps_completed = false;
            break;
          }
        }

        if ((all_deps_completed || force) && node.task &&
            !node.metadata.completion_time) {
          node.dependencies_processed.store(true);
          node.dependency_count.store(0);
          to_process.push_back(id);
          std::cout << "Emergency: Marking task " << id << " as ready"
                    << std::endl;
        }
      }
    }

    bool processed = false;
    for (auto id : to_process) {
      if (auto ready_task = dependency_graph.try_get_ready_task()) {
        auto metadata = std::make_shared<TaskMetadata>();
        metadata->submission_time = std::chrono::steady_clock::now();
        metadata->name = "emergency_task_" + std::to_string(id);
        PrioritizedTask ptask{
            PriorityType{},
            std::packaged_task<void()>([task = std::move(ready_task->task),
                                        task_id = ready_task->task_id, metadata,
                                        this]() mutable {
              std::cout << "Executing emergency task " << metadata->name
                        << " with task_id " << task_id << std::endl;
              task(task_id);
            }),
            metadata, std::make_shared<std::atomic<bool>>(false)};
        execute_task(ptask, 0);
        processed = true;
      }
    }

    return processed;
  }

  void wait_for_tasks(
      std::chrono::milliseconds timeout = std::chrono::seconds(30)) {
    const auto start = std::chrono::steady_clock::now();
    bool warning_printed = false;

    while (true) {
      {
        std::unique_lock lock(queue_mutex);
        if (pending_task_count.load(std::memory_order_acquire) == 0 &&
            dependency_graph.pending_count() == 0) {
          return;
        }
      }

      if (std::chrono::steady_clock::now() - start > timeout) {
        std::cerr << "Timeout waiting for tasks, forcing completion...\n";
        emergency_process_blocked_tasks(true);
        break;
      }

      if (std::chrono::steady_clock::now() - start > timeout / 2 &&
          !warning_printed) {
        std::cerr << "Warning: Tasks taking longer than expected...\n";
        debug_check_state();
        warning_printed = true;
        emergency_process_blocked_tasks(1);
      }

      {
        std::unique_lock lock(queue_mutex);
        tasks_completed_condition.wait_for(
            lock, std::chrono::milliseconds(100), [this]() {
              return pending_task_count.load(std::memory_order_acquire) == 0 &&
                     dependency_graph.pending_count() == 0;
            });
      }
    }

    if (pending_task_count.load() > 0 || dependency_graph.pending_count() > 0) {
      std::stringstream ss;
      ss << "Timeout waiting for tasks (" << timeout.count() << "ms)\n";
      ss << "Pending tasks: " << pending_task_count.load() << "\n";
      ss << "Dependency tasks: " << dependency_graph.pending_count() << "\n";
      throw std::runtime_error(ss.str());
    }
  }

  void debug_check_state() {
    std::scoped_lock lock(queue_mutex);

    std::cout << "=== Thread Pool State ===\n";
    std::cout << "Pending tasks: " << pending_task_count.load() << "\n";
    std::cout << "Idle workers: " << idle_thread_count.load() << "/"
              << workers.size() << "\n";
    std::cout << "Dependency tasks: " << dependency_graph.pending_count()
              << "\n";
    std::cout << "Global queue size: " << global_queue.size() << "\n";

    for (size_t i = 0; i < workers.size(); ++i) {
      std::scoped_lock ctx_lock(thread_contexts[i].mutex);
      std::cout << "Worker " << i << " (" << thread_contexts[i].thread_id
                << "): " << (thread_contexts[i].queue.empty() ? "idle" : "busy")
                << "\n";
    }

    dependency_graph.debug_print();
  }

  void emergency_task_unlock() {
    while (auto dep_task = dependency_graph.try_get_ready_task()) {
      auto metadata = std::make_shared<TaskMetadata>();
      metadata->submission_time = std::chrono::steady_clock::now();
      PrioritizedTask task =
          PrioritizedTask{PriorityType{}, std::move(*dep_task), metadata,
                          std::make_shared<std::atomic<bool>>(false)};
      execute_task(task, 0);
    }

    pending_task_count.store(0);
  }

  void shutdown() {
    is_active.store(false);
    queue_condition.notify_all();
    tasks_completed_condition.notify_all();
    dependency_graph.clear_all();
    for (auto &worker : workers) {
      if (worker.joinable()) {
        worker.request_stop();
        worker.join();
      }
    }
  }

  void emergency_shutdown() {
    is_active.store(false);

    {
      std::scoped_lock lock(queue_mutex);
      while (!global_queue.empty()) {
        global_queue.pop();
      }
      pending_task_count.store(0);
    }

    dependency_graph.clear_all();

    queue_condition.notify_all();
    tasks_completed_condition.notify_all();

    for (auto &worker : workers) {
      if (worker.joinable()) {
        worker.request_stop();
        worker.join();
      }
    }
  }

  static void log_task_submission(const TaskHandle &handle) {
    std::string tags_str;
    for (const auto &tag : handle.metadata->tags) {
      if (!tags_str.empty()) tags_str += ", ";
      tags_str += tag;
    }

    std::cout << "Submitted task " << handle.task_id << " tags: " << tags_str
              << " priority: "
              << handle.metadata->priority
                     .value_or(std::chrono::steady_clock::duration::zero())
                     .count()
              << " name: " << handle.metadata->name << "\n";
  }

  static void log_task_start(const TaskMetadata &meta) {
    std::cout << "Started task " << meta.name << " on thread "
              << meta.worker_thread_id << "\n";
  }

  static void log_task_state(const TaskMetadata &meta) {
    std::cout << "Task " << meta.name << " deps=" << meta.dependencies.size()
              << " status=" << (meta.completion_time ? "done" : "pending")
              << std::endl;
  }

  static void wait_for_dependencies(const std::vector<TaskHandle> &deps,
                                    const std::chrono::milliseconds timeout) {
    const auto start = std::chrono::steady_clock::now();
    for (const auto &dep : deps) {
      while (!dep.is_ready()) {
        if (std::chrono::steady_clock::now() - start > timeout) {
          throw std::runtime_error("Dependency timeout");
        }
        std::this_thread::yield();
      }
    }
  }

  static void verify_dependencies(const std::vector<TaskHandle> &deps) {
    for (const auto &dep : deps) {
      if (dep.cancelled->load()) {
        throw std::runtime_error("Dependency task " +
                                 std::to_string(dep.task_id) + " is cancelled");
      }
      if (!dep.metadata) {
        throw std::runtime_error("Dependency task " +
                                 std::to_string(dep.task_id) +
                                 " has invalid metadata");
      }
    }
  }
  static void cancel_task(const TaskHandle &handle) {
    if (handle.cancelled) {
      handle.cancelled->store(true);
      dependency_graph.mark_completed(handle.task_id);
    }
  }

  void configure_thread(size_t worker_index, int cpu_core = -1,
                        int os_priority = 0,
                        std::function<void()> init = nullptr,
                        std::function<void()> cleanup = nullptr) {
    if (worker_index >= workers.size()) return;

    auto &context = thread_contexts[worker_index];
    context.cpu_core.store(cpu_core);
    context.os_priority.store(os_priority);
    context.initialization = init;
    context.teardown = cleanup;

    if (cpu_core >= 0) {
      if (const int max_cores = get_processor_count(); cpu_core >= max_cores) {
        log_error("Invalid CPU core in configure_thread - max is " +
                      std::to_string(max_cores - 1),
                  TaskMetadata{});
        context.cpu_core.store(-1);
      }
    }

    if (context.thread_id != std::thread::id()) {
      dispatch_to_thread(worker_index, [this, worker_index] {
        apply_thread_settings(worker_index);
      });
    }
  }

  struct ThreadPoolStats {
    size_t tasks_executed;
    size_t tasks_pending;
    size_t threads_active;
    size_t threads_idle;
    std::chrono::microseconds total_cpu_time;
    size_t dependency_tasks_pending;
  };

  ThreadPoolStats get_stats() const {
    std::scoped_lock lock(queue_mutex);
    return {total_tasks_executed.load(std::memory_order_relaxed),
            global_queue.size(),
            workers.size() - idle_thread_count.load(std::memory_order_relaxed),
            idle_thread_count.load(std::memory_order_relaxed),
            std::chrono::microseconds(
                total_execution_time_us.load(std::memory_order_relaxed)),
            dependency_graph.pending_count()};
  }

  void set_on_task_complete_callback(
      std::function<void(const TaskMetadata &)> callback) {
    on_task_complete = std::move(callback);
  }

  void set_on_task_error_callback(
      std::function<void(std::exception_ptr, const TaskMetadata &)> callback) {
    on_task_error = std::move(callback);
  }

  std::string get_thread_name(std::thread::id id) const {
    std::scoped_lock lock(thread_names_mutex);
    auto it = thread_names.find(id);
    return it != thread_names.end() ? it->second : "";
  }

  void set_thread_name(std::thread::id id, const std::string &name) {
    std::scoped_lock lock(thread_names_mutex);
    thread_names[id] = name;
  }

  void reset_statistics() {
    total_tasks_executed.store(0);
    total_execution_time_us.store(0);
  }
};

}  // namespace thread_pool
