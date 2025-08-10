#pragma once
#include <atomic>
#include <functional>
#include <type_traits>
#include <vector>

namespace tp {

class ThreadPool;  // fwd

class Task {
 public:
  Task() = default;

  template <typename F,
			typename = std::enable_if_t<std::is_invocable_r_v<void, F>>>
  explicit Task(F&& f) : func_(std::forward<F>(f)) {}

  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;
  Task(Task&&) = default;
  Task& operator=(Task&&) = default;

  template <typename... Tasks>
  void precede(Tasks*... ts) {
	(precede_one(ts), ...);
  }

  template <typename... Tasks>
  void succeed(Tasks*... ts) {
	(succeed_one(ts), ...);
  }

  bool cancel() {
	return (flags_.fetch_or(kCancelled, std::memory_order_acq_rel) &
			kInvoked) == 0;
  }

  void reset() { flags_.store(0, std::memory_order_relaxed); }

 private:
  friend class ThreadPool;

  void precede_one(Task* other) {
	next_.push_back(other);
	other->total_preds_++;
	other->remain_preds_.fetch_add(1, std::memory_order_relaxed);
  }

  void succeed_one(Task* other) {
	other->next_.push_back(this);
	total_preds_++;
	remain_preds_.fetch_add(1, std::memory_order_relaxed);
  }

  static constexpr int kCancelled = 1;
  static constexpr int kInvoked = 1 << 1;

  std::function<void()> func_{};
  std::vector<Task*> next_{};
  int total_preds_{0};
  alignas(64) std::atomic<int> remain_preds_{0};
  alignas(64) std::atomic<int> flags_{0};
};

}  // namespace tp
