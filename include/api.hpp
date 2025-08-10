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

struct ScheduleOptions {
  Priority priority = Priority::Normal;
  std::optional<uint32_t> affinity{};
  int concurrency = 1;
  std::size_t capacity = SIZE_MAX;
  TaskGraph::Overflow overflow = TaskGraph::Overflow::Block;
};

class JobHandle {
 public:
  JobHandle() = default;
  explicit JobHandle(std::size_t id) : id_(id) {}
  [[nodiscard]] bool valid() const noexcept { return id_ != npos; }
  [[nodiscard]] size_t id() const noexcept { return id_; }

 private:
  size_t id_ = npos;
  static constexpr size_t npos = static_cast<size_t>(-1);
  friend class TaskScope;
};

class TaskScope {
 public:
  explicit TaskScope(Pool& pool) : pool_(pool), graph_(pool) {}

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

  template <class F>
  JobHandle then(JobHandle dep, F&& f, const ScheduleOptions& opt = {}) {
	auto h = submit(std::forward<F>(f), opt);
	graph_.add_edge({dep.id()}, {h.id()});
	return h;
  }

  template <class F>
  JobHandle when_all(std::span<const JobHandle> deps, F&& f,
					 const ScheduleOptions& opt = {}) {
	auto h = submit(std::forward<F>(f), opt);
	for (auto& d : deps) graph_.add_edge({d.id()}, {h.id()});
	return h;
  }
  template <class F>
  JobHandle when_all(std::initializer_list<JobHandle> deps, F&& f,
					 const ScheduleOptions& opt = {}) {
	return when_all(std::span<const JobHandle>(deps.begin(), deps.size()),
					std::forward<F>(f), opt);
  }

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

  Handle run() {
	if (ran_) return handle_;
	handle_ = graph_.run();
	ran_ = true;
	return handle_;
  }

  void wait() {
	if (!ran_) run();
	if (handle_.valid()) pool_.wait(handle_);
	last_error_ = graph_.last_error();

	ran_ = false;
	handle_ = {};
  }

  [[nodiscard]] std::exception_ptr last_error() const noexcept { return last_error_; }

  void run_and_wait() {
	run();
	wait();
	if (auto ep = last_error_) std::rethrow_exception(ep);
  }

  void clear() {
	ensure_not_running_ended();
	graph_.clear();
	last_error_ = nullptr;
  }

  ~TaskScope() {
	try {
	  if (!ran_ && graph_.size() > 0) run();
	  if (ran_) wait();
	} catch (...) {
	}
  }

 private:
  void ensure_not_running_ended() {
	if (ran_) {
	  if (handle_.valid()) pool_.wait(handle_);
	  last_error_ = graph_.last_error();
	  ran_ = false;
	  handle_ = Handle{};
	}
  }

  Pool& pool_;
  TaskGraph graph_;
  Handle handle_{};
  bool ran_{false};
  std::exception_ptr last_error_{};
};

}  // namespace tp