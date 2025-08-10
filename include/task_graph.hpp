#pragma once
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include "mpmc_queue.hpp"
#include "small_function.hpp"
#include "thread_pool.hpp"

namespace tp {

template <typename T, std::size_t N = 4>
class SmallVec {
 public:
  SmallVec() = default;
  SmallVec(const SmallVec&) = delete;
  SmallVec& operator=(const SmallVec&) = delete;

  SmallVec(SmallVec&&) = default;
  SmallVec& operator=(SmallVec&&) = default;

  void push_back(const T& v) {
	if (use_vec()) {
	  hv_.push_back(v);
	  return;
	}
	if (sz_ < N) {
	  sso_[sz_++] = v;
	  return;
	}
	spill_to_vec();
	hv_.push_back(v);
  }
  void push_back(T&& v) {
	if (use_vec()) {
	  hv_.push_back(std::move(v));
	  return;
	}
	if (sz_ < N) {
	  sso_[sz_++] = std::move(v);
	  return;
	}
	spill_to_vec();
	hv_.push_back(std::move(v));
  }

  [[nodiscard]] std::size_t size() const noexcept { return use_vec() ? hv_.size() : sz_; }
  [[nodiscard]] bool empty() const noexcept { return size() == 0; }

  T* data() noexcept { return use_vec() ? hv_.data() : sso_.data(); }
  [[nodiscard]] const T* data() const noexcept {
	return use_vec() ? hv_.data() : sso_.data();
  }

  T& operator[](std::size_t i) noexcept { return data()[i]; }
  const T& operator[](std::size_t i) const noexcept { return data()[i]; }

  T* begin() noexcept { return data(); }
  T* end() noexcept { return data() + size(); }
  [[nodiscard]] const T* begin() const noexcept { return data(); }
  [[nodiscard]] const T* end() const noexcept { return data() + size(); }

 private:
  [[nodiscard]] bool use_vec() const noexcept { return spilled_; }
  void spill_to_vec() {
	if (spilled_) return;
	hv_.reserve(N * 2);
	for (std::size_t i = 0; i < sz_; ++i) hv_.push_back(std::move(sso_[i]));
	sz_ = 0;
	spilled_ = true;
  }

  std::array<T, N> sso_{};
  std::vector<T> hv_{};
  std::size_t sz_{0};
  bool spilled_{false};
};

class TaskGraph {
 public:
  struct NodeId {
	std::size_t idx{};
  };

  enum class Overflow { Block, Drop, Fail };

  struct NodeOptions {
	Priority priority = Priority::Normal;
	std::optional<uint32_t> affinity{};
	int concurrency = 1;
	std::size_t capacity = SIZE_MAX;
	Overflow overflow = Overflow::Block;
  };

  explicit TaskGraph(Pool& pool) : pool_(pool) {}

  template <class F>
  NodeId add_node(F&& f) {
    return add_node(std::forward<F>(f), NodeOptions{});
  }

  template <class F>
  NodeId add_node(F&& f, NodeOptions opt) {
    require_state_buildable();
    auto n = std::make_unique<Node>();
    n->fn.emplace(std::forward<F>(f));
    n->prio = opt.priority;
    n->affinity = opt.affinity;
    n->max_concurrency = opt.concurrency <= 0 ? INT_MAX : opt.concurrency;
    n->capacity = opt.capacity;
    n->overflow = opt.overflow;
    nodes_.emplace_back(std::move(n));
    state_ = State::Building;
    return NodeId{nodes_.size() - 1};
  }

  void add_edge(const NodeId a, const NodeId b) const {
	require_state_buildable();
	auto* na = nodes_.at(a.idx).get();
	auto* nb = nodes_.at(b.idx).get();
	na->succ.push_back(b.idx);
	++nb->preds_total;
  }

  bool seal() {
	if (state_ == State::Sealed || state_ == State::Idle) return true;
	if (state_ == State::Running) return false;

	std::vector<int> indeg(nodes_.size(), 0);
	for (std::size_t i = 0; i < nodes_.size(); ++i)
	  for (auto j : nodes_[i]->succ) ++indeg[j];

	std::vector<std::size_t> q;
	q.reserve(nodes_.size());
	for (std::size_t i = 0; i < nodes_.size(); ++i)
	  if (indeg[i] == 0) q.push_back(i);

	std::size_t seen = 0;
	for (std::size_t qi = 0; qi < q.size(); ++qi) {
	  auto u = q[qi];
	  ++seen;
	  for (auto v : nodes_[u]->succ)
		if (--indeg[v] == 0) q.push_back(v);
	}
	if (seen != nodes_.size()) return false;

	state_ = State::Sealed;
	return true;
  }

  void clear() {
	ensure_not_running();
	nodes_.clear();
	state_ = State::Building;
  }

  void reset() {
	ensure_not_running();
	for (auto& up : nodes_) {
	  auto* n = up.get();
	  n->preds_remain.store(n->preds_total, std::memory_order_relaxed);
	  n->inflight.store(0, std::memory_order_relaxed);
	  n->queued.store(0, std::memory_order_relaxed);
	  n->scheduled.store(false, std::memory_order_relaxed);
	  while (n->inbox.pop().has_value()) {
	  }
	}
	state_ = (state_ == State::Building) ? State::Building : State::Sealed;
  }

  Handle run() {
	if (nodes_.empty()) return Handle{};
	if (!seal()) throw std::logic_error("TaskGraph: cycle detected in run()");
	reset();
	state_ = State::Running;

	ctx_ = std::make_shared<RunCtx>();
	auto ctr = std::make_shared<Handle::Counter>();
	ctr->count.store(static_cast<int>(nodes_.size()),
					 std::memory_order_relaxed);
	active_ctr_ = ctr;

	auto core = std::make_shared<Core>();
	core->nodes = &nodes_;
	core->pool = &pool_;
	core->ctx = ctx_;

	for (std::size_t i = 0; i < nodes_.size(); ++i) {
	  Node* n = nodes_[i].get();
	  if (n->preds_total == 0) {
		enqueue_token_and_maybe_dispatch(core, i, ctr);
	  }
	}

	return Handle{std::move(ctr)};
  }

  [[nodiscard]] std::exception_ptr last_error() const noexcept {
	if (!ctx_) return nullptr;
	if (ctx_->has_error.load(std::memory_order_acquire)) {
	  return ctx_->err;
	}
	return nullptr;
  }

  [[nodiscard]] std::size_t size() const noexcept { return nodes_.size(); }

  ~TaskGraph() {
	if (active_ctr_) {
	  auto c = active_ctr_;
	  std::unique_lock<std::mutex> lk(c->mu);
	  c->cv.wait(lk,
				 [&] { return c->count.load(std::memory_order_acquire) == 0; });
	  active_ctr_.reset();
	}
  }

  TaskGraph(const TaskGraph&) = delete;
  TaskGraph& operator=(const TaskGraph&) = delete;
  TaskGraph(TaskGraph&&) = delete;
  TaskGraph& operator=(TaskGraph&&) = delete;

 private:
  struct RunCtx {
	std::atomic<bool> cancel{false};
	std::exception_ptr err{};
	std::atomic<bool> has_error{false};
	std::once_flag set_error_once;
  };

  struct Node {
	small_function<void(), 128> fn;
	SmallVec<std::size_t, 4> succ;

	int preds_total{0};
	std::atomic<int> preds_remain{0};

	int max_concurrency{1};
	std::atomic<int> inflight{0};

	std::size_t capacity{SIZE_MAX};
	std::atomic<std::size_t> queued{0};
	detail::mpmc_queue<uint8_t> inbox;

	Overflow overflow{Overflow::Block};

	Priority prio{Priority::Normal};
	std::optional<uint32_t> affinity{};
	std::atomic<bool> scheduled{false};
  };

  struct Core {
	std::vector<std::unique_ptr<Node>>* nodes{};
	Pool* pool{};
	std::shared_ptr<RunCtx> ctx;
  };

  enum class State { Building, Sealed, Running, Idle };

  static inline void complete_counter(Handle::Counter* ctr) {
	if (!ctr) return;
	if (ctr->count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
	  std::lock_guard<std::mutex> lk(ctr->mu);
	  ctr->cv.notify_all();
	}
  }

  static inline void set_error_(RunCtx& ctx, std::exception_ptr eptr) {
	std::call_once(ctx.set_error_once, [&] {
	  ctx.err = std::move(eptr);
	  ctx.has_error.store(true, std::memory_order_release);
	  ctx.cancel.store(true, std::memory_order_release);
	});
  }

  static void enqueue_token_and_maybe_dispatch(
	  const std::shared_ptr<Core>& core, std::size_t i,
	  const std::shared_ptr<Handle::Counter>& ctr) {
	auto& nodes = *core->nodes;
	RunCtx& ctx = *core->ctx;
	assert(i < nodes.size());
	Node* n = nodes[i].get();

	auto try_enqueue = [&]() -> bool {
	  auto cur = n->queued.load(std::memory_order_relaxed);
	  while (cur < n->capacity) {
		if (n->queued.compare_exchange_weak(cur, cur + 1,
											std::memory_order_acq_rel)) {
		  n->inbox.push(uint8_t{1});
		  return true;
		}
	  }
	  return false;
	};

	switch (n->overflow) {
	  case Overflow::Drop:
		(void)try_enqueue();
		break;
	  case Overflow::Fail:
		if (!try_enqueue()) {
		  set_error_(ctx, std::make_exception_ptr(
							  std::runtime_error("TaskGraph: inbox overflow")));
		  return;
		}
		break;
	  case Overflow::Block: {
		unsigned spins = 0;
		while (!try_enqueue()) {
		  if (ctx.cancel.load(std::memory_order_acquire)) return;
		  if (++spins < 64)
			std::this_thread::yield();
		  else {
			std::this_thread::sleep_for(std::chrono::microseconds(50));
			spins = 0;
		  }
		}
		break;
	  }
	}

	maybe_dispatch_node(core, i, ctr);
  }

  static void maybe_dispatch_node(const std::shared_ptr<Core>& core,
								  std::size_t i,
								  const std::shared_ptr<Handle::Counter>& ctr) {
	auto& nodes = *core->nodes;
	Node* n = nodes[i].get();
	int cur = n->inflight.load(std::memory_order_relaxed);
	while (cur < n->max_concurrency) {
	  if (n->inflight.compare_exchange_weak(cur, cur + 1,
											std::memory_order_acq_rel)) {
		schedule_worker_once(core, i, ctr);
		return;
	  }
	}
  }

  static void schedule_worker_once(
	  const std::shared_ptr<Core>& core, std::size_t i,
	  const std::shared_ptr<Handle::Counter>& ctr) {
	auto& nodes = *core->nodes;
	RunCtx& ctx = *core->ctx;
	Node* n = nodes[i].get();

	SubmitOptions opt;
	opt.priority = n->prio;
	opt.affinity = n->affinity;

	core->pool->submit(
		[core, i, ctr] {
		  auto& nodes = *core->nodes;
		  RunCtx& ctx = *core->ctx;

		  Node* self = nodes[i].get();

		  auto tok = self->inbox.pop();
		  if (tok.has_value()) {
			self->queued.fetch_sub(1, std::memory_order_acq_rel);

			if (!ctx.cancel.load(std::memory_order_acquire)) {
			  try {
				if (self->fn) self->fn();
			  } catch (...) {
				set_error_(ctx, std::current_exception());
			  }
			}

			for (auto j : self->succ) {
			  Node* s = nodes[j].get();
			  if (s->preds_remain.fetch_sub(1, std::memory_order_acq_rel) ==
				  1) {
				enqueue_token_and_maybe_dispatch(core, j, ctr);
			  }
			}
		  }

		  self->inflight.fetch_sub(1, std::memory_order_acq_rel);

		  if (!ctx.cancel.load(std::memory_order_acquire)) {
			if (!self->inbox.empty()) {
			  maybe_dispatch_node(core, i, ctr);
			}
		  }

		  complete_counter(ctr.get());
		},
		opt);
  }

  void require_state_buildable() const {
	if (state_ == State::Running)
	  throw std::logic_error("TaskGraph: can't mutate while running");
  }
  void ensure_not_running() const {
	if (state_ == State::Running)
	  throw std::logic_error("TaskGraph: operation is invalid while running");
  }

  Pool& pool_;
  std::vector<std::unique_ptr<Node>> nodes_;
  std::shared_ptr<RunCtx> ctx_;
  std::shared_ptr<Handle::Counter> active_ctr_;
  State state_{State::Building};
};

}  // namespace tp