/** \file task_graph.hpp
 * \brief Task graph for managing and executing tasks with dependencies.
 *
 * This file defines the TaskGraph class, which manages a directed acyclic graph
 * of tasks and schedules their execution in a thread pool.
 */

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

/** \class SmallVec
 * \brief A small vector with small buffer optimization.
 *
 * \tparam T The type of elements stored in the vector.
 * \tparam N The size of the small buffer (default: 4).
 */
template <typename T, std::size_t N = 4>
class SmallVec {
 public:
  /** \brief Default constructor, initializes an empty vector. */
  SmallVec() = default;

  /** \brief Deleted copy constructor to prevent copying. */
  SmallVec(const SmallVec&) = delete;

  /** \brief Deleted copy assignment operator to prevent copying. */
  SmallVec& operator=(const SmallVec&) = delete;

  /** \brief Move constructor. */
  SmallVec(SmallVec&&) = default;

  /** \brief Move assignment operator. */
  SmallVec& operator=(SmallVec&&) = default;

  /** \brief Adds an element to the vector.
   *
   * \param v The element to add (copied).
   */
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

  /** \brief Adds an element to the vector.
   *
   * \param v The element to add (moved).
   */
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

  /** \brief Gets the number of elements in the vector.
   *
   * \return The number of elements.
   */
  [[nodiscard]] std::size_t size() const noexcept {
	return use_vec() ? hv_.size() : sz_;
  }

  /** \brief Checks if the vector is empty.
   *
   * \return True if the vector is empty, false otherwise.
   */
  [[nodiscard]] bool empty() const noexcept { return size() == 0; }

  /** \brief Gets a pointer to the vector's data.
   *
   * \return Pointer to the data.
   */
  T* data() noexcept { return use_vec() ? hv_.data() : sso_.data(); }

  /** \brief Gets a const pointer to the vector's data.
   *
   * \return Const pointer to the data.
   */
  [[nodiscard]] const T* data() const noexcept {
	return use_vec() ? hv_.data() : sso_.data();
  }

  /** \brief Accesses an element by index.
   *
   * \param i The index of the element.
   * \return Reference to the element.
   */
  T& operator[](std::size_t i) noexcept { return data()[i]; }

  /** \brief Accesses an element by index (const).
   *
   * \param i The index of the element.
   * \return Const reference to the element.
   */
  const T& operator[](std::size_t i) const noexcept { return data()[i]; }

  /** \brief Gets an iterator to the beginning of the vector.
   *
   * \return Pointer to the first element.
   */
  T* begin() noexcept { return data(); }

  /** \brief Gets an iterator to the end of the vector.
   *
   * \return Pointer to one past the last element.
   */
  T* end() noexcept { return data() + size(); }

  /** \brief Gets a const iterator to the beginning of the vector.
   *
   * \return Const pointer to the first element.
   */
  [[nodiscard]] const T* begin() const noexcept { return data(); }

  /** \brief Gets a const iterator to the end of the vector.
   *
   * \return Const pointer to one past the last element.
   */
  [[nodiscard]] const T* end() const noexcept { return data() + size(); }

 private:
  /** \brief Checks if the vector uses the heap-allocated vector.
   *
   * \return True if using the heap vector, false if using SSO.
   */
  [[nodiscard]] bool use_vec() const noexcept { return spilled_; }

  /** \brief Spills the small buffer to the heap vector. */
  void spill_to_vec() {
	if (spilled_) return;
	hv_.reserve(N * 2);
	for (std::size_t i = 0; i < sz_; ++i) hv_.push_back(std::move(sso_[i]));
	sz_ = 0;
	spilled_ = true;
  }

  std::array<T, N> sso_{}; /**< \brief Small buffer for elements. */
  std::vector<T> hv_{};	   /**< \brief Heap-allocated vector for overflow. */
  std::size_t sz_{0};	   /**< \brief Number of elements in SSO. */
  bool spilled_{false}; /**< \brief Whether the vector has spilled to heap. */
};

/** \class TaskGraph
 * \brief Manages a directed acyclic graph of tasks for execution in a thread
 * pool.
 */
class TaskGraph {
 public:
  /** \struct NodeId
   * \brief Identifies a node in the task graph.
   */
  struct NodeId {
	std::size_t idx{}; /**< \brief Index of the node. */
  };

  /** \enum Overflow
   * \brief Policy for handling inbox overflow.
   */
  enum class Overflow {
	Block, /**< \brief Block until space is available. */
	Drop,  /**< \brief Drop the task. */
	Fail   /**< \brief Fail with an exception. */
  };

  /** \struct NodeOptions
   * \brief Configuration options for a task graph node.
   */
  struct NodeOptions {
	Priority priority = Priority::Normal; /**< \brief Task priority. */
	std::optional<uint32_t> affinity{}; /**< \brief Optional worker affinity. */
	int concurrency = 1;				/**< \brief Maximum concurrency. */
	std::size_t capacity = SIZE_MAX;	/**< \brief Inbox capacity. */
	Overflow overflow = Overflow::Block; /**< \brief Overflow policy. */
  };

  /** \brief Constructs a task graph associated with a thread pool.
   *
   * \param pool The thread pool to execute tasks.
   */
  explicit TaskGraph(Pool& pool) : pool_(pool) {}

  /** \brief Adds a node to the task graph with default options.
   *
   * \tparam F The type of the callable.
   * \param f The callable to execute.
   * \return The ID of the added node.
   */
  template <class F>
  NodeId add_node(F&& f) {
	return add_node(std::forward<F>(f), NodeOptions{});
  }

  /** \brief Adds a node to the task graph with specified options.
   *
   * \tparam F The type of the callable.
   * \param f The callable to execute.
   * \param opt Node configuration options.
   * \return The ID of the added node.
   */
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

  /** \brief Adds a directed edge between two nodes.
   *
   * \param a The source node.
   * \param b The destination node.
   */
  void add_edge(const NodeId a, const NodeId b) const {
	require_state_buildable();
	auto* na = nodes_.at(a.idx).get();
	auto* nb = nodes_.at(b.idx).get();
	na->succ.push_back(b.idx);
	++nb->preds_total;
  }

  /** \brief Seals the task graph, checking for cycles.
   *
   * \return True if the graph is acyclic and sealed, false if a cycle is
   * detected.
   */
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

  /** \brief Clears the task graph. */
  void clear() {
	ensure_not_running();
	nodes_.clear();
	state_ = State::Building;
  }

  /** \brief Resets the task graph's execution state. */
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

  /** \brief Runs the task graph.
   *
   * \return A handle to track the execution.
   * \throws std::logic_error If a cycle is detected.
   */
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

  /** \brief Gets the last error encountered during execution.
   *
   * \return The exception pointer or nullptr if no error.
   */
  [[nodiscard]] std::exception_ptr last_error() const noexcept {
	if (!ctx_) return nullptr;
	if (ctx_->has_error.load(std::memory_order_acquire)) {
	  return ctx_->err;
	}
	return nullptr;
  }

  /** \brief Gets the number of nodes in the graph.
   *
   * \return The number of nodes.
   */
  [[nodiscard]] std::size_t size() const noexcept { return nodes_.size(); }

  /** \brief Destroys the task graph, waiting for completion if running. */
  ~TaskGraph() {
	if (active_ctr_) {
	  auto c = active_ctr_;
	  std::unique_lock<std::mutex> lk(c->mu);
	  c->cv.wait(lk,
				 [&] { return c->count.load(std::memory_order_acquire) == 0; });
	  active_ctr_.reset();
	}
  }

  /** \brief Deleted copy constructor to prevent copying. */
  TaskGraph(const TaskGraph&) = delete;

  /** \brief Deleted copy assignment operator to prevent copying. */
  TaskGraph& operator=(const TaskGraph&) = delete;

  /** \brief Deleted move constructor to prevent moving. */
  TaskGraph(TaskGraph&&) = delete;

  /** \brief Deleted move assignment operator to prevent moving. */
  TaskGraph& operator=(TaskGraph&&) = delete;

 private:
  /** \struct RunCtx
   * \brief Runtime context for task graph execution.
   */
  struct RunCtx {
	std::atomic<bool> cancel{false};	/**< \brief Cancellation flag. */
	std::exception_ptr err{};			/**< \brief Last exception. */
	std::atomic<bool> has_error{false}; /**< \brief Error flag. */
	std::once_flag set_error_once; /**< \brief Ensures single error setting. */
  };

  /** \struct Node
   * \brief Represents a node in the task graph.
   */
  struct Node {
	small_function<void(), 128> fn;	  /**< \brief The callable to execute. */
	SmallVec<std::size_t, 4> succ;	  /**< \brief Successor node indices. */
	int preds_total{0};				  /**< \brief Total predecessors. */
	std::atomic<int> preds_remain{0}; /**< \brief Remaining predecessors. */
	int max_concurrency{1};		  /**< \brief Maximum concurrent executions. */
	std::atomic<int> inflight{0}; /**< \brief Current concurrent executions. */
	std::size_t capacity{SIZE_MAX};		/**< \brief Inbox capacity. */
	std::atomic<std::size_t> queued{0}; /**< \brief Number of queued tokens. */
	detail::mpmc_queue<uint8_t> inbox;	/**< \brief Queue for task tokens. */
	Overflow overflow{Overflow::Block}; /**< \brief Overflow policy. */
	Priority prio{Priority::Normal};	/**< \brief Task priority. */
	std::optional<uint32_t> affinity{}; /**< \brief Optional worker affinity. */
	std::atomic<bool> scheduled{
		false}; /**< \brief Whether the node is scheduled. */
  };

  /** \struct Core
   * \brief Core data for task graph execution.
   */
  struct Core {
	std::vector<std::unique_ptr<Node>>*
		nodes{};				 /**< \brief Pointer to nodes. */
	Pool* pool{};				 /**< \brief Pointer to thread pool. */
	std::shared_ptr<RunCtx> ctx; /**< \brief Runtime context. */
  };

  /** \enum State
   * \brief States of the task graph.
   */
  enum class State {
	Building, /**< \brief Graph is being built. */
	Sealed,	  /**< \brief Graph is sealed and ready to run. */
	Running,  /**< \brief Graph is executing. */
	Idle	  /**< \brief Graph is idle. */
  };

  /** \brief Completes a counter when a task finishes.
   *
   * \param ctr The counter to update.
   */
  static inline void complete_counter(Handle::Counter* ctr) {
	if (!ctr) return;
	if (ctr->count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
	  std::lock_guard<std::mutex> lk(ctr->mu);
	  ctr->cv.notify_all();
	}
  }

  /** \brief Sets an error in the runtime context.
   *
   * \param ctx The runtime context.
   * \param eptr The exception pointer to set.
   */
  static inline void set_error_(RunCtx& ctx, std::exception_ptr eptr) {
	std::call_once(ctx.set_error_once, [&] {
	  ctx.err = std::move(eptr);
	  ctx.has_error.store(true, std::memory_order_release);
	  ctx.cancel.store(true, std::memory_order_release);
	});
  }

  /** \brief Enqueues a token and possibly dispatches a node.
   *
   * \param core The core execution data.
   * \param i The node index.
   * \param ctr The counter for tracking completion.
   */
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

  /** \brief Dispatches a node if possible.
   *
   * \param core The core execution data.
   * \param i The node index.
   * \param ctr The counter for tracking completion.
   */
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

  /** \brief Schedules a worker to execute a node.
   *
   * \param core The core execution data.
   * \param i The node index.
   * \param ctr The counter for tracking completion.
   */
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

  /** \brief Ensures the graph is in a buildable state.
   *
   * \throws std::logic_error If the graph is running.
   */
  void require_state_buildable() const {
	if (state_ == State::Running)
	  throw std::logic_error("TaskGraph: can't mutate while running");
  }

  /** \brief Ensures the graph is not running.
   *
   * \throws std::logic_error If the graph is running.
   */
  void ensure_not_running() const {
	if (state_ == State::Running)
	  throw std::logic_error("TaskGraph: operation is invalid while running");
  }

  Pool& pool_; /**< \brief The associated thread pool. */
  std::vector<std::unique_ptr<Node>>
	  nodes_;					/**< \brief The nodes in the graph. */
  std::shared_ptr<RunCtx> ctx_; /**< \brief Runtime context. */
  std::shared_ptr<Handle::Counter> active_ctr_; /**< \brief Active counter. */
  State state_{State::Building}; /**< \brief Current state of the graph. */
};

}  // namespace tp