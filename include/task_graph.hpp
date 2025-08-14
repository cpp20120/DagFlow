#pragma once
/**
 * @file task_graph.hpp
 * @brief Tokenized Directed Acyclic Graph (DAG) scheduler that executes on a
 * Pool.
 *
 * @details
 * Conceptual model:
 *  - A Node contains a `void()` function, successor indices, and runtime
 * counters.
 *  - The graph is built in "Building" state by adding nodes and edges. Each
 * node can be assigned a number of tokens to prime when it becomes ready.
 *  - On run:
 *      * The graph is sealed; a topological check detects cycles.
 *      * Sources (nodes with indegree zero) are primed with their tokens.
 *      * Each token enqueued into a node's inbox leads to one execution of the
 * node's function.
 *      * After an execution completes, each successor's `preds_remain` is
 * decremented; when it reaches zero, the successor is primed with its own
 * tokens.
 *
 * Overflow policy:
 *  - Each node has an inbox capacity and an overflow policy:
 *      * Block: spin/yield/sleep until space is available.
 *      * Drop: best-effort; extra tokens are dropped.
 *      * Fail: immediately set an error and cancel the run.
 *
 * Error and cancellation:
 *  - The first exception thrown by any node is captured and stored in the run
 * context. Further scheduling is cancelled.
 *
 * Thread-safety:
 *  - Graph building is intended to be single-threaded.
 *  - Execution is multi-threaded: tasks are submitted to the provided Pool.
 */

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
#include "small_vec.hpp"
#include "thread_pool.hpp"

namespace tp {
/**
 * @class TaskGraph
 * @brief DAG builder and single-run executor.
 * @details
 * Conceptual model:
 *  - A Node contains a `void()` function, successor indices, and runtime
 * counters.
 *  - The graph is built in "Building" state by adding nodes and edges. Each
 * node can be assigned a number of tokens to prime when it becomes ready.
 *  - On run:
 *      * The graph is sealed; a topological check detects cycles.
 *      * Sources (nodes with indegree zero) are primed with their tokens.
 *      * Each token enqueued into a node's inbox leads to one execution of the
 * node's function.
 *      * After an execution completes, each successor's `preds_remain` is
 * decremented; when it reaches zero, the successor is primed with its own
 * tokens.
 */
class TaskGraph {
 public:
  /**
   * @struct NodeId
   * @brief Opaque identifier of a node within this graph.
   */
  struct NodeId {
	std::size_t idx{};
  };
  /**
   * @enum Overflow
   * @brief Behavior when a node inbox hits capacity.
   *
   * - Block: spin/yield/sleep until space becomes available.
   * - Drop: best-effort enqueue (silently drop tokens on pressure).
   * - Fail: treat overflow as an error and cancel the run.
   */
  enum class Overflow { Block, Drop, Fail };
  /**
   * @struct NodeOptions
   * @brief Node creation options (mirrors @ref tp::ScheduleOptions).
   *
   * @var priority  Submit priority for pool.
   * @var affinity  Optional worker affinity hint.
   * @var concurrency Maximum concurrent executions; `<=0` means unlimited.
   * @var capacity  Inbox capacity (max tokens queued).
   * @var overflow  Enqueue policy when capacity is reached.
   */
  struct NodeOptions {
	Priority priority = Priority::Normal;  ///<	Submit priority for pool.
	std::optional<uint32_t> affinity{};	   ///< Optional worker affinity hint.
	int concurrency =
		1;	///< Maximum concurrent executions; `<=0` means unlimited.
	std::size_t capacity = SIZE_MAX;  ///< Inbox capacity (max tokens queued).
	Overflow overflow =
		Overflow::Block;  ///<  Enqueue policy when capacity is reached.
  };
  /**
   * @brief Bind the graph to a pool.
   */
  explicit TaskGraph(Pool& pool) : pool_(pool) {}
  /**
   * @brief Add node with default options.
   * @tparam F Callable (void()).
   * @param f  Node body (moved).
   * @return NodeId of the created node.
   * @note The node will be primed with `tokens_primed=1` by default; adjust via
   * @ref set_tokens.
   */
  template <class F>
  NodeId add_node(F&& f) {
	return add_node(std::forward<F>(f), NodeOptions{});
  }
  /**
   * @brief Add node with explicit options.
   * @tparam F Callable (void()).
   * @param f   Node body (moved).
   * @param opt Node options (priority, affinity, concurrency, capacity,
   * overflow).
   * @return NodeId of the created node.
   *
   * @throw std::logic_error if the graph is running (mutations forbidden).
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
	n->tokens_primed = 1;  // default one token per node
	nodes_.emplace_back(std::move(n));
	state_ = State::Building;
	return NodeId{nodes_.size() - 1};
  }
  /**
   * @brief Add a directed edge a->b meaning b depends on a.
   * @param a Predecessor node id.
   * @param b Successor node id.
   *
   * @throw std::logic_error if the graph is running (mutations forbidden).
   */
  void add_edge(NodeId a, NodeId b) {
	require_state_buildable();
	auto* na = nodes_.at(a.idx).get();
	auto* nb = nodes_.at(b.idx).get();
	na->succ.push_back(b.idx);
	++nb->preds_total;
  }
  // Set number of tokens (units of work) this node will receive once it becomes
  // ready.
  /**
   * @brief Set number of tokens to prime once the node becomes ready.
   * @param id     Node id.
   * @param tokens Token count (>0). Zero is coerced to 1.
   */
  void set_tokens(NodeId id, std::size_t tokens) {
	require_state_buildable();
	nodes_.at(id.idx)->tokens_primed = (tokens == 0 ? 1 : tokens);
  }
  /**
   * @brief Seal the graph: verify acyclicity and lock topology.
   * @return true if sealed successfully; false if a cycle was detected.
   *
   * @details
   * Runs a Kahn-like topological pass checking that all nodes are visited.
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
  /**
   * @brief Clear all nodes (requires not running).
   * @post State becomes Building.
   */
  void clear() {
	ensure_not_running();
	nodes_.clear();
	state_ = State::Building;
  }
  /**
   * @brief Reset runtime counters for a new run; keeps the current topology.
   * @post State remains Building or Sealed (unchanged).
   */
  void reset() {
	ensure_not_running();
	for (auto& up : nodes_) {
	  auto* n = up.get();
	  n->preds_remain.store(n->preds_total, std::memory_order_relaxed);
	  n->inflight.store(0, std::memory_order_relaxed);
	  n->queued.store(0, std::memory_order_relaxed);
	  while (n->inbox.pop().has_value()) {
	  }
	}
	state_ = (state_ == State::Building) ? State::Building : State::Sealed;
  }
  /**
   * @brief Run the graph once: seal (throw on cycle), reset, and start
   * execution.
   * @return A @ref tp::Handle with a countdown equal to the sum of all tokens
   * in the graph.
   * @throws std::logic_error when a cycle is detected.
   */
  Handle run() {
	if (nodes_.empty()) return Handle{};
	if (!seal()) throw std::logic_error("TaskGraph: cycle detected in run()");
	reset();
	state_ = State::Running;

	ctx_ = std::make_shared<RunCtx>();

	// total work = sum of tokens across all nodes
	int total_tokens = 0;
	for (auto& up : nodes_) total_tokens += static_cast<int>(up->tokens_primed);
	auto ctr = std::make_shared<Handle::Counter>();
	ctr->count.store(total_tokens, std::memory_order_relaxed);
	active_ctr_ = ctr;

	auto core = std::make_shared<Core>();
	core->nodes = &nodes_;
	core->pool = &pool_;
	core->ctx = ctx_;

	// Prime sources: when preds_total==0 enqueue tokens_primed
	for (std::size_t i = 0; i < nodes_.size(); ++i) {
	  Node* n = nodes_[i].get();
	  if (n->preds_total == 0) {
		enqueue_tokens_and_maybe_dispatch(core, i, ctr, n->tokens_primed);
	  }
	}

	return Handle{std::move(ctr)};
  }
  /**
   * @brief The first exception captured during the last run, if any.
   */
  [[nodiscard]] std::exception_ptr last_error() const noexcept {
	if (!ctx_) return nullptr;
	if (ctx_->has_error.load(std::memory_order_acquire)) return ctx_->err;
	return nullptr;
  }
  /**
   * @brief Number of nodes currently in the graph.
   */
  [[nodiscard]] std::size_t size() const noexcept { return nodes_.size(); }
  /// Destructor waits for any active counter to reach zero before releasing.
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
  /**
   * @struct RunCtx
   * @brief Per-run shared context: cancellation and error propagation.
   *
   * @var cancel   Flag to indicate the run should be cancelled.
   * @var err      Stored first exception.
   * @var has_error Whether @ref err is set.
   * @var set_error_once One-time flag to ensure first-exception semantics.
   */
  struct RunCtx {
	std::atomic<bool> cancel{false};
	std::exception_ptr err{};
	std::atomic<bool> has_error{false};
	std::once_flag set_error_once;
  };
  /**
   * @struct Node
   * @brief Runtime state of a node during execution.
   *
   * @var fn Small SSO function wrapper `void()`.
   * @var succ Successor node indices (fan-out).
   * @var preds_total Static indegree; `preds_remain` decremented at runtime.
   * @var max_concurrency Max number of simultaneous executions.
   * @var inflight Current number of executing workers for this node.
   * @var capacity Inbox capacity (max queued tokens).
   * @var queued Current number of queued tokens (approximate).
   * @var inbox Token inbox (uint8_t payload).
   * @var overflow Policy when inbox is full (Block/Drop/Fail).
   * @var prio Submit priority for the pool.
   * @var affinity Optional worker affinity hint.
   * @var tokens_primed Tokens to enqueue when node becomes ready.
   */
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
	std::size_t tokens_primed{1};
  };
  /**
   * @struct Core
   * @brief Shared execution core pointing to node storage, pool, and run
   * context.
   */
  struct Core {
	std::vector<std::unique_ptr<Node>>* nodes{};
	Pool* pool{};
	std::shared_ptr<RunCtx> ctx;
  };
  /// Internal state of the graph.
  enum class State { Building, Sealed, Running, Idle };
  /**
   * @brief Decrement the run counter; notify waiters when it reaches zero.
   */
  static inline void complete_counter(Handle::Counter* ctr) {
	if (!ctr) return;
	if (ctr->count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
	  std::lock_guard<std::mutex> lk(ctr->mu);
	  ctr->cv.notify_all();
	}
  }
  /**
   * @brief Store first exception and request cancellation.
   */
  static inline void set_error_(RunCtx& ctx, std::exception_ptr eptr) {
	std::call_once(ctx.set_error_once, [&] {
	  ctx.err = std::move(eptr);
	  ctx.has_error.store(true, std::memory_order_release);
	  ctx.cancel.store(true, std::memory_order_release);
	});
  }
  /**
   * @brief Try to enqueue a single token into node inbox under capacity limit.
   * @return true if enqueued; false if capacity was full.
   */
  static bool try_enqueue_one(Node& n) {
	auto cur = n.queued.load(std::memory_order_relaxed);
	while (cur < n.capacity) {
	  if (n.queued.compare_exchange_weak(cur, cur + 1,
										 std::memory_order_acq_rel)) {
		n.inbox.push(uint8_t{1});
		return true;
	  }
	}
	return false;
  }
  /**
   * @brief Enqueue @p tokens for node @p i and schedule its worker if needed.
   */
  static void enqueue_tokens_and_maybe_dispatch(
	  const std::shared_ptr<Core>& core, std::size_t i,
	  const std::shared_ptr<Handle::Counter>& ctr, std::size_t tokens) {
	auto& nodes = *core->nodes;
	RunCtx& ctx = *core->ctx;
	Node* n = nodes[i].get();

	auto enqueue_policy = [&](std::size_t count) {
	  for (std::size_t k = 0; k < count; ++k) {
		switch (n->overflow) {
		  case Overflow::Drop:
			(void)try_enqueue_one(*n);
			break;
		  case Overflow::Fail:
			if (!try_enqueue_one(*n)) {
			  set_error_(ctx, std::make_exception_ptr(std::runtime_error(
								  "TaskGraph: inbox overflow")));
			  return;
			}
			break;
		  case Overflow::Block: {
			unsigned spins = 0;
			while (!try_enqueue_one(*n)) {
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
	  }
	};

	enqueue_policy(tokens);
	maybe_dispatch_node(core, i, ctr);
  }
  /**
   * @brief If under concurrency limit, schedule one worker to process a token.
   */
  static void maybe_dispatch_node(const std::shared_ptr<Core>& core,
								  std::size_t i,
								  const std::shared_ptr<Handle::Counter>& ctr) {
	auto& nodes = *core->nodes;
	Node* n = nodes[i].get();

	for (;;) {
	  int cur = n->inflight.load(std::memory_order_relaxed);
	  int free = n->max_concurrency - cur;
	  if (free <= 0) return;

	  std::size_t queued = n->queued.load(std::memory_order_acquire);
	  if (queued == 0) return;

	  int launch = static_cast<int>(std::min<std::size_t>(queued, free));
	  if (n->inflight.compare_exchange_weak(cur, cur + launch,
											std::memory_order_acq_rel,
											std::memory_order_relaxed)) {
		for (int k = 0; k < launch; ++k) {
		  schedule_worker_once(core, i, ctr);
		}
		return;
	  }
	}
  }
  /**
   * @brief Submit a single worker task that pops one token and executes node
   * body.
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

		  bool executed = false;

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
			executed = true;

			// successors: if this execution made the successor ready
			// (preds==0), prime it with its tokens_primed:
			for (auto j : self->succ) {
			  Node* s = nodes[j].get();
			  if (s->preds_remain.fetch_sub(1, std::memory_order_acq_rel) ==
				  1) {
				enqueue_tokens_and_maybe_dispatch(core, j, ctr,
												  s->tokens_primed);
			  }
			}
		  }

		  self->inflight.fetch_sub(1, std::memory_order_acq_rel);

		  if (!ctx.cancel.load(std::memory_order_acquire) &&
			  !self->inbox.empty()) {
			maybe_dispatch_node(core, i, ctr);
		  }

		  if (executed) complete_counter(ctr.get());
		},
		opt);
  }
  /// @throw std::logic_error if the graph is running (mutations forbidden).
  void require_state_buildable() const {
	if (state_ == State::Running)
	  throw std::logic_error("TaskGraph: can't mutate while running");
  }
  /// @throw std::logic_error if called while running.
  void ensure_not_running() const {
	if (state_ == State::Running)
	  throw std::logic_error("TaskGraph: operation invalid while running");
  }

  /// Pool to submit worker tasks to.
  Pool& pool_;

  /// Node storage (ownership).
  std::vector<std::unique_ptr<Node>> nodes_;

  /// Per-run context (cancellation/error).
  std::shared_ptr<RunCtx> ctx_;

  /// Active counter for the current run (if any).
  std::shared_ptr<Handle::Counter> active_ctr_;

  /// Current graph state.
  State state_{State::Building};
};

}  // namespace tp
