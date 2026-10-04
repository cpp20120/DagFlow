#include "dagflow/task_graph.hpp"

#include <limits>

namespace dagflow {
namespace {
constexpr std::size_t minimum_token_count = 1;
constexpr unsigned execution_budget = 64;
}  // namespace

void TaskGraph::add_edge(NodeId a, NodeId b) {
  ensure_not_running();
  auto& from = builder_.nodes.at(a.idx);
  auto& destination = builder_.nodes.at(b.idx);
  if (builder_.edge_count == std::numeric_limits<index_type>::max())
    throw std::length_error("TaskGraph: too many edges for uint32_t offsets");
  from.successors.push_back(static_cast<index_type>(b.idx));
  ++destination.predecessor_count;
  ++builder_.edge_count;
  sealed_ = false;
}

void TaskGraph::set_tokens(NodeId id, std::size_t tokens) {
  ensure_not_running();
  builder_.nodes.at(id.idx).initial_tokens =
      tokens == 0 ? minimum_token_count : tokens;
  sealed_ = false;
}

bool TaskGraph::seal() {
  if (run_state_ && !run_state_->completion.ready()) return false;
  if (sealed_) return true;
  const auto count = static_cast<index_type>(builder_.nodes.size());
  std::vector<index_type, detail::RuntimeAllocator<index_type>> indegree;
  indegree.reserve(count);
  std::vector<index_type, detail::RuntimeAllocator<index_type>> ready;
  ready.reserve(count);
  for (index_type i = 0; i < count; ++i) {
    indegree.push_back(builder_.nodes[i].predecessor_count);
    if (!indegree.back()) ready.push_back(i);
  }
  const auto root_count = static_cast<index_type>(ready.size());
  for (std::size_t i = 0; i < ready.size(); ++i)
    for (auto next : builder_.nodes[ready[i]].successors)
      if (--indegree[next] == 0) ready.push_back(next);
  if (ready.size() != count) return false;

  CompiledTopology next;
  next.nodes = detail::make_owned_array<NodeDef>(count);
  next.edges = detail::make_owned_array<Edge>(builder_.edge_count);
  next.roots = detail::make_owned_array<index_type>(root_count);
  std::copy_n(ready.begin(), root_count, next.roots.get());
  next.node_count = count;
  next.edge_count = builder_.edge_count;
  next.root_count = root_count;
  index_type offset = 0;
  for (index_type i = 0; i < count; ++i) {
    const auto& source = builder_.nodes[i];
    const auto& opt = source.options;
    if (opt.priority != Priority::High && opt.priority != Priority::Normal)
      throw std::invalid_argument("TaskGraph: invalid priority");
    if (opt.overflow != Overflow::Block && opt.overflow != Overflow::Drop &&
        opt.overflow != Overflow::Fail)
      throw std::invalid_argument("TaskGraph: invalid overflow policy");
    if (opt.capacity == 0 && opt.overflow == Overflow::Block)
      throw std::invalid_argument(
          "TaskGraph: Block requires positive capacity");
    auto& node = next.nodes[i];
    node.submit = SubmitOptions{.affinity = opt.affinity, .priority = opt.priority};
    node.tokens = opt.overflow == Overflow::Drop
                      ? std::min(source.initial_tokens, opt.capacity)
                      : source.initial_tokens;
    node.overflowed =
        opt.overflow == Overflow::Fail && source.initial_tokens > opt.capacity;
    const auto concurrency =
        opt.concurrency <= 0
            ? pool_.thread_count()
            : std::min(pool_.thread_count(),
                       static_cast<index_type>(opt.concurrency));
    node.lanes = static_cast<index_type>(std::max<std::size_t>(
        minimum_token_count,
        std::min({node.tokens, opt.capacity, std::size_t{concurrency}})));
    node.predecessor_count = source.predecessor_count;
    node.edge_begin = offset;
    for (auto successor : source.successors) next.edges[offset++] = successor;
    node.edge_end = offset;
  }

  // All potentially throwing work precedes callable transfer. A failed seal
  // leaves both the previous compiled callables and pending callables intact.
  detail::OwnedArray<NodeState> grown_runtime;
  detail::OwnedArray<InitialTask> grown_tasks;
  if (count > runtime_capacity_) {
    grown_runtime = detail::make_owned_array<NodeState>(count);
    grown_tasks = detail::make_owned_array<InitialTask>(count);
  }
  for (index_type i = 0; i < compiled_.node_count; ++i)
    next.nodes[i].work = std::move(compiled_.nodes[i].work);
  for (std::size_t i = 0; i < builder_.pending_work.size(); ++i)
    next.nodes[compiled_.node_count + i].work =
        std::move(builder_.pending_work[i]);
  compiled_ = std::move(next);
  // Do not retain a second array of empty inline callable buffers after seal.
  std::vector<Work, detail::RuntimeAllocator<Work>>{}.swap(builder_.pending_work);
  if (grown_runtime) {
    runtime_ = std::move(grown_runtime);
    initial_tasks_ = std::move(grown_tasks);
    runtime_capacity_ = count;
  }
  sealed_ = true;
  return true;
}

void TaskGraph::clear() {
  ensure_not_running();
  run_state_.reset();
  compiled_ = {};
  builder_.nodes.clear();
  builder_.pending_work.clear();
  builder_.edge_count = 0;
  runtime_.reset();
  initial_tasks_.reset();
  runtime_capacity_ = 0;
  sealed_ = false;
}

void TaskGraph::reset() {
  ensure_not_running();
  // Completion readiness already ends every borrowed graph access. Preserve
  // the allocation, but reconstruct the state (including once_flag and error)
  // rather than trying to reset synchronization primitives piecemeal. Old
  // Handles retain their own immutable completion allocation.
  if (run_state_) {
    static_assert(std::is_nothrow_default_constructible_v<RunState>);
    std::destroy_at(run_state_.get());
    std::construct_at(run_state_.get());
  }
}

Handle TaskGraph::run() {
  ensure_not_running();
  if (empty()) return {};
  if (!seal()) throw std::logic_error("TaskGraph: cycle detected in run()");
  detail::OwnedObject<RunState> owner;
  if (!run_state_) {
    owner = detail::make_owned<RunState>();
    detail::runtime_count(detail::RuntimeEvent::graph_run_state_allocations);
  }
  auto publication = detail::CompletionCredit::create();
  auto result = publication.handle();
  // Allocate the new completion first: failure preserves the previous result.
  // Never recycle CompletionState itself, since old Handles can outlive this
  // run and even the graph. Only the exclusively graph-owned RunState is reused.
  if (run_state_)
    reset();
  else
    run_state_ = std::move(owner);
  auto* state = run_state_.get();
  state->nodes = {compiled_.nodes.get(), compiled_.node_count};
  state->edges = {compiled_.edges.get(), compiled_.edge_count};
  state->pool = &pool_;
  state->runtime = {runtime_.get(), compiled_.node_count};
  state->initial_tasks = {initial_tasks_.get(), compiled_.node_count};
  state->completion = result;
  for (index_type i = 0; i < compiled_.node_count; ++i) {
    // Only joins count arrivals. Roots and single-predecessor nodes have one
    // activator; token/lane state is initialized there, even on a repeated run
    // after cancellation left some nodes untouched.
    if (state->nodes[i].predecessor_count > 1)
      state->runtime[i].predecessors.store(state->nodes[i].predecessor_count,
                                           std::memory_order_relaxed);
  }
  for (index_type root = 0; root < compiled_.root_count; ++root) {
    if (state->cancel.load(std::memory_order_acquire)) break;
    const auto i = compiled_.roots[root];
    activate(*state, i, publication);
    publish_initial(*state, i, publication);
  }
  // The sentinel retires on return. No state access follows its retirement.
  return result;
}

void TaskGraph::cancel() noexcept {
  if (run_state_) run_state_->cancel.store(true, std::memory_order_release);
}

std::exception_ptr TaskGraph::last_error() const noexcept {
  if (run_state_ && run_state_->has_error.load(std::memory_order_acquire))
    return run_state_->error;
  return nullptr;
}

TaskGraph::~TaskGraph() {
  if (run_state_) pool_.wait(run_state_->completion);
}

void TaskGraph::record_error(RunState& state, detail::CompletionCredit& credit,
                             std::exception_ptr error) {
  std::call_once(state.error_once, [&] {
    state.error = std::move(error);
    credit.fail(state.error);
    state.has_error.store(true, std::memory_order_release);
    state.cancel.store(true, std::memory_order_release);
  });
}

void TaskGraph::activate(RunState& state, index_type i,
                         detail::CompletionCredit& credit) {
  const auto& node = state.nodes[i];
  auto& runtime = state.runtime[i];
  if (node.overflowed) {
    try {
      throw std::runtime_error("TaskGraph: inbox overflow");
    } catch (...) {
      record_error(state, credit, std::current_exception());
    }
  }
  const auto lanes =
      state.cancel.load(std::memory_order_acquire) ? 1 : node.lanes;
  runtime.next.store(0, std::memory_order_relaxed);
  runtime.lanes.store(lanes, std::memory_order_relaxed);
  // The final lane is owned by the caller, keeping this node alive while the
  // other lanes can already execute on another worker.
  for (index_type k = 1; k < lanes; ++k) publish(state, i, credit);
}

void TaskGraph::publish(RunState& state, index_type i,
                        detail::CompletionCredit& credit) {
  const auto opt = state.nodes[i].submit;
  try {
    state.pool->enqueue(
        [borrowed = &state, i](detail::CompletionCredit& accepted) {
          execute(*borrowed, i, accepted);
        },
        opt, credit.fork());
  } catch (...) {
    // Failed publication has already retired its child credit. The caller's
    // credit still protects state while we cancel and retire the reserved lane.
    record_error(state, credit, std::current_exception());
    execute(state, i, credit);
  }
}

void TaskGraph::InitialTask::invoke(detail::ScheduledTask* task) {
  auto* self = static_cast<InitialTask*>(task);
  execute(*self->run, self->index, self->done);
}

void TaskGraph::InitialTask::release(detail::ScheduledTask* task) noexcept {
  // Pool normally moved the credit out already. Rejected publication instead
  // arrives here with a live credit. End every slot access before retiring it:
  // completion may immediately permit graph destruction or a subsequent run.
  // The array owns object lifetime; releasing a borrow must not destroy/free it.
  auto done = std::move(task->done);
  done.finish();
}

void TaskGraph::publish_initial(RunState& state, index_type i,
                                detail::CompletionCredit& credit) {
  auto& task = state.initial_tasks[i];
  assert(!task.done);
  task.ops = &InitialTask::operations;
  task.prio = state.nodes[i].submit.priority;
  task.run = &state;
  task.index = i;
  task.done = credit.fork();
#if defined(DAGFLOW_RUNTIME_DIAGNOSTICS)
  // This is a borrowed array slot, not a per-publication allocation/free.
  task.allocating_thread = UINT64_MAX;
#endif
  try {
    state.pool->enqueue_prepared(detail::OwnedScheduledTask{&task},
                                 state.nodes[i].submit);
  } catch (...) {
    // The erased owner releases the slot's child credit on rejection. The
    // caller still owns this lane and state; cancel and retire it as usual.
    record_error(state, credit, std::current_exception());
    execute(state, i, credit);
  }
  // Publication transfers the borrow. No slot access follows, even if a worker
  // completed the packet before enqueue_prepared returned.
}

void TaskGraph::execute(RunState& state, index_type i,
                        detail::CompletionCredit& credit) {
  unsigned budget = execution_budget;
  for (;;) {
    const auto& node = state.nodes[i];
    auto& runtime = state.runtime[i];
    // An ordinary DAG node has one admitted invocation and therefore one lane.
    // It cannot need a cursor or token continuation. Select this path once per
    // node, keeping tokenized nodes on their original bounded CAS claim loop.
    if (node.tokens == 1) {
      if (!state.cancel.load(std::memory_order_acquire)) {
        try {
          node.work(0);
        } catch (...) {
          record_error(state, credit, std::current_exception());
        }
        --budget;
      }
    } else {
      while (!state.cancel.load(std::memory_order_acquire)) {
        auto token = runtime.next.load(std::memory_order_relaxed);
        if (token >= node.tokens) break;
        // fetch_add would allow competing lanes to wrap the cursor at SIZE_MAX.
        if (!runtime.next.compare_exchange_weak(
                token, token + 1, std::memory_order_relaxed))
          continue;
        try {
          node.work(token);
        } catch (...) {
          record_error(state, credit, std::current_exception());
        }
        // At the budget boundary, yield only for remaining unclaimed work.
        // Another lane may claim it after this probe; the racing continuation
        // still owns a lane and can safely retire it. The cursor never goes
        // backwards, so observing exhaustion also prevents budget underflow.
        if (--budget == 0 && !state.cancel.load(std::memory_order_acquire) &&
            runtime.next.load(std::memory_order_relaxed) < node.tokens) {
          detail::runtime_count(detail::RuntimeEvent::graph_continuations);
          publish(state, i, credit);
          return;
        }
      }
    }

    index_type next = no_node;
    // Only concurrent lanes need an acquire/release join. For a compiled single
    // lane this driver owns completion and all callable writes. Do not use a
    // cancellation-time live count to bypass a multi-lane node's final join.
    if ((node.lanes == 1 ||
         runtime.lanes.fetch_sub(1, std::memory_order_acq_rel) == 1) &&
        !state.cancel.load(std::memory_order_acquire)) {
      for (index_type edge = node.edge_begin; edge < node.edge_end; ++edge) {
        if (state.cancel.load(std::memory_order_acquire)) break;
        const auto j = state.edges[edge];
        // One incoming edge has exactly one releaser. With multiple incoming
        // edges (including duplicates), the final acq_rel arrival gathers all
        // predecessor writes before activation; do not weaken that join.
        if (state.nodes[j].predecessor_count > 1 &&
            state.runtime[j].predecessors.fetch_sub(
                1, std::memory_order_acq_rel) != 1)
          continue;
        activate(state, j, credit);
        const auto& candidate = state.nodes[j].submit;
        // Do not bypass an affinity or priority change.
        if (next == no_node && candidate.affinity == node.submit.affinity &&
            candidate.priority == node.submit.priority)
          next = j;
        else
          publish_initial(state, j, credit);
      }
    }
    if (next == no_node) break;
    if ((budget == 0 || --budget == 0) &&
        !state.cancel.load(std::memory_order_acquire)) {
      detail::runtime_count(detail::RuntimeEvent::graph_continuations);
      publish_initial(state, next, credit);
      break;
    }
    i = next;
  }
  // End borrowed access. The executor destroys the wrapper and then retires
  // its credit. The graph may immediately destroy RunState after ready().
}

void TaskGraph::ensure_not_running() const {
  if (run_state_ && !run_state_->completion.ready())
    throw std::logic_error("TaskGraph: operation invalid while running");
}
}  // namespace dagflow
