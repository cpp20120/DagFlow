#pragma once

#include <algorithm>
#include <atomic>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

#include "dagflow/config.hpp"
#include "dagflow/detail/runtime_memory.hpp"
#include "dagflow/detail/small_function.hpp"
#include "dagflow/detail/small_vector.hpp"
#include "dagflow/thread_pool.hpp"

namespace dagflow {

/// Reusable DAG bound to a pool, with at most one active run.
/// Build/mutate/run on one thread; keep the pool alive through destruction.
class TaskGraph {
 public:
  using size_type = std::size_t;
  struct NodeId {
    // Keep the public input wide so invalid IDs are checked before narrowing.
    size_type idx{};
    friend bool operator==(NodeId, NodeId) = default;
  };
  enum class Overflow { Block, Drop, Fail };
  struct NodeOptions {
    Priority priority = Priority::Normal;
    std::optional<uint32_t> affinity{};
    int concurrency = 1;
    /// Maximum admitted live executions. Block defers excess tokens without
    /// blocking a worker; Drop admits only the first capacity tokens; Fail
    /// cancels the run if the initial batch exceeds capacity.
    std::size_t capacity = SIZE_MAX;
    Overflow overflow = Overflow::Block;
  };

  explicit TaskGraph(Pool& pool) : pool_(pool) {}
  template <class F>
  NodeId emplace(F&& f) {
    return emplace(std::forward<F>(f), NodeOptions{});
  }
  template <class F>
  NodeId emplace(F&& f, NodeOptions opt) {
    return emplace_indexed(
        [fn = std::forward<F>(f)](std::size_t) mutable { fn(); }, opt);
  }
  template <class F>
  NodeId add_node(F&& f) {
    return emplace(std::forward<F>(f));
  }
  template <class F>
  NodeId add_node(F&& f, NodeOptions opt) {
    return emplace(std::forward<F>(f), opt);
  }
  void add_edge(NodeId a, NodeId b);
  /// Each node executes this many times per run; zero is treated as one.
  /// Its successors become ready only after the entire node has finished.
  void set_tokens(NodeId id, std::size_t tokens);
  /// Compile CSR and execution options. Returns false for a cycle/active run.
  /// Node/edge counts are limited to UINT32_MAX; token counts remain size_t.
  bool seal();
  void clear();
  /// Discard the last run's result, retaining graph structure and callables.
  void reset();
  /// Start a new run after the previous one has completed.
  Handle run();
  /// Request cooperative cancellation. Already executing callables finish;
  /// unstarted tokens are skipped. Keep run()/reset()/clear() externally
  /// synchronized with this call. Cancellation itself is not an exception.
  void cancel() noexcept;
  [[nodiscard]] std::exception_ptr last_error() const noexcept;
  [[nodiscard]] size_type size() const noexcept {
    return builder_.nodes.size();
  }
  [[nodiscard]] bool empty() const noexcept { return builder_.nodes.empty(); }
  /// Workers cooperatively execute pool work while awaiting destruction.
  ~TaskGraph();
  TaskGraph(const TaskGraph&) = delete;
  TaskGraph& operator=(const TaskGraph&) = delete;
  TaskGraph(TaskGraph&&) = delete;
  TaskGraph& operator=(TaskGraph&&) = delete;

 private:
  friend class GraphScope;
  using index_type = std::uint32_t;
  using Work = small_function<void(std::size_t), DAGFLOW_TASK_FN_SIZE>;
  using Edge = index_type;
  static constexpr index_type no_node = std::numeric_limits<index_type>::max();
  struct BuilderNode {
    small_vector<index_type, DAGFLOW_DEFAULT_SMALL_VECTOR_CAPACITY> successors;
    index_type predecessor_count{0};
    NodeOptions options;
    std::size_t initial_tokens{1};
  };
  struct BuilderTopology {
    std::vector<BuilderNode, detail::RuntimeAllocator<BuilderNode>> nodes;
    // Existing callables stay in compiled storage until the next successful
    // seal. Only newly added nodes own pending callables here.
    std::vector<Work, detail::RuntimeAllocator<Work>> pending_work;
    index_type edge_count{0};
  };
  struct NodeDef {
    // Callable state may change during execution; compiled topology does not.
    mutable Work work;
    SubmitOptions submit;
    std::size_t tokens{0};
    index_type predecessor_count{0};
    index_type edge_begin{0}, edge_end{0};
    index_type lanes{1};
    bool overflowed{false};
  };
  struct CompiledTopology {
    detail::OwnedArray<NodeDef> nodes;
    detail::OwnedArray<Edge> edges;
    detail::OwnedArray<index_type> roots;
    index_type node_count{0}, edge_count{0}, root_count{0};
  };
  struct NodeState {
    std::atomic<index_type> predecessors{0};
    std::atomic<std::size_t> next{0};
    std::atomic<index_type> lanes{0};
  };
  struct RunState;
  // Exactly one initial driver per node/run may borrow this packet. Additional
  // lanes and same-node token continuations use ordinary owning packets. Never
  // reuse this slot within a run: an older driver can still be in Pool's epilogue.
  struct InitialTask : detail::ScheduledTask {
    RunState* run{};
    index_type index{};
    static void invoke(detail::ScheduledTask* task);
    static void release(detail::ScheduledTask* task) noexcept;
    inline static constexpr detail::ScheduledTaskOps operations{
        .invoke = invoke, .destroy = release};
  };
  struct RunState {
    std::span<const NodeDef> nodes;
    std::span<const Edge> edges;
    std::span<NodeState> runtime;
    std::span<InitialTask> initial_tasks;
    Pool* pool{};
    Handle completion;
    std::atomic<bool> cancel{false};
    std::exception_ptr error;
    std::atomic<bool> has_error{false};
    std::once_flag error_once;
  };
  template <class F>
  NodeId emplace_indexed(F&& f, NodeOptions opt) {
    ensure_not_running();
    if (opt.capacity == 0 && opt.overflow == Overflow::Block)
      throw std::invalid_argument(
          "TaskGraph: Block requires positive capacity");
    if (builder_.nodes.size() == no_node)
      throw std::length_error("TaskGraph: too many nodes for uint32_t indices");
    Work work(std::forward<F>(f));
    builder_.nodes.emplace_back();
    try {
      builder_.pending_work.push_back(std::move(work));
    } catch (...) {
      builder_.nodes.pop_back();
      throw;
    }
    builder_.nodes.back().options = opt;
    sealed_ = false;
    return {builder_.nodes.size() - 1};
  }
  static void record_error(RunState& state, detail::CompletionCredit& credit,
                           std::exception_ptr error);
  static void publish(RunState& state, index_type i,
                      detail::CompletionCredit& credit);
  static void publish_initial(RunState& state, index_type i,
                              detail::CompletionCredit& credit);
  /// Prepare the lanes, publishing all except one reserved for the caller.
  static void activate(RunState& state, index_type i,
                       detail::CompletionCredit& credit);
  static void execute(RunState& state, index_type i,
                      detail::CompletionCredit& credit);
  void ensure_not_running() const;

  Pool& pool_;
  BuilderTopology builder_;
  CompiledTopology compiled_;
  detail::OwnedArray<NodeState> runtime_;
  detail::OwnedArray<InitialTask> initial_tasks_;
  size_type runtime_capacity_{0};
  // The graph owns all storage. A live credit grants access through RunState's
  // spans; after completion packet epilogues must not touch graph/run storage.
  detail::OwnedObject<RunState> run_state_;
  bool sealed_{false};
};
}  // namespace dagflow
