#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <ranges>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include <dagflow/task_graph.hpp>
#include <dagflow/thread_pool.hpp>

namespace dagflow {

using ScheduleOptions = TaskGraph::NodeOptions;

class JobHandle {
 public:
  /// Constructs an invalid handle.
  JobHandle() = default;

  /// Constructs a handle referencing the internal node index @p id.
  explicit JobHandle(std::size_t id) : id_(id) {}

  /// @return true if this handle refers to a real node.
  [[nodiscard]] bool valid() const noexcept { return id_ != npos; }
  explicit operator bool() const noexcept { return valid(); }
  friend bool operator==(JobHandle, JobHandle) = default;

  /// @return underlying node identifier.
  [[nodiscard]] std::size_t id() const noexcept { return id_; }

 private:
  /// Internal node id or npos if invalid.
  std::size_t id_ = npos;

  /// Sentinel for an invalid handle.
  static constexpr std::size_t npos = static_cast<std::size_t>(-1);
};

/// Reusable graph builder. Destruction finishes active or unstarted work.
class GraphScope {
 public:
  using size_type = TaskGraph::size_type;

  [[nodiscard]] size_type size() const noexcept { return graph_.size(); }
  [[nodiscard]] bool empty() const noexcept { return graph_.empty(); }

  /// Construct a GraphScope bound to a Pool.
  explicit GraphScope(Pool& pool) : pool_(pool), graph_(pool) {}

  template <class F>
  JobHandle emplace(F&& f, const ScheduleOptions& opt = {}) {
    auto nid = graph_.emplace(std::forward<F>(f), opt);
    dirty_ = true;
    return JobHandle{nid.idx};
  }

  /// Compatibility spelling; emplace() builds a node without executing it.
  template <class F>
  JobHandle submit(F&& f, const ScheduleOptions& opt = {}) {
    return emplace(std::forward<F>(f), opt);
  }

  template <class F>
  JobHandle then(JobHandle dep, F&& f, const ScheduleOptions& opt = {}) {
    auto h = emplace(std::forward<F>(f), opt);
    graph_.add_edge({dep.id()}, {h.id()});
    return h;
  }

  template <class F>
  JobHandle when_all(std::span<const JobHandle> deps, F&& f,
                     const ScheduleOptions& opt = {}) {
    auto h = emplace(std::forward<F>(f), opt);
    for (auto& d : deps) graph_.add_edge({d.id()}, {h.id()});
    return h;
  }

  template <class F>
  JobHandle when_all(std::initializer_list<JobHandle> deps, F&& f,
                     const ScheduleOptions& opt = {}) {
    return when_all(std::span(deps.begin(), deps.size()), std::forward<F>(f),
                    opt);
  }

  /// Build a tokenized node with a separate callable copy per chunk.
  template <class It, class F>
  JobHandle parallel_for(It begin, It end, F&& f,
                         const ScheduleOptions& opt = {}) {
    using FnType = std::decay_t<F>;
    struct Block {
      It b, e;
      FnType fn;
    };
    struct Parts {
      std::vector<Block, detail::RuntimeAllocator<Block>> blocks;
    };
    auto parts = detail::make_owned<Parts>();

    const auto n = static_cast<std::size_t>(std::distance(begin, end));
    if (n == 0) return emplace([] {}, opt);

    constexpr std::size_t target = DAGFLOW_DEFAULT_RANGE_CHUNK;
    const std::size_t chunks =
        std::max<std::size_t>(1, (n + target - 1) / target);

    static_assert(std::is_copy_constructible_v<FnType>,
                  "parallel_for stores a callable copy per chunk");
    FnType callable(std::forward<F>(f));
    parts->blocks.reserve(chunks);
    for (std::size_t c = 0; c < chunks; ++c) {
      std::size_t lo = c * n / chunks;
      std::size_t hi = (c + 1) * n / chunks;

      It bb = std::next(begin, static_cast<std::iter_difference_t<It>>(lo));
      It ee = std::next(begin, static_cast<std::iter_difference_t<It>>(hi));

      parts->blocks.push_back(Block{bb, ee, callable});
    }

    const auto token_count = parts->blocks.size();
    auto nid = graph_.emplace_indexed(
        [parts = std::move(parts)](std::size_t i) {
          auto& p = parts->blocks[i];
          for (auto it = p.b; it != p.e; ++it) p.fn(*it);
        },
        opt);

    graph_.set_tokens(nid, token_count);
    dirty_ = true;
    return JobHandle{nid.idx};
  }

  /// Build from an lvalue range or a borrowed temporary (e.g. span). Iterators
  /// are retained for every run: backing data and iterator-owning views must
  /// remain valid until the node is removed or this scope is destroyed.
  /// Each chunk owns a callable copy, as in the iterator overload.
  template <std::ranges::forward_range R, class F>
    requires std::ranges::borrowed_range<R>
  JobHandle parallel_for(R&& range, F&& f, const ScheduleOptions& opt = {}) {
    auto begin = std::ranges::begin(range);
    auto end = std::ranges::next(begin, std::ranges::end(range));
    return parallel_for(begin, end, std::forward<F>(f), opt);
  }

  template <class It, class F>
  JobHandle parallel_for_after(JobHandle dep, It begin, It end, F&& f,
                               const ScheduleOptions& opt = {}) {
    auto h = parallel_for(begin, end, std::forward<F>(f), opt);
    graph_.add_edge({dep.id()}, {h.id()});
    return h;
  }

  /// Range overload; retains the same borrowed iterators as parallel_for().
  template <std::ranges::forward_range R, class F>
    requires std::ranges::borrowed_range<R>
  JobHandle parallel_for_after(JobHandle dep, R&& range, F&& f,
                               const ScheduleOptions& opt = {}) {
    auto begin = std::ranges::begin(range);
    auto end = std::ranges::next(begin, std::ranges::end(range));
    return parallel_for_after(dep, begin, end, std::forward<F>(f), opt);
  }

  /// Start once; returns the same handle until wait() consumes it.
  Handle run() {
    if (ran_) return handle_;
    handle_ = graph_.run();
    ran_ = true;
    dirty_ = false;
    return handle_;
  }

  /// Start pending work if needed, wait, and collect its error without
  /// throwing.
  void wait() {
    if (!ran_ && dirty_) run();
    if (!ran_) return;
    if (handle_.valid()) pool_.wait(handle_);
    last_error_ = graph_.last_error();
    ran_ = false;
    handle_ = {};
  }

  /// Run, wait, and rethrow the first captured task exception.
  void run_and_wait() {
    run();
    wait();
    if (auto ep = last_error_) std::rethrow_exception(ep);
  }

  /// @return The first captured exception from the last execution, or nullptr
  /// if none.
  [[nodiscard]] std::exception_ptr last_error() const noexcept {
    return last_error_;
  }

  /// Finish an active run before clearing the graph.
  void clear() {
    finish_active_run();
    graph_.clear();
    dirty_ = false;
    last_error_ = nullptr;
  }

  ~GraphScope() {
    try {
      if (!ran_ && dirty_ && !graph_.empty()) run();
      if (ran_) wait();
    } catch (...) {
    }
  }

 private:
  /// Ensure no pending run is in progress; wait it out and collect last_error_.
  void finish_active_run() {
    if (ran_) {
      if (handle_.valid()) pool_.wait(handle_);
      last_error_ = graph_.last_error();
      ran_ = false;
      handle_ = Handle{};
    }
  }

  /// Reference to the underlying pool used for execution.
  Pool& pool_;

  /// The underlying TaskGraph managed by this scope.
  TaskGraph graph_;

  /// The handle of the last run, if any.
  Handle handle_{};

  /// Whether a run is currently active and has not been waited yet.
  bool ran_{false};
  bool dirty_{false};

  /// Last exception captured by the underlying TaskGraph during the last run.
  std::exception_ptr last_error_{};
};

}  // namespace dagflow
