#pragma once

// API spelling adapter only: the legacy runtime itself is built unchanged.
#ifdef DAGFLOW_SUITE_LEGACY
#include <array>
#include <climits>
#include <cstdint>
#include <utility>
#include <stdexcept>
#include "thread_pool.hpp"
#include "task_graph.hpp"

namespace suite_backend {
inline constexpr const char* name = "github";
class Pool : public dagflow::Pool {
 public:
  using dagflow::Pool::Pool;
  template <class F> void submit_detached(F&& f) {
    dagflow::SubmitOptions options;
    options.skip_counter = true;
    submit(std::forward<F>(f), options);
  }
  template <class Range> void submit_batch_detached(Range) {
    throw std::invalid_argument("legacy API has no detached batch submission");
  }
};
class TaskGraph : public dagflow::TaskGraph {
 public:
  using dagflow::TaskGraph::TaskGraph;
  template <class F> NodeId emplace(F&& f) {
    return add_node(std::forward<F>(f));
  }
  template <class F> NodeId emplace(F&& f, NodeOptions options) {
    return add_node(std::forward<F>(f), options);
  }
};
// Legacy handles have no error channel. Work's output validation still runs.
inline void rethrow(const dagflow::Handle&) {}
inline void rethrow_graph(const TaskGraph& graph, const dagflow::Handle&) {
  if (auto error = graph.last_error()) std::rethrow_exception(error);
}
}  // namespace suite_backend

namespace dagflow::detail {
using RuntimeCounts = std::array<std::uint64_t, 0>;
inline RuntimeCounts runtime_counts() { return {}; }
inline constexpr bool runtime_diagnostics_enabled = false;
inline constexpr std::array<const char*, 0> runtime_event_names{};
inline const char* allocator_backend() { return "system"; }
}  // namespace dagflow::detail
#else
#include <dagflow/dagflow.hpp>
#include <dagflow/detail/runtime_diagnostics.hpp>
namespace suite_backend {
inline constexpr const char* name = "current";
using Pool = dagflow::Pool;
using TaskGraph = dagflow::TaskGraph;
inline void rethrow(const dagflow::Handle& handle) { handle.rethrow_if_failed(); }
inline void rethrow_graph(const TaskGraph&, const dagflow::Handle& handle) {
  rethrow(handle);
}
}  // namespace suite_backend
#endif
