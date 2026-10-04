#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace dagflow::detail {
// Diagnostic builds only. These process-wide counters are not a Pool API or a
// consistent live snapshot. Read deltas around a drained workload. Idle workers
// may still change probe/parking counters at the boundaries. Never use counts
// for scheduling decisions or lifetime/termination tests.
enum class RuntimeEvent : std::size_t {
  packets,
  external_submit,
  worker_submit,
  inline_execute,
  external_retry,
  local_push,
  local_spill,
  ingress_push,
  ingress_full,
  local_acquire,
  drain_tasks,
  remote_drain_tasks,
  steal_probe,
  steal_empty,
  steal_lost,
  stolen_tasks,
  wake_call,
  wake_signal,
  park_call,
  park_timeout,
  help_attempt,
  help_success,
  executed,
  cross_thread_free,
  completion_created,
  external_batch,
  batch_tasks,
  memory_allocations,
  memory_requested_bytes,
  memory_deallocations,
  packet_allocations,
  packet_allocated_bytes,
  graph_run_state_allocations,
  graph_continuations,
  overflow_push,
  overflow_acquire,
  count
};
inline constexpr std::array<std::string_view,
                            static_cast<std::size_t>(RuntimeEvent::count)>
    runtime_event_names{"packets",
                        "external_submit",
                        "worker_submit",
                        "inline_execute",
                        "external_retry",
                        "local_push",
                        "local_spill",
                        "ingress_push",
                        "ingress_full",
                        "local_acquire",
                        "drain_tasks",
                        "remote_drain_tasks",
                        "steal_probe",
                        "steal_empty",
                        "steal_lost",
                        "stolen_tasks",
                        "wake_call",
                        "wake_signal",
                        "park_call",
                        "park_timeout",
                        "help_attempt",
                        "help_success",
                        "executed",
                        "cross_thread_free",
                        "completion_created",
                        "external_batch",
                        "batch_tasks",
                        "memory_allocations",
                        "memory_requested_bytes",
                        "memory_deallocations",
                        "packet_allocations",
                        "packet_allocated_bytes",
                        "graph_run_state_allocations",
                        "graph_continuations",
                        "overflow_push",
                        "overflow_acquire"};
using RuntimeCounts = std::array<uint64_t, runtime_event_names.size()>;
#if defined(DAGFLOW_RUNTIME_DIAGNOSTICS)
inline constexpr bool runtime_diagnostics_enabled = true;
void runtime_count(RuntimeEvent event, uint64_t amount = 1) noexcept;
uint64_t runtime_diagnostic_thread() noexcept;
RuntimeCounts runtime_counts() noexcept;
// Above the private-lane limit counts stay exact but use a shared overflow
// lane.
uint64_t runtime_diagnostic_overflow_threads() noexcept;
#else
inline constexpr bool runtime_diagnostics_enabled = false;
inline void runtime_count(RuntimeEvent, uint64_t = 1) noexcept {}
inline RuntimeCounts runtime_counts() noexcept { return {}; }
inline uint64_t runtime_diagnostic_overflow_threads() noexcept { return 0; }
#endif
}  // namespace dagflow::detail
