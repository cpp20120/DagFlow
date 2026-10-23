#pragma once
// Fuzz-only scheduling checkpoints. No instrumentation or state in normal
// builds.
#if defined(DAGFLOW_FUZZ_HOOKS)
#include <atomic>
#include <cstdint>
#include <thread>
namespace dagflow::detail::fuzz_points {
enum class Point : unsigned {
  idle_announce,
  before_cv,
  wake_claim,
  external_publish,
  before_execute,
  before_retire,
  external_retry,
  graph_before_dependency_check,
  graph_before_inline_continuation,
  deque_before_publish,
  deque_after_owner_claim,
  deque_before_owner_cas,
  deque_after_thief_snapshot,
  deque_before_thief_cas,
  completion_before_ready,
  completion_after_ready,
  completion_before_register,
  scope_before_cancel_check,
  scope_after_cancel_check,
  task_before_destroy,
  task_after_destroy,
  parking_predicate_false,
  parking_timed_out,
  graph_before_lane_join,
  graph_after_lane_join,
  shutdown_before_drain
};
inline thread_local int allocation_budget = -1;
using Callback = void (*)(Point, void*) noexcept;
inline std::atomic<Callback> callback{nullptr};
inline std::atomic<void*> context{nullptr};
inline void install(Callback fn, void* ctx) noexcept {
  context.store(ctx, std::memory_order_relaxed);
  callback.store(fn, std::memory_order_release);
}
inline void clear() noexcept {
  callback.store(nullptr, std::memory_order_release);
}
inline void hit(Point p) noexcept {
  if (auto fn = callback.load(std::memory_order_acquire))
    fn(p, context.load(std::memory_order_relaxed));
}
}  // namespace dagflow::detail::fuzz_points
#define DAGFLOW_FUZZ_POINT(name)       \
  ::dagflow::detail::fuzz_points::hit( \
      ::dagflow::detail::fuzz_points::Point::name)
#else
#define DAGFLOW_FUZZ_POINT(name) ((void)0)
#endif
