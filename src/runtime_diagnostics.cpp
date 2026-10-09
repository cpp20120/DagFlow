#include <dagflow/detail/runtime_diagnostics.hpp>

#if defined(DAGFLOW_RUNTIME_DIAGNOSTICS)
#include <algorithm>
#include <atomic>

namespace dagflow::detail {
namespace {
constexpr std::size_t private_lane_count = 4096;
struct alignas(64) Lane {
  std::array<std::atomic<uint64_t>, runtime_event_names.size()> values{};
};
// Fixed, process-lifetime storage: hot instrumentation never allocates or
// takes locks, and exited producer threads leave their counters available.
// Atomic owner writes permit a race-free diagnostic snapshot while workers
// probe/park. Relaxed RMWs still cost time: this build is NOT a timing
// baseline.
Lane lanes[private_lane_count + 1];
std::atomic<uint64_t> next_thread{0};
}  // namespace
uint64_t runtime_diagnostic_thread() noexcept {
  static thread_local const uint64_t id =
      next_thread.fetch_add(1, std::memory_order_relaxed);
  return id;
}
void runtime_count(RuntimeEvent event, uint64_t amount) noexcept {
  const auto lane =
      std::min<uint64_t>(runtime_diagnostic_thread(), private_lane_count);
  lanes[lane].values[static_cast<std::size_t>(event)].fetch_add(
      amount, std::memory_order_relaxed);
}
RuntimeCounts runtime_counts() noexcept {
  RuntimeCounts result{};
  const auto count = std::min<uint64_t>(
      next_thread.load(std::memory_order_relaxed), private_lane_count + 1);
  for (std::size_t lane = 0; lane < count; ++lane)
    for (std::size_t i = 0; i < result.size(); ++i)
      result[i] += lanes[lane].values[i].load(std::memory_order_relaxed);
  return result;
}
uint64_t runtime_diagnostic_overflow_threads() noexcept {
  const auto count = next_thread.load(std::memory_order_relaxed);
  return count > private_lane_count ? count - private_lane_count : 0;
}
}  // namespace dagflow::detail
#endif
