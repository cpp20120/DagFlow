#include <atomic>
#include <chrono>
#include <dagflow/detail/fuzz_points.hpp>
#include <dagflow/task_graph.hpp>
#include <dagflow/thread_pool.hpp>
#include <thread>
#include <vector>

#include "support.hpp"

// Test-only scheduling checkpoints isolate the narrow windows that can be
// skipped by probabilistic stress: cancel after propagation begins and cancel
// at the budget boundary of a same-affinity inline continuation.
namespace {
namespace hooks = dagflow::detail::fuzz_points;
struct Gate {
  hooks::Point point;
  unsigned threshold;
  std::atomic<unsigned> hits{0}, phase{0};
};
void stop_at(hooks::Point point, void* context) noexcept {
  auto& gate = *static_cast<Gate*>(context);
  if (point != gate.point) return;
  const unsigned hit = gate.hits.fetch_add(1, std::memory_order_acq_rel) + 1;
  if (hit != gate.threshold) return;
  gate.phase.store(1, std::memory_order_release);
  while (gate.phase.load(std::memory_order_acquire) != 2)
    std::this_thread::yield();
}
void finish_at_boundary(Gate& gate, dagflow::TaskGraph& graph) {
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(5);
  while (gate.phase.load(std::memory_order_acquire) != 1 &&
         std::chrono::steady_clock::now() < deadline)
    std::this_thread::yield();
  const bool reached = gate.phase.load(std::memory_order_acquire) == 1;
  graph.cancel();
  gate.phase.store(2, std::memory_order_release);
  CHECK(reached);
}
void dependency_walk() {
  dagflow::Config config;
  config.threads = 2;
  dagflow::Pool pool(config);
  std::atomic<unsigned> invoked{0};
  dagflow::TaskGraph graph(pool);
  auto root = graph.emplace([&]{ invoked.fetch_add(1); });
  for (unsigned i = 0; i < 96; ++i) {
    auto leaf = graph.emplace([&]{ invoked.fetch_add(1); });
    graph.add_edge(root, leaf);
  }
  Gate gate{hooks::Point::graph_before_dependency_check, 1};
  hooks::install(stop_at, &gate);
  auto result = graph.run();
  finish_at_boundary(gate, graph);
  pool.wait_and_rethrow(result);
  hooks::clear();
  CHECK(invoked == 1);
}
void inline_budget() {
  dagflow::Config config;
  config.threads = 1;
  dagflow::Pool pool(config);
  std::atomic<unsigned> invoked{0};
  dagflow::TaskGraph graph(pool);
  std::vector<dagflow::TaskGraph::NodeId> chain;
  for (unsigned i = 0; i < 66; ++i)
    chain.push_back(graph.emplace([&]{ invoked.fetch_add(1); }));
  for (std::size_t i = 1; i < chain.size(); ++i)
    graph.add_edge(chain[i - 1], chain[i]);
  // Every node consumes one unit of budget; each inline jump another.
  // On the 32nd transition the remaining budget becomes zero.
  Gate gate{hooks::Point::graph_before_inline_continuation, 32};
  hooks::install(stop_at, &gate);
  auto result = graph.run();
  finish_at_boundary(gate, graph);
  pool.wait_and_rethrow(result);
  hooks::clear();
  CHECK(invoked == 32);
}
} // namespace
int main() {
  for (unsigned round = 0; round < 8; ++round) {
    dependency_walk();
    inline_budget();
  }
}
