#include <array>
#include <atomic>
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <dagflow/dagflow.hpp>
#include <memory>
#include <stdexcept>
#include <vector>

#include "property_support.hpp"

namespace {
using namespace dagflow::test::property;
constexpr unsigned max_nodes = 8;

enum Transition : unsigned {
  BlockTokens, DropTokens, EmptyDrop, FanIn, FanOut, Reuse, Failure,
  Cancellation, Clear, TransitionCount
};
constexpr std::array<const char*, TransitionCount> names{
    "block tokens", "drop tokens", "drop all", "join", "fork",
    "replay", "failure", "cancellation", "clear"};
std::array<std::atomic<unsigned>, TransitionCount> observed{};
void mark(Transition t) { observed[t].fetch_add(1, std::memory_order_relaxed); }

struct NodeSpec {
  unsigned requested{};
  unsigned capacity{};
  dagflow::TaskGraph::Overflow policy{};
  dagflow::Priority priority{};
  unsigned lanes{};
};
struct GraphSpec {
  unsigned size{};
  std::array<NodeSpec, max_nodes> nodes{};
  std::array<std::array<unsigned, max_nodes>, max_nodes> edges{};
};

// Independent *sequential* specification. No run-state, credit, lane,
// CSR, atomic predecessor counter or packet logic from the SUT is used here.
struct Oracle {
  std::array<unsigned, max_nodes> expected{};
  std::array<std::array<bool, max_nodes>, max_nodes> ancestors{};
  bool has_join{}, has_fork{};

  explicit Oracle(const GraphSpec& graph) {
    for (unsigned i = 0; i < graph.size; ++i) {
      const auto& node = graph.nodes[i];
      const unsigned normalized = std::max(1u, node.requested);
      expected[i] = node.policy == dagflow::TaskGraph::Overflow::Drop
                        ? std::min(normalized, node.capacity) : normalized;
      unsigned predecessor_edges = 0, outgoing_edges = 0;
      for (unsigned j = 0; j < graph.size; ++j) {
        predecessor_edges += graph.edges[j][i];
        outgoing_edges += graph.edges[i][j];
        ancestors[i][j] = graph.edges[j][i] != 0;
      }
      has_join |= predecessor_edges > 1;
      has_fork |= outgoing_edges > 1;
    }
    // Reachability closure, independent of TaskGraph's Kahn/CSR compilation.
    for (unsigned via = 0; via < graph.size; ++via)
      for (unsigned dst = 0; dst < graph.size; ++dst)
        for (unsigned src = 0; src < graph.size; ++src)
          ancestors[dst][src] = ancestors[dst][src] ||
                                 (ancestors[dst][via] && ancestors[via][src]);
  }

  bool accepts(const GraphSpec& graph,
               const std::array<unsigned, max_nodes>& actual,
               bool invalid_order) const {
    if (invalid_order) return false;
    for (unsigned i = 0; i < graph.size; ++i)
      if (actual[i] != expected[i]) return false;
    return true;
  }
};

GraphSpec generate_spec(const Command& command) {
  GraphSpec spec;
  spec.size = 1 + static_cast<unsigned>(command.a % max_nodes);
  Random rng(command.b + 0xa0761d6478bd642fULL);
  constexpr std::array<unsigned, 7> tokens{0, 1, 2, 3, 7, 65, 129};
  for (unsigned i = 0; i < spec.size; ++i) {
    auto& n = spec.nodes[i];
    n.requested = tokens[rng.bounded(tokens.size())];
    n.policy = rng.bounded(2) ? dagflow::TaskGraph::Overflow::Block
                              : dagflow::TaskGraph::Overflow::Drop;
    n.capacity = static_cast<unsigned>(rng.bounded(4));
    if (n.policy == dagflow::TaskGraph::Overflow::Block) ++n.capacity;
    n.priority = rng.bounded(2) ? dagflow::Priority::High
                                : dagflow::Priority::Normal;
    n.lanes = static_cast<unsigned>(rng.bounded(5));
  }
  for (unsigned i = 0; i < spec.size; ++i)
    for (unsigned j = i + 1; j < spec.size; ++j)
      if (rng.bounded(4) == 0) spec.edges[i][j] = rng.bounded(4) == 0 ? 2 : 1;
  return spec;
}

// The model is recreated for each generated command to keep the graph's
// structure and observable execution independent of previous cases. Each
// operation still tests a nontrivial sequence: build -> run -> drain ->
// reset -> replay -> clear, or run -> externally cancel/fail -> drain.
class GraphReferenceMachine {
  enum Op : unsigned { SuccessfulDAG, FailRoot, CancelRoot, Count };
  dagflow::Pool pool_{[] {
    dagflow::Config config;
    config.threads = 4;
    config.shards = 2;
    return config;
  }()};
  unsigned histories_{};

  void successful(const Command& command) {
    const GraphSpec spec = generate_spec(command);
    const Oracle oracle(spec);
    std::array<std::atomic<unsigned>, max_nodes> counts{};
    std::atomic<bool> order_violation{false};
    dagflow::TaskGraph graph(pool_);
    for (unsigned i = 0; i < spec.size; ++i) {
      dagflow::TaskGraph::NodeOptions options;
      const auto& node = spec.nodes[i];
      options.capacity = node.capacity;
      options.overflow = node.policy;
      options.concurrency = static_cast<int>(node.lanes);
      options.priority = node.priority;
      options.affinity = (i % 3 == 0) ? std::optional<uint32_t>{i % 2} : std::nullopt;
      graph.emplace([&, i] {
        // Acquire all preceding writes before declaring a successor eligible.
        for (unsigned pred = 0; pred < spec.size; ++pred)
          if (oracle.ancestors[i][pred] &&
              counts[pred].load(std::memory_order_acquire) != oracle.expected[pred])
            order_violation.store(true, std::memory_order_relaxed);
        if (counts[i].fetch_add(1, std::memory_order_acq_rel) >= oracle.expected[i])
          order_violation.store(true, std::memory_order_relaxed);
      }, options);
      if (node.policy == dagflow::TaskGraph::Overflow::Block) mark(BlockTokens);
      if (node.policy == dagflow::TaskGraph::Overflow::Drop) {
        mark(DropTokens);
        if (node.capacity == 0) mark(EmptyDrop);
      }
      graph.set_tokens({i}, node.requested);
    }
    for (unsigned i = 0; i < spec.size; ++i)
      for (unsigned j = i + 1; j < spec.size; ++j)
        for (unsigned k = 0; k < spec.edges[i][j]; ++k)
          graph.add_edge({i}, {j});
    if (oracle.has_join) mark(FanIn);
    if (oracle.has_fork) mark(FanOut);
    require(graph.seal(), "reference-generated DAG is acyclic");

    std::vector<dagflow::Handle> historical;
    for (unsigned iteration = 0; iteration < 2; ++iteration) {
      for (auto& count : counts) count.store(0, std::memory_order_relaxed);
      order_violation.store(false, std::memory_order_relaxed);
      const auto completion = graph.run();
      pool_.wait_and_rethrow(completion);
      require(completion.ready(), "reference run is completed");
      std::array<unsigned, max_nodes> actual{};
      for (unsigned i = 0; i < spec.size; ++i)
        actual[i] = counts[i].load(std::memory_order_acquire);
      require(oracle.accepts(spec, actual, order_violation.load()),
              "graph matches independent token and predecessor specification");
      historical.push_back(completion);
      if (iteration == 0) {
        graph.reset();
        mark(Reuse);
      }
    }
    graph.clear();
    mark(Clear);
    require(graph.empty(), "clear empties graph definition");
    for (const auto& handle : historical) {
      require(handle.ready(), "historical completions survive graph clear");
      handle.rethrow_if_failed();
    }
  }

  void exceptional(bool cancel) {
    dagflow::TaskGraph graph(pool_);
    std::atomic<bool> entered{false}, release{false};
    std::atomic<unsigned> tail{0};
    struct ReleaseOnExit {
      std::atomic<bool>& release;
      ~ReleaseOnExit() { release.store(true, std::memory_order_release); release.notify_all(); }
    } guard{release};
    dagflow::TaskGraph::NodeOptions options;
    if (!cancel) {
      options.overflow = dagflow::TaskGraph::Overflow::Fail;
      options.capacity = 1;
      graph.emplace([&] { tail.fetch_add(100); }, options);
      graph.set_tokens({0}, 2);
      const auto done = graph.run();
      pool_.wait(done);
      bool failed = false;
      try { done.rethrow_if_failed(); } catch (const std::runtime_error&) { failed = true; }
      require(failed && graph.last_error(), "overflow fail propagates an error");
      require(tail.load() == 0, "failed initial admission runs no callback");
      mark(Failure);
      return;
    }
    const auto root = graph.emplace([&] {
      entered.store(true, std::memory_order_release);
      entered.notify_all();
      release.wait(false, std::memory_order_acquire);
    });
    auto successor = graph.emplace([&] { tail.fetch_add(1); });
    graph.add_edge(root, successor);
    const auto done = graph.run();
    entered.wait(false, std::memory_order_acquire);
    graph.cancel();
    release.store(true, std::memory_order_release);
    release.notify_all();
    pool_.wait(done);
    require(done.ready() && !graph.last_error(), "cancellation completes without failure");
    require(tail.load() == 0, "cancellation suppresses not-yet-started successor");
    mark(Cancellation);
  }

 public:
  Command next(Random& random) const {
    const auto op = static_cast<unsigned>(random.bounded(10));
    return {op < 8 ? SuccessfulDAG : (op == 8 ? FailRoot : CancelRoot),
            random.bounded(1 << 12), random.bounded(1 << 16)};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < (1u << 12) && c.b < (1u << 16);
  }
  void apply(const Command& c) {
    if (c.op == SuccessfulDAG) successful(c);
    else exceptional(c.op == CancelRoot);
    ++histories_;
  }
  void invariant() const { require(histories_ <= 10000, "bounded graph histories"); }
  void finish() { pool_.wait_idle(); }
};

void verify_mutation_sensitivity() {
  GraphSpec spec;
  spec.size = 3;
  for (auto& n : spec.nodes) {
    n.requested = 2;
    n.capacity = 2;
    n.policy = dagflow::TaskGraph::Overflow::Block;
  }
  spec.edges[0][1] = 1;
  spec.edges[1][2] = 1;
  Oracle oracle(spec);
  const auto correct = oracle.expected;
  require(oracle.accepts(spec, correct, false), "valid synthetic trace passes oracle");
  auto missing = correct;
  --missing[1];
  require(!oracle.accepts(spec, missing, false), "oracle detects dropped invocation");
  auto duplicated = correct;
  ++duplicated[2];
  require(!oracle.accepts(spec, duplicated, false), "oracle detects duplicate invocation");
  require(!oracle.accepts(spec, correct, true), "oracle detects premature successor");
  // This calibrates test assertions; it is NOT a production-runtime mutation score.
}
}

int main() {
  verify_mutation_sensitivity();
  run<GraphReferenceMachine>("reference graph: DAG tokens, dependencies, outcomes and reuse", 20, 20);
  for (unsigned category = 0; category < TransitionCount; ++category) {
    std::printf("transition %-15s %u\n", names[category],
                observed[category].load(std::memory_order_relaxed));
  }
}
