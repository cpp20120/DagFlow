#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <dagflow/task_graph.hpp>
#include <dagflow/thread_pool.hpp>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <vector>

#include "property_support.hpp"

namespace {
using namespace dagflow::test::property;

// A batch published from a worker is intentionally NOT put through the
// external 64-packet queue path. Children must remain visible to worker
// helping/stealing and their detached epilogues must be included in wait_idle.
class WorkerBatchMachine {
  enum Op : unsigned { Batch, NestedBatch, Count };
  static constexpr std::array<unsigned, 9> lengths{0, 1, 2, 63, 64, 65, 66, 127, 129};
  std::array<std::atomic<unsigned>, 129> actual_{};
  std::array<unsigned, 129> expected_{};
  dagflow::Pool pool_{[] {
    dagflow::Config cfg;
    cfg.threads = 2;
    cfg.shards = 2;
    return cfg;
  }()};

  void publish(unsigned n, bool nested) {
    // This function is always called by a worker of pool_.
    std::vector<std::function<void()>> jobs;
    jobs.reserve(n);
    for (unsigned i = 0; i < n; ++i) {
      jobs.emplace_back([this, i] {
        actual_[i].fetch_add(1, std::memory_order_relaxed);
      });
    }
    dagflow::SubmitOptions opt;
    opt.priority = nested ? dagflow::Priority::High : dagflow::Priority::Normal;
    opt.affinity = nested ? std::optional<uint32_t>{1} : std::nullopt;
    pool_.submit_batch_detached(std::span{jobs}, opt);
    if (nested) {
      // A single awaited descendant exercises cooperative helping even while
      // the detached siblings are still queued. It is not an idle barrier.
      auto child = pool_.submit([this] {
        auto grandchild = pool_.submit([] {});
        pool_.wait_and_rethrow(grandchild);
      });
      pool_.wait_and_rethrow(child);
    }
  }

 public:
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)),
            rng.bounded(lengths.size()), rng.bounded(2)};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < lengths.size() && c.b < 2;
  }
  void apply(const Command& c) {
    const auto n = lengths[c.a];
    const bool nested = c.op == NestedBatch;
    auto parent = pool_.submit([this, n, nested] { publish(n, nested); },
                               dagflow::SubmitOptions{
                                   .affinity = static_cast<uint32_t>(c.b),
                                   .priority = nested ? dagflow::Priority::High
                                                      : dagflow::Priority::Normal});
    pool_.wait_and_rethrow(parent);
    pool_.wait_idle();  // Detached siblings may outlive the parent's handle.
    for (unsigned i = 0; i < n; ++i) ++expected_[i];
  }
  void invariant() const {
    for (unsigned i = 0; i < actual_.size(); ++i)
      require(actual_[i].load(std::memory_order_relaxed) == expected_[i],
              "worker-side batch invokes every accepted child exactly once");
  }
  void finish() { pool_.wait_idle(); }
};

// Reference outcomes don't depend on how TaskGraph compiles its CSR, lanes,
// continuation budget, affinities or priority queues. A predecessor must
// complete all of its tokens before the successor becomes eligible.
class GraphContinuationMachine {
  enum Op : unsigned { Success, Drop, Fail, Cancel, Count };
  static constexpr std::array<unsigned, 7> tokens{1, 2, 63, 64, 65, 127, 129};
  dagflow::Pool pool_{[] { dagflow::Config cfg; cfg.threads = 4; return cfg; }()};
  unsigned runs_{};

 public:
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)),
            rng.bounded(tokens.size()), rng.bounded(8)};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < tokens.size() && c.b < 8;
  }
  void apply(const Command& c) {
    const unsigned n = tokens[c.a];
    std::atomic<unsigned> prefix{0}, successor{0};
    std::atomic<bool> premature{false};
    dagflow::TaskGraph graph(pool_);
    dagflow::TaskGraph::NodeOptions driver;
    driver.concurrency = static_cast<int>(1 + (c.b % 4));
    driver.priority = (c.b & 1) ? dagflow::Priority::High : dagflow::Priority::Normal;
    driver.affinity = static_cast<uint32_t>(c.b % 4);
    driver.capacity = (c.op == Fail || c.op == Drop) ? n - 1 : n;
    driver.overflow = c.op == Fail ? dagflow::TaskGraph::Overflow::Fail
                      : c.op == Drop ? dagflow::TaskGraph::Overflow::Drop
                                     : dagflow::TaskGraph::Overflow::Block;
    const auto root = graph.emplace([&] {
      prefix.fetch_add(1, std::memory_order_relaxed);
    }, driver);
    graph.set_tokens(root, n);
    dagflow::TaskGraph::NodeOptions continuation;
    continuation.priority = (c.b & 2) ? dagflow::Priority::High
                                      : dagflow::Priority::Normal;
    continuation.affinity = static_cast<uint32_t>((c.b + ((c.b & 4) ? 0 : 1)) % 4);
    const auto tail = graph.emplace([&] {
      const auto expected = c.op == Drop ? n - 1 : n;
      if (prefix.load(std::memory_order_relaxed) != expected)
        premature.store(true, std::memory_order_relaxed);
      successor.fetch_add(1, std::memory_order_relaxed);
    }, continuation);
    graph.add_edge(root, tail);
    require(graph.seal(), "acyclic two-stage graph seals");

    // Cancel is injected synchronously from a token, at a known boundary.
    // Other concurrently executing lanes can already be running; therefore
    // cancellation guarantees a range, not a specific count of completions.
    if (c.op == Cancel) {
      graph.clear();
      dagflow::TaskGraph::NodeOptions cancellable = driver;
      cancellable.capacity = n;
      cancellable.concurrency = 1;
      const auto a = graph.emplace([&] {
        auto done = prefix.fetch_add(1, std::memory_order_relaxed) + 1;
        if (done == std::max(1u, n / 2)) graph.cancel();
      }, cancellable);
      graph.set_tokens(a, n);
      const auto b = graph.emplace([&] { successor.fetch_add(1); }, continuation);
      graph.add_edge(a, b);
    }

    for (unsigned repeat = 0; repeat < 2; ++repeat) {
      prefix.store(0);
      successor.store(0);
      premature.store(false);
      const auto result = graph.run();
      pool_.wait(result);
      require(result.ready(), "graph run completes before reuse");
      bool failed = false;
      try {
        result.rethrow_if_failed();
      } catch (const std::runtime_error&) {
        failed = true;
      }
      require(failed == (c.op == Fail), "Fail overflow retains an error");
      const auto observed = prefix.load();
      if (c.op == Cancel) {
        require(observed >= std::max(1u, n / 2) && observed <= n,
                "cancellation prevents further unstarted tokens");
        require(successor.load() == 0, "cancelled graph skips dependencies");
      } else if (c.op == Fail) {
        require(observed == 0 && successor.load() == 0,
                "Fail overflow does not execute rejected root or successor");
      } else {
        require(observed == (c.op == Drop ? n - 1 : n),
                "Drop/Block executes the exact admitted token count");
        require(successor.load() == 1 && !premature.load(),
                "successor executes once after all predecessor tokens");
      }
    }
    ++runs_;
  }
  void invariant() const { require(runs_ <= 10000, "bounded graph trace"); }
  void finish() { pool_.wait_idle(); }
};


// A worker may synchronously await range work that is exclusively runnable
// by its own pool. Test both fixed chunks and recursive work stealing, and
// retain borrowed range storage until the awaited handle has completed.
class WorkerRangeMachine {
  enum Op : unsigned { Fixed, Split, Count };
  static constexpr std::array<unsigned, 8> lengths{
      0, 1, 63, 64, 65, 16384, 16385, 32769};
  dagflow::Pool pool_{[] {
    dagflow::Config cfg;
    cfg.threads = 2;
    return cfg;
  }()};
  unsigned observed_{};

 public:
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)),
            rng.bounded(lengths.size()), rng.bounded(3)};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < lengths.size() && c.b < 3;
  }
  void apply(const Command& c) {
    const auto n = lengths[c.a];
    std::vector<unsigned> values(n, 0);
    auto guard = std::make_shared<int>(7);
    std::weak_ptr<int> observer = guard;
    auto outer = pool_.submit([this, &values, c, guard = std::move(guard)] {
      auto work = [guard](unsigned& value) { ++value; };
      dagflow::Handle handle;
      if (c.op == Fixed)
        handle = pool_.for_each(std::span{values}, std::move(work));
      else
        handle = pool_.for_each_ws(std::span{values}, std::move(work), {},
                                   std::array<std::size_t, 3>{16, 64, 4096}[c.b]);
      require(handle.valid() == !values.empty(),
              "zero-length worker range has no completion state");
      if (handle.valid()) pool_.wait_and_rethrow(handle);
    });
    pool_.wait_and_rethrow(outer);
    pool_.wait_idle();
    require(observer.expired(), "range captures retire with their completion");
    for (auto value : values)
      require(value == 1, "every element is visited exactly once by a worker range");
    ++observed_;
  }
  void invariant() const { require(observed_ <= 10000, "bounded range history"); }
  void finish() { pool_.wait_idle(); }
};

// Chained continuations cross the cooperative 64-step budget, while a fanout
// with a three-way join exercises mixed priority and affinity re-publication.
// The independent oracle checks partial ordering and cardinality only.
class GraphRoutingMachine {
  enum Op : unsigned { Chain, Diamond, Count };
  static constexpr std::array<unsigned, 6> depths{1, 2, 63, 64, 65, 129};
  dagflow::Pool pool_{[] { dagflow::Config cfg; cfg.threads = 4; return cfg; }()};
  unsigned histories_{};

 public:
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)),
            rng.bounded(depths.size()), rng.bounded(8)};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < depths.size() && c.b < 8;
  }
  void apply(const Command& c) {
    dagflow::TaskGraph graph(pool_);
    const unsigned count = c.op == Chain ? depths[c.a] : 5;
    auto calls = std::make_unique<std::atomic<unsigned>[]>(count);
    std::atomic<bool> order_violated{false};
    std::vector<dagflow::TaskGraph::NodeId> nodes;
    nodes.reserve(count);
    for (unsigned i = 0; i < count; ++i) {
      dagflow::TaskGraph::NodeOptions opt;
      opt.priority = (c.b & 1) && (i & 1) ? dagflow::Priority::High
                                           : dagflow::Priority::Normal;
      if (c.b & 2) opt.affinity = static_cast<std::uint32_t>(i % 4);
      nodes.push_back(graph.emplace([&, i] {
        // For the diamond, node 4 depends on every middle node.
        if (i > 0 && (c.op == Chain || i == 4)) {
          if (c.op == Chain) {
            if (calls[i - 1].load(std::memory_order_relaxed) == 0)
              order_violated.store(true, std::memory_order_relaxed);
          } else {
            for (unsigned predecessor = 1; predecessor <= 3; ++predecessor)
              if (calls[predecessor].load(std::memory_order_relaxed) == 0)
                order_violated.store(true, std::memory_order_relaxed);
          }
        }
        calls[i].fetch_add(1, std::memory_order_relaxed);
      }, opt));
    }
    if (c.op == Chain) {
      for (unsigned i = 1; i < count; ++i) graph.add_edge(nodes[i - 1], nodes[i]);
    } else {
      for (unsigned i = 1; i <= 3; ++i) {
        graph.add_edge(nodes[0], nodes[i]);
        graph.add_edge(nodes[i], nodes[4]);
      }
    }
    require(graph.seal(), "generated chain/diamond is acyclic");
    for (unsigned replay = 0; replay < 2; ++replay) {
      for (unsigned i = 0; i < count; ++i) calls[i].store(0);
      order_violated.store(false);
      auto result = graph.run();
      pool_.wait_and_rethrow(result);
      require(!order_violated.load(), "continuations preserve predecessor joins");
      for (unsigned i = 0; i < count; ++i)
        require(calls[i].load() == 1, "each chained/joined node runs exactly once");
    }
    ++histories_;
  }
  void invariant() const { require(histories_ <= 10000, "bounded routing history"); }
  void finish() { pool_.wait_idle(); }
};

} // namespace

int main() {
  run<WorkerBatchMachine>("worker batch and recursive helping", 16, 24);
  run<GraphContinuationMachine>("graph overflow and continuations", 16, 20);
  run<WorkerRangeMachine>("worker-side range helping and lifetime", 12, 16);
  run<GraphRoutingMachine>("graph routing, budgets and joins", 12, 16);
}
