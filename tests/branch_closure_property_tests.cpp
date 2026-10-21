#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <dagflow/graph_scope.hpp>
#include <dagflow/detail/small_vector.hpp>
#include <dagflow/task_graph.hpp>
#include <dagflow/thread_pool.hpp>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <vector>

#include "property_support.hpp"

namespace {
using namespace dagflow::test::property;

// A reference vector is deliberately independent of the implementation's
// chunking. Empty ranges, multi-chunk fixed ranges, one/many WS tiles and the
// lvalue/borrowed-range overloads must all produce the identical histogram.
class RangeBoundaryMachine {
  enum Op : unsigned { FixedIterator, FixedRange, SplitIterator, SplitRange, Count };
  static constexpr std::array<std::size_t, 8> lengths{
      0, 1, 2, 63, 64, 16384, 16385, 32769};
  dagflow::Pool pool_{[] { dagflow::Config c; c.threads = 4; c.shards = 2; return c; }()};
  unsigned histories_{};
 public:
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)), rng.bounded(lengths.size()), rng.bounded(8)};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < lengths.size() && c.b < 8;
  }
  void apply(const Command& c) {
    std::vector<unsigned> values(lengths[c.a]);
    std::atomic<unsigned> invocations{0};
    auto fn = [&](unsigned& value) {
      ++value;
      invocations.fetch_add(1, std::memory_order_relaxed);
    };
    dagflow::SubmitOptions opt{
        .affinity = (c.b & 1) ? std::optional<uint32_t>{3} : std::nullopt,
        .priority = (c.b & 2) ? dagflow::Priority::High : dagflow::Priority::Normal,
        .mode = (c.b & 4) ? dagflow::SubmissionMode::Enqueue
                          : dagflow::SubmissionMode::Spawn};
    dagflow::Handle done;
    if (c.op == FixedIterator)
      done = pool_.for_each(values.begin(), values.end(), fn, opt);
    else if (c.op == FixedRange)
      done = pool_.for_each(std::span{values}, fn, opt);
    else if (c.op == SplitIterator)
      done = pool_.for_each_ws(values.begin(), values.end(), fn, opt,
                               std::array<std::size_t, 4>{1, 16, 64, 16384}[c.b % 4]);
    else
      done = pool_.for_each_ws(std::span{values}, fn, opt,
                               std::array<std::size_t, 4>{1, 16, 64, 16384}[c.b % 4]);
    require(done.valid() == !values.empty(), "empty ranges do not allocate a completion");
    pool_.wait_and_rethrow(done);
    pool_.wait_idle();
    require(invocations.load() == values.size(), "each element invoked once");
    require(std::all_of(values.begin(), values.end(), [](unsigned v){return v == 1;}),
            "range partition has no overlap or gap");
    ++histories_;
  }
  void invariant() const { require(histories_ <= 20000, "bounded range history"); }
  void finish() { pool_.wait_idle(); }
};

class GraphScopeBoundaryMachine {
  enum Op : unsigned { AutoRun, ExplicitRun, Repeat, Clear, Count };
  static constexpr std::array<std::size_t, 7> lengths{0,1,63,64,16384,16385,32769};
  dagflow::Pool pool_{[] { dagflow::Config c; c.threads = 4; return c; }()};
  unsigned histories_{};
 public:
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)), rng.bounded(lengths.size()), rng.bounded(4)};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < lengths.size() && c.b < 4;
  }
  void apply(const Command& c) {
    std::vector<unsigned> values(lengths[c.a]);
    unsigned predecessor_calls=0;
    unsigned continuation_calls=0;
    unsigned times = c.op == Repeat ? 2 : 1;
    {
      dagflow::GraphScope scope(pool_);
      auto start = scope.emplace([&]{ ++predecessor_calls; });
      dagflow::JobHandle parallel;
      auto visitor = [](unsigned& value) { ++value; };
      if (c.b & 1)
        parallel = scope.parallel_for_after(start, std::span{values}, visitor);
      else
        parallel = scope.parallel_for_after(start, values.begin(), values.end(), visitor);
      auto finish = scope.when_all({parallel}, [&]{ ++continuation_calls; });
      require(start.valid() && parallel.valid() && finish.valid(), "graph handles are valid");
      if (c.op == AutoRun) {
        // Destructor must start an unstarted graph and drain all borrows.
      } else {
        auto first = scope.run();
        require(first.valid(), "nonempty graph produces a completion");
        require(scope.run().valid(), "repeated run before wait returns the same result");
        scope.wait();
        if (c.op == Repeat) scope.run_and_wait();
        if (c.op == Clear) {
          scope.clear();
          require(scope.empty(), "clear discards graph after drain");
          times = 1;
        }
      }
    }
    pool_.wait_idle();
    require(predecessor_calls == times && continuation_calls == times,
            "GraphScope startup, join and replay are exactly once per run");
    require(std::all_of(values.begin(), values.end(), [times](unsigned v){return v == times;}),
            "parallel_for chunking and reuse preserves every element");
    ++histories_;
  }
  void invariant() const { require(histories_ <= 20000, "bounded graphscope history"); }
  void finish() { pool_.wait_idle(); }
};

// While one node owns an incomplete token, topology changes and concurrent
// re-runs are rejected. No sleeps: the node is held by an explicit atomic gate
// until every rejection has been tested.
class RunningGraphMachine {
  enum Op : unsigned { NoCancel, Cancel, Throw, Count };
  dagflow::Pool pool_{[] { dagflow::Config c; c.threads=2; return c; }()};
  unsigned histories_{};
 public:
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)), rng.bounded(4), rng.bounded(2)};
  }
  bool precondition(const Command& c) const { return c.op < Count && c.a < 4 && c.b < 2; }
  void apply(const Command& c) {
    std::atomic<bool> arrived{false}, release{false};
    std::atomic<unsigned> successor{0};
    dagflow::TaskGraph graph(pool_);
    // Release the blocked graph even when a property assertion fails: its
    // destructor otherwise correctly waits forever for our unfinished task.
    struct GateRelease {
      std::atomic<bool>& gate;
      ~GateRelease() { gate.store(true, std::memory_order_release); gate.notify_all(); }
    } gate_release{release};
    dagflow::TaskGraph::NodeOptions opt;
    opt.priority = c.b ? dagflow::Priority::High : dagflow::Priority::Normal;
    opt.affinity = 1;
    auto root = graph.emplace([&] {
      arrived.store(true, std::memory_order_release);
      arrived.notify_all();
      release.wait(false, std::memory_order_acquire);
      if (c.op == Throw) throw std::runtime_error("graph failure");
    }, opt);
    auto tail = graph.emplace([&]{ successor.fetch_add(1); });
    graph.add_edge(root, tail);
    auto handle = graph.run();
    arrived.wait(false, std::memory_order_acquire);
    auto rejects = [&](auto&& fn) {
      bool threw = false;
      try { fn(); } catch (const std::logic_error&) { threw=true; }
      require(threw, "running graph rejects concurrent topology mutation or run");
    };
    rejects([&]{ (void)graph.run(); });
    rejects([&]{ graph.reset(); });
    rejects([&]{ graph.clear(); });
    require(!graph.seal(), "seal refuses to compile an active run");
    rejects([&]{ graph.add_edge(root, tail); });
    rejects([&]{ graph.set_tokens(root, 2); });
    if (c.op == Cancel) graph.cancel();
    release.store(true, std::memory_order_release);
    release.notify_all();
    pool_.wait(handle);
    bool error = false;
    try { handle.rethrow_if_failed(); } catch (const std::runtime_error&) { error=true; }
    require(error == (c.op == Throw), "graph errors propagate through result");
    require(successor.load() == (c.op == NoCancel ? 1u : 0u),
            "cancellation/failure suppresses successor propagation");
    // Reuse the completed graph, then exercise early cancellation again.
    if (c.op != NoCancel) {
      graph.reset();
      graph.cancel();
    }
    ++histories_;
  }
  void invariant() const { require(histories_ <= 20000, "bounded live graph history"); }
  void finish() { pool_.wait_idle(); }
};
// The nominal capacity limit is far beyond allocatable memory. Verify the
// public reserve rejection without attempting to allocate it or fabricating a
// huge live vector. Normal growth, move and clear must remain usable after it.
class VectorCapacityMachine {
  enum Op : unsigned { Append, Clear, Move, OversizeReserve, Count };
  dagflow::small_vector<int, 2> actual_;
  std::vector<int> model_;
  unsigned histories_{};
 public:
  Command next(Random& rng) const {
    const unsigned op = static_cast<unsigned>(rng.bounded(Count));
    return {model_.size() > 96 && op == Append ? Clear : op,
            rng.bounded(100), 0};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 100 && c.b == 0 &&
           (c.op != Append || model_.size() <= 96);
  }
  void apply(const Command& c) {
    if (c.op == Append) {
      actual_.push_back(static_cast<int>(c.a));
      model_.push_back(static_cast<int>(c.a));
    } else if (c.op == Clear) {
      actual_.clear();
      model_.clear();
    } else if (c.op == Move) {
      dagflow::small_vector<int, 2> moved(std::move(actual_));
      actual_ = std::move(moved);
    } else {
      bool rejected = false;
      try { actual_.reserve(actual_.max_size() + 1); }
      catch (const std::length_error&) { rejected = true; }
      require(rejected, "oversized capacity rejected before allocation");
    }
    ++histories_;
  }
  void invariant() const {
    require(actual_.size() == model_.size(), "vector size matches reference");
    for (std::size_t i = 0; i < model_.size(); ++i)
      require(actual_[i] == model_[i], "vector retains elements after rejection");
    require(histories_ <= 20000, "bounded capacity history");
  }
};
} // namespace

int main() {
  run<RangeBoundaryMachine>("branch closure: range overloads and partitions", 12, 12);
  run<GraphScopeBoundaryMachine>("branch closure: graphscope ranges and replay", 8, 12);
  run<RunningGraphMachine>("branch closure: active graph state transitions", 12, 12);
  run<VectorCapacityMachine>("branch closure: vector capacity and recovery", 20, 30);
}
