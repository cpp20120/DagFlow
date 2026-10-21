#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <dagflow/graph_scope.hpp>
#include <dagflow/task_scope.hpp>
#include <dagflow/thread_pool.hpp>
#include <forward_list>
#include <memory>
#include <span>
#include <vector>

#include "property_support.hpp"

namespace {
using namespace dagflow::test::property;

// A single occupied worker makes the pending-task cancellation boundary
// deterministic: no queued callback can pass TaskScope::invoke before cancel.
// A close (without cancellation) must instead drain precisely that queue.
class QueuedScopeMachine {
  enum Op : unsigned { CancelPending, ClosePending, Count };
  dagflow::Pool pool_{[] { dagflow::Config c; c.threads = 1; return c; }()};
  std::uint64_t expected_{0};
  std::atomic<std::uint64_t> invoked_{0};
 public:
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)), rng.bounded(5),
            rng.bounded(2)};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 5 && c.b < 2;
  }
  void apply(const Command& c) {
    static constexpr std::array<unsigned, 5> sizes{0, 1, 2, 17, 65};
    const auto count = sizes[c.a];
    std::atomic<bool> arrived{false};
    std::atomic<bool> release{false};
    dagflow::TaskScope scope(pool_);
    struct GateGuard {
      std::atomic<bool>& flag;
      ~GateGuard() {
        flag.store(true, std::memory_order_release);
        flag.notify_all();
      }
    } guard{release};  // Destroyed BEFORE scope, even on property failure.

    require(scope.spawn([&] {
              arrived.store(true, std::memory_order_release);
              arrived.notify_all();
              release.wait(false, std::memory_order_acquire);
              invoked_.fetch_add(1, std::memory_order_relaxed);
            }), "initial scope task admitted");
    arrived.wait(false, std::memory_order_acquire);
    std::vector<std::weak_ptr<int>> captures;
    for (unsigned i = 0; i < count; ++i) {
      auto lifetime = std::make_shared<int>(static_cast<int>(i));
      captures.push_back(lifetime);
      dagflow::SubmitOptions options;
      options.mode = c.b ? dagflow::SubmissionMode::Enqueue
                         : dagflow::SubmissionMode::Spawn;
      options.priority = (i & 1) ? dagflow::Priority::High
                                 : dagflow::Priority::Normal;
      require(scope.spawn([this, lifetime] {
                invoked_.fetch_add(1, std::memory_order_relaxed);
              }, options), "pending scope work admitted before close/cancel");
    }
    const auto completion = scope.completion();
    require(completion.valid() && !completion.ready(),
            "scope completion remains pending with held root");
    if (c.op == CancelPending) {
      scope.cancel();
      require(scope.cancelled(), "cancellation published to scope state");
    } else {
      scope.close();
      require(!scope.cancelled(), "close is not cancellation");
    }
    require(!scope.spawn([] {}), "closed/cancelled external admission rejects work");
    release.store(true, std::memory_order_release);
    release.notify_all();
    scope.join();
    expected_ += 1 + (c.op == ClosePending ? count : 0);
    require(completion.ready(), "scope completion closes after all epilogues");
    require(scope.last_error() == nullptr, "normal cancellation has no task error");
    for (const auto& capture : captures)
      require(capture.expired(), "queued capture retired despite cancellation");
    require(invoked_.load(std::memory_order_relaxed) == expected_,
            "close executes all accepted work; cancel suppresses queued calls");
  }
  void invariant() const {
    require(invoked_.load(std::memory_order_relaxed) == expected_,
            "scope model execution accounting");
  }
  void finish() { pool_.wait_idle(); }
};

// Test all public range variants against the same reference model. Empty
// ranges must still produce a valid GraphScope node; nonempty ones must visit
// every element, including the remainder after the 16K chunk boundary.
class GraphScopeRangesMachine {
  enum Op : unsigned { Iterator, BorrowedSpan, AfterIterator, AfterSpan, Count };
  static constexpr std::array<std::size_t, 7> sizes{
      0, 1, 63, 64, 16384, 16385, 32769};
  dagflow::Pool pool_{[] { dagflow::Config c; c.threads = 3; c.shards = 2; return c; }()};
  unsigned histories_{};
 public:
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)),
            rng.bounded(sizes.size()), rng.bounded(4)};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < sizes.size() && c.b < 4;
  }
  void apply(const Command& c) {
    std::vector<unsigned> values(sizes[c.a], 0);
    std::atomic<unsigned> starts{0};
    struct Visitor {
      void operator()(unsigned& value) const { ++value; }
    };
    {
      dagflow::GraphScope scope(pool_);
      dagflow::JobHandle parent;
      const bool has_parent = c.op >= AfterIterator;
      if (has_parent)
        parent = scope.emplace([&] { starts.fetch_add(1); });
      dagflow::JobHandle parallel;
      const bool borrowed = c.op == BorrowedSpan || c.op == AfterSpan;
      if (has_parent) {
        if (borrowed)
          parallel = scope.parallel_for_after(parent, std::span{values}, Visitor{});
        else
          parallel = scope.parallel_for_after(parent, values.begin(), values.end(),
                                              Visitor{});
      } else if (borrowed) {
        parallel = scope.parallel_for(std::span{values}, Visitor{});
      } else {
        parallel = scope.parallel_for(values.begin(), values.end(), Visitor{});
      }
      require(parallel.valid(), "empty and nonempty ranges yield a graph node");
      if (c.b == 0) {
        // The destructor runs an unstarted graph and drains retained borrows.
      } else if (c.b == 1) {
        scope.run_and_wait();
      } else if (c.b == 2) {
        auto handle = scope.run();
        require(handle.valid(), "explicit graph execution returns completion");
        scope.wait();
      } else {
        scope.clear();
        require(scope.empty(), "clear discards unstarted graph");
      }
    }
    // Exercise the no-op destructor separately (no dirty graph at all).
    { dagflow::GraphScope untouched(pool_); }
    pool_.wait_idle();
    const auto runs = c.b == 3 ? 0u : 1u;
    require(starts.load() == (c.op >= AfterIterator ? runs : 0u),
            "predecessor runs exactly once for executed graph");
    require(std::all_of(values.begin(), values.end(),
                        [runs](unsigned value) { return value == runs; }),
            "graph range partitions are disjoint and collectively complete");
    ++histories_;
  }
  void invariant() const {
    require(histories_ <= 20000, "bounded graph range history");
  }
  void finish() { pool_.wait_idle(); }
};

// Non-random access forward iterators exercise the independent for_each
// chunk-advance logic; a zero-size range must not create a completion.
class ForwardRangeMachine {
  enum Op : unsigned { Iterator, LvalueRange, Count };
  static constexpr std::array<std::size_t, 5> sizes{0, 1, 64, 16385, 32769};
  dagflow::Pool pool_{[] { dagflow::Config c; c.threads = 2; return c; }()};
  unsigned histories_{};
 public:
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)), rng.bounded(sizes.size()),
            rng.bounded(2)};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < sizes.size() && c.b < 2;
  }
  void apply(const Command& c) {
    std::forward_list<unsigned> values(sizes[c.a], 0);
    std::atomic<unsigned> calls{0};
    auto visit = [&](unsigned& value) {
      ++value;
      calls.fetch_add(1, std::memory_order_relaxed);
    };
    dagflow::SubmitOptions options;
    options.mode = c.b ? dagflow::SubmissionMode::Enqueue
                       : dagflow::SubmissionMode::Spawn;
    auto done = c.op == Iterator
        ? pool_.for_each(values.begin(), values.end(), visit, options)
        : pool_.for_each(values, visit, options);
    require(done.valid() == (sizes[c.a] != 0),
            "empty forward range has no completion");
    pool_.wait_and_rethrow(done);
    require(calls.load() == sizes[c.a], "forward chunks execute each item once");
    for (auto value : values)
      require(value == 1, "forward chunks neither skip nor overlap items");
    ++histories_;
  }
  void invariant() const {
    require(histories_ <= 20000, "bounded forward-range history");
  }
  void finish() { pool_.wait_idle(); }
};
} // namespace

int main() {
  run<QueuedScopeMachine>("queued scope cancellation/close custody", 12, 16);
  run<GraphScopeRangesMachine>("graph scope empty/chunked ranges", 12, 18);
  run<ForwardRangeMachine>("forward iterator partitioning", 12, 18);
}
