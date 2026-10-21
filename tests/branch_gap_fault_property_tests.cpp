#include <atomic>
#include <cstddef>
#include <cstdint>
#include <dagflow/detail/fuzz_points.hpp>
#include <dagflow/detail/runtime_memory.hpp>
#include <dagflow/detail/test_faults.hpp>
#include <dagflow/task_graph.hpp>
#include <dagflow/thread_pool.hpp>
#include <functional>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <stdexcept>
#include <vector>

#include "property_support.hpp"

namespace {
using namespace dagflow::test::property;
namespace faults = dagflow::detail::test_faults;
namespace fuzz = dagflow::detail::fuzz_points;

template<class Error, class F>
bool throws(F&& f) {
  try { f(); } catch (const Error&) { return true; }
  return false;
}

// Keep both the runtime's thread-local fault state and the header-side budget
// scoped to ONE history command. Shrink/replay never depends on prior failures.
struct FaultReset {
  ~FaultReset() {
    faults::reset();
    fuzz::allocation_budget = -1;
  }
};

// Observe backpressure as a state transition, not as an arbitrary pause:
// simulated queue-full does not take custody; accepted packets execute once;
// after a retry, idle accounting and subsequent healthy publications recover.
class ExternalRetryMachine {
  enum Op : unsigned { Single, Batch, EmptyBatch, Count };
  dagflow::Pool pool_{[]{dagflow::Config c; c.threads=2; c.shards=2; return c;}()};
  std::atomic<std::uint64_t> executed_{0};
  std::uint64_t expected_{};
 public:
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)), rng.bounded(7), rng.bounded(8)};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 7 && c.b < 8;
  }
  void apply(const Command& c) {
    FaultReset guard;
    const unsigned failures = static_cast<unsigned>(c.a);
    dagflow::SubmitOptions opt;
    opt.priority = c.b & 1 ? dagflow::Priority::High : dagflow::Priority::Normal;
    if (c.b & 2) opt.affinity = 1;
    if (c.op == Single) {
      faults::reject_external_ingress = failures;
      auto handle = pool_.submit([this] { executed_.fetch_add(1); }, opt);
      pool_.wait_and_rethrow(handle);
      ++expected_;
      require(faults::reject_external_ingress == 0,
              "single submission retries every queue rejection");
    } else if (c.op == Batch) {
      const std::size_t count = c.b & 4 ? 65 : static_cast<std::size_t>(c.b + 1);
      std::vector<std::function<void()>> tasks;
      for (std::size_t i = 0; i < count; ++i)
        tasks.emplace_back([this] { executed_.fetch_add(1); });
      faults::reject_external_ingress = failures;
      pool_.submit_batch_detached(std::span{tasks}, opt);
      pool_.wait_idle();
      expected_ += count;
      require(faults::reject_external_ingress == 0,
              "batch submission retries rejected ingress packets");
    } else {
      std::vector<std::function<void()>> empty;
      faults::reject_external_ingress = failures;
      pool_.submit_batch_detached(std::span{empty}, opt);
      require(faults::reject_external_ingress == failures,
              "empty batch does not touch ingress");
    }
    // A successful follow-up publish must not inherit the retry injection.
    faults::reject_external_ingress = 0;
    auto follow_up = pool_.submit([this]{executed_.fetch_add(1);});
    pool_.wait_and_rethrow(follow_up);
    ++expected_;
    pool_.wait_idle();
  }
  void invariant() const {
    require(executed_.load() == expected_,
            "retry/recovery preserves exactly-once execution and idle barrier");
  }
  void finish() { pool_.wait_idle(); }
};

struct Counted {
  static inline int alive = 0;
  Counted() { ++alive; }
  ~Counted() { --alive; }
};

// Every failure is observed at the real allocation boundary. No OOM or
// impossible allocation is attempted: the large-array guard is checked before
// the backend, while allocation_budget rejects a particular allocation.
class AllocationFailureMachine {
  enum Op : unsigned { FailObject, FailArray, DelayFailure, Oversized,
                       NullDeleter, Healthy, Count };
  unsigned histories_{};
 public:
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)), rng.bounded(5), 0};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 5 && c.b == 0;
  }
  void apply(const Command& c) {
    FaultReset guard;
    require(Counted::alive == 0, "no object ownership across trace commands");
    switch (c.op) {
      case FailObject:
        fuzz::allocation_budget = 0;
        require(throws<std::bad_alloc>([]{
                  (void)dagflow::detail::make_owned<Counted>();
                }), "make_owned surfaces injected allocation failure");
        require(fuzz::allocation_budget == -1,
                "one-shot OOM fault is consumed");
        break;
      case FailArray:
        fuzz::allocation_budget = 0;
        require(throws<std::bad_alloc>([]{
                  (void)dagflow::detail::make_owned_array<Counted>(3);
                }), "array OOM does not construct partial elements");
        break;
      case DelayFailure: {
        fuzz::allocation_budget = static_cast<int>(c.a + 1);
        for (std::size_t i = 0; i < c.a + 1; ++i) {
          auto owner = dagflow::detail::make_owned<Counted>();
          require(owner != nullptr, "remaining budget permits allocation");
        }
        require(throws<std::bad_alloc>([]{
                  (void)dagflow::detail::make_owned<Counted>();
                }), "failure occurs at exact budget boundary");
        break;
      }
      case Oversized: {
        const auto count = static_cast<std::size_t>(
            std::numeric_limits<std::ptrdiff_t>::max()) / sizeof(Counted) + 1;
        require(throws<std::length_error>([&]{
                  (void)dagflow::detail::make_owned_array<Counted>(count);
                }), "oversized array rejected before allocator and constructors");
        break;
      }
      case NullDeleter: {
        dagflow::detail::ObjectDeleter<Counted>{}(nullptr);
        dagflow::detail::ArrayDeleter<Counted>{3}(nullptr);
        dagflow::detail::deallocate_bytes(nullptr, alignof(Counted));
        auto* empty = dagflow::detail::allocate_bytes(0, alignof(Counted));
        require(empty != nullptr, "zero-size allocation is a valid owned request");
        dagflow::detail::deallocate_bytes(empty, alignof(Counted));
        struct alignas(64) Large { char bytes[64]; };
        dagflow::detail::RuntimeAllocator<Large> allocator;
        require(throws<std::bad_array_new_length>([&] {
                  (void)allocator.allocate(std::numeric_limits<std::size_t>::max());
                }), "STL allocation count overflow is rejected");
        break;
      }
      case Healthy: {
        auto object = dagflow::detail::make_owned<Counted>();
        auto array = dagflow::detail::make_owned_array<Counted>(c.a + 1);
        require(object && array && Counted::alive == static_cast<int>(c.a + 2),
                "ordinary ownership works after injected errors");
        break;
      }
    }
    require(Counted::alive == 0, "every successful allocation is retired");
    ++histories_;
  }
  void invariant() const {
    require(Counted::alive == 0, "failure has no leaked constructed objects");
    require(histories_ <= 20000, "bounded stateful allocation history");
  }
};

// A failed seal is transactional. Invalid priority/overflow are rejected at
// compile, zero-token requests normalize to one, and cancelling without an
// active run must not poison the graph's subsequent execution.
class GraphValidationMachine {
  enum Op : unsigned { BadPriority, BadOverflow, BadCapacity, CancelIdle, ZeroTokens, Count };
  dagflow::Pool pool_{[]{dagflow::Config c; c.threads=0; return c;}()};
  std::atomic<unsigned> executed_{0};
  unsigned expected_{};
 public:
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)), rng.bounded(8), 0};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 8 && c.b == 0;
  }
  void apply(const Command& c) {
    FaultReset guard;
    dagflow::TaskGraph graph(pool_);
    dagflow::TaskGraph::NodeOptions opt;
    if (c.op == BadPriority) opt.priority = static_cast<dagflow::Priority>(255);
    if (c.op == BadOverflow) opt.overflow = static_cast<dagflow::TaskGraph::Overflow>(255);
    if (c.op == BadCapacity) {
      opt.capacity = 0;
      opt.overflow = dagflow::TaskGraph::Overflow::Block;
      require(throws<std::invalid_argument>([&] {
                (void)graph.emplace([]{}, opt);
              }), "invalid Block capacity rejected at builder entry");
      opt.capacity = 1;
    }
    const auto root = graph.emplace([this]{ executed_.fetch_add(1); }, opt);
    if (c.op == CancelIdle) graph.cancel();
    if (c.op == ZeroTokens) graph.set_tokens(root, 0);
    if (c.op == BadPriority || c.op == BadOverflow) {
      require(throws<std::invalid_argument>([&]{(void)graph.seal();}),
              "invalid graph options fail during compilation");
      graph.clear();
      graph.emplace([this]{ executed_.fetch_add(1); });
    }
    require(graph.seal(), "valid graph compiles after failed seal");
    pool_.wait_and_rethrow(graph.run());
    ++expected_;
  }
  void invariant() const {
    require(pool_.thread_count() == 1, "zero worker configuration normalizes to one");
    require(executed_.load() == expected_, "graph executes exactly one valid node");
  }
  void finish() { pool_.wait_idle(); }
};
} // namespace

int main() {
  run<ExternalRetryMachine>("external backpressure retries and recovery", 14, 36);
  run<AllocationFailureMachine>("allocation budgets and recovery", 16, 40);
  run<GraphValidationMachine>("graph validation and idle cancellation", 12, 28);
}
