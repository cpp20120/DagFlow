#include <algorithm>
#include <atomic>
#include <cstdint>
#include <dagflow/detail/runtime_memory.hpp>
#include <dagflow/detail/small_vector.hpp>
#include <dagflow/graph_scope.hpp>
#include <dagflow/thread_pool.hpp>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

#include "property_support.hpp"

namespace {
using dagflow::test::property::Command;
using dagflow::test::property::Random;
using dagflow::test::property::require;
using dagflow::test::property::run;

// Raw callback admission, closure, worker-only rejection and external idle
// barriers use the same reference counts as the typed submission path.
// CPU pinning is deliberately best-effort; the property is progress, not an
// OS-specific affinity mask (containers may not grant the requested CPU).
class PoolBoundaryMachine {
  enum Op { Raw, RawFailure, WorkerIdle, Idle, Close, Reopen, Count };
  std::unique_ptr<dagflow::Pool> pool_;
  std::atomic<int> actual_{0};
  int expected_ = 0;
  bool closed_ = false;

  static dagflow::Config config() {
    dagflow::Config cfg;
    cfg.threads = 2;
    cfg.shards = 2;
    cfg.pin_threads = true;
    return cfg;
  }
  static void raw(void* context) {
    static_cast<std::atomic<int>*>(context)->fetch_add(1);
  }
  static void throwing(void*) { throw std::runtime_error("raw function failure"); }
  void reopen() {
    pool_.reset();
    pool_ = std::make_unique<dagflow::Pool>(config());
    closed_ = false;
  }

 public:
  PoolBoundaryMachine() { reopen(); }
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)), rng.bounded(8), 0};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 8 && c.b == 0;
  }
  void apply(const Command& c) {
    switch (c.op) {
      case Raw:
      case RawFailure: {
        bool rejected = false;
        try {
          auto result = c.op == Raw
                            ? pool_->submit(&raw, &actual_)
                            : pool_->submit(&throwing, nullptr);
          pool_->wait(result);
          bool failed = false;
          try {
            result.rethrow_if_failed();
          } catch (const std::runtime_error&) {
            failed = true;
          }
          require(failed == (c.op == RawFailure),
                  "raw callback exception is retained in its handle");
          if (c.op == Raw) ++expected_;
        } catch (const std::logic_error&) {
          rejected = true;
        }
        require(rejected == closed_, "closed raw admission has no side effects");
        break;
      }
      case WorkerIdle: {
        // A task cannot wait for its own physical epilogue. It must receive a
        // synchronous logic_error, without leaking an accounting credit.
        if (closed_) break;
        std::atomic<bool> rejected{false};
        auto handle = pool_->submit([this, &rejected] {
          try {
            pool_->wait_idle();
          } catch (const std::logic_error&) {
            rejected.store(true);
          }
        });
        pool_->wait_and_rethrow(handle);
        require(rejected.load(), "worker wait_idle diagnoses self-deadlock");
        break;
      }
      case Idle:
        pool_->wait_idle();
        break;
      case Close:
        pool_->close();
        closed_ = true;
        break;
      case Reopen:
        pool_->wait_idle();
        reopen();
        break;
    }
  }
  void invariant() const {
    require(pool_->closed() == closed_, "admission state matches model");
    require(actual_.load() == expected_,
            "every accepted raw callback executes exactly once");
  }
  void finish() { pool_->wait_idle(); }
};

// Capacity validation is transactional: rejected graph definitions cannot
// execute, and destroying a dirty scope must suppress only its own validation
// exception. Valid follow-up graphs are still reusable and execute normally.
class GraphBoundaryMachine {
  enum Op { InvalidSeal, Recover, InvalidScope, ValidScope, EmptyDrop, Count };
  dagflow::Pool pool_{[] { dagflow::Config cfg; cfg.threads = 1; return cfg; }()};
  unsigned expected_ = 0;
  std::atomic<unsigned> calls_{0};

  static dagflow::TaskGraph::NodeOptions invalid_options() {
    dagflow::TaskGraph::NodeOptions opt;
    opt.capacity = 0;
    opt.overflow = dagflow::TaskGraph::Overflow::Block;
    return opt;
  }
 public:
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)), rng.bounded(8), 0};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 8 && c.b == 0;
  }
  void apply(const Command& c) {
    switch (c.op) {
      case InvalidSeal:
      case Recover: {
        dagflow::TaskGraph graph(pool_);
        bool threw = false;
        try {
          graph.emplace([this] { ++calls_; }, invalid_options());
        } catch (const std::invalid_argument&) {
          threw = true;
        }
        require(threw, "Block with zero capacity is rejected before builder mutation");
        if (c.op == Recover) {
          require(graph.empty(), "failed emplace rolls back builder state");
          graph.emplace([this] { ++calls_; });
          require(graph.seal(), "a rejected node does not poison the builder");
          pool_.wait_and_rethrow(graph.run());
          ++expected_;
        }
        break;
      }
      case InvalidScope: {
        // A self-cycle is constructible, but run() diagnoses it. The dirty
        // scope runs during destruction and must contain that exception.
        auto guard = std::make_shared<int>(17);
        std::weak_ptr<int> lifetime = guard;
        {
          dagflow::GraphScope scope(pool_);
          scope.emplace([this, owner = std::move(guard)] { ++calls_; });
          scope.then(dagflow::JobHandle{1}, [this] { ++calls_; });
        }
        require(lifetime.expired(),
                "cyclic GraphScope destructor retires definition capture");
        break;
      }
      case ValidScope: {
        {
          dagflow::GraphScope scope(pool_);
          scope.emplace([this] { ++calls_; });
        }
        ++expected_;
        break;
      }
      case EmptyDrop: {
        dagflow::TaskGraph graph(pool_);
        dagflow::TaskGraph::NodeOptions opt;
        opt.capacity = 0;
        opt.overflow = dagflow::TaskGraph::Overflow::Drop;
        graph.emplace([this] { ++calls_; }, opt);
        pool_.wait_and_rethrow(graph.run());
        break;
      }
    }
  }
  void invariant() const {
    require(calls_.load() == expected_,
            "only valid admitted graph bodies may execute");
  }
  void finish() { pool_.wait_idle(); }
};

// Packet construction is a transaction: if moving the callable into a packet
// throws, packet storage is freed and the transferred credit retires without
// requiring an invocation. Exercises a real throwing constructor, not a fake
// result from a mocked allocation API.
class PacketConstructionMachine {
  enum Op { Successful, Failed, Count };
  struct Error {};
  struct Callable {
    std::shared_ptr<unsigned> calls;
    bool fail;
    Callable(std::shared_ptr<unsigned> owner, bool should_fail)
        : calls(std::move(owner)), fail(should_fail) {}
    Callable(const Callable&) = delete;
    Callable(Callable&& other) : calls(other.calls), fail(other.fail) {
      if (fail) throw Error{};
    }
    void operator()(dagflow::detail::CompletionCredit&) { ++*calls; }
  };
  unsigned attempted_ = 0;
 public:
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)), rng.bounded(8), 0};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 8 && c.b == 0;
  }
  void apply(const Command& c) {
    const bool fail = c.op == Failed;
    auto calls = std::make_shared<unsigned>(0);
    auto credit = dagflow::detail::CompletionCredit::create();
    auto observer = credit.handle();
    bool threw = false;
    try {
      auto packet = dagflow::detail::make_scheduled_task(
          Callable{calls, fail}, dagflow::Priority::Normal, credit.fork());
      require(!fail, "failing move cannot produce a packet");
      packet->ops->invoke(packet.get());
      require(*calls == 1, "packet calls body exactly once on success");
      packet.reset();
    } catch (const Error&) {
      threw = true;
    }
    require(threw == fail, "construction propagates the original move error");
    require(*calls == unsigned(!fail), "failed construction never invokes body");
    require(!observer.ready(), "live root credit still owns completion");
    // Release root separately, proving the failed constructor cannot retain
    // an extra credit or block an observing Handle.
    credit.finish();
    require(observer.ready(), "all packet/constructor credits are retired");
    ++attempted_;
  }
  void invariant() const {
    require(attempted_ <= 10000, "bounded trace");
    const std::string_view backend = dagflow::detail::allocator_backend();
    require(backend == "system" || backend == "mimalloc" ||
                backend == "tbbmalloc",
            "runtime allocator backend reports a recognized implementation");
  }
};

// Explicit const-at checks complement the mutable-index and capacity model.
class ConstVectorMachine {
  enum Op { Append, Remove, InBounds, OutOfBounds, Count };
  dagflow::small_vector<int, 4> actual_;
  std::vector<int> expected_;
 public:
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)), rng.bounded(16), 0};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 16 && c.b == 0;
  }
  void apply(const Command& c) {
    switch (c.op) {
      case Append:
        actual_.push_back(static_cast<int>(c.a));
        expected_.push_back(static_cast<int>(c.a));
        break;
      case Remove:
        if (!expected_.empty()) {
          actual_.pop_back();
          expected_.pop_back();
        }
        break;
      case InBounds:
        if (!expected_.empty()) {
          const auto& read_only = actual_;
          const auto index = c.a % expected_.size();
          require(actual_.at(index) == expected_[index],
                  "mutable indexed read observes the reference sequence");
          require(read_only.at(index) == expected_[index],
                  "const indexed read observes the reference sequence");
        }
        break;
      case OutOfBounds: {
        bool rejected = false;
        try {
          const auto& read_only = actual_;
          (void)read_only.at(expected_.size() + c.a);
        } catch (const std::out_of_range&) {
          rejected = true;
        }
        require(rejected, "const at rejects every index past logical size");
        break;
      }
    }
  }
  void invariant() const {
    const auto& read_only = actual_;
    require(read_only.size() == expected_.size(), "vector size agrees");
    for (std::size_t i = 0; i < expected_.size(); ++i) {
      require(actual_.at(i) == expected_[i],
              "mutable at agrees with reference model");
      require(read_only.at(i) == expected_[i],
              "const values agree across inline and heap storage");
    }
  }
};
}  // namespace

int main() {
  run<PoolBoundaryMachine>("raw admission, pinning and worker idle policy", 24, 48);
  run<GraphBoundaryMachine>("invalid graph construction and scope cleanup", 24, 48);
  run<PacketConstructionMachine>("packet construction rollback and credits", 24, 48);
  run<ConstVectorMachine>("const vector access and bounds transitions", 24, 48);
}
