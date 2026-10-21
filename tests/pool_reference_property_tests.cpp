#include <algorithm>
#include <array>
#include <atomic>
#include <dagflow/dagflow.hpp>
#include <memory>
#include <stdexcept>
#include <vector>
#include <cstdio>

#include "property_support.hpp"

namespace {
using namespace dagflow::test::property;

enum Transition : unsigned {
  Accepted, FailedTask, Detached, FanIn, WaitOne, WaitIdle, Rejected,
  WorkerJoin, Recreated, Count
};
constexpr std::array<const char*, Count> categories{
    "accepted", "failed task", "detached", "fan-in", "wait-one",
    "idle drain", "closed reject", "worker join", "recreate"};
std::array<std::atomic<unsigned>, Count> coverage{};
void tick(Transition t) { coverage[t].fetch_add(1, std::memory_order_relaxed); }

// Model only observable effects and completion obligations, not scheduler
// occupancy or actual execution order. This deliberately permits all legal
// worker schedules while ruling out drops, duplication, leaked completion and
// wrong failure propagation.
class PoolReferenceMachine {
  enum Op : unsigned {
    Submit, Throw, SubmitDetached, Combine, Await, Drain, Close, Nested, Recreate,
    TotalOps
  };
  struct Obligation {
    std::shared_ptr<std::atomic<unsigned>> calls;
    dagflow::Handle completion;
    bool failure{};
    bool has_handle{};
  };
  std::unique_ptr<dagflow::Pool> pool_ = new_pool();
  std::vector<Obligation> obligations_;
  std::vector<dagflow::Handle> historical_;
  std::vector<dagflow::Handle> joins_;
  bool closed_{};
  unsigned histories_{};

  static std::unique_ptr<dagflow::Pool> new_pool() {
    dagflow::Config cfg;
    cfg.threads = 2;
    cfg.shards = 2;
    return std::make_unique<dagflow::Pool>(cfg);
  }
  static auto fresh_counter() { return std::make_shared<std::atomic<unsigned>>(0); }
  static void verify_handle(const Obligation& o) {
    if (!o.has_handle) return;
    require(o.completion.ready(), "drained accepted handle is ready");
    bool failed{};
    try { o.completion.rethrow_if_failed(); }
    catch (const std::runtime_error&) { failed = true; }
    require(failed == o.failure, "completion preserves originating task error");
  }
  void verify_drained() {
    pool_->wait_idle();
    for (const auto& o : obligations_) {
      require(o.calls->load(std::memory_order_acquire) == 1,
              "each accepted effect happens exactly once");
      verify_handle(o);
    }
    for (const auto& handle : joins_)
      require(handle.ready(), "fan-in waits for every input completion");
  }
  void submit(const Command& c, Op operation) {
    const bool failure = operation == Throw;
    auto counter = fresh_counter();
    dagflow::SubmitOptions opt;
    opt.priority = c.a & 1 ? dagflow::Priority::High : dagflow::Priority::Normal;
    opt.affinity = static_cast<std::uint32_t>(c.a % 2);
    opt.mode = c.a & 2 ? dagflow::SubmissionMode::Enqueue : dagflow::SubmissionMode::Spawn;
    bool rejected{};
    dagflow::Handle handle;
    try {
      if (operation == SubmitDetached) {
        pool_->submit_detached([counter] { counter->fetch_add(1); }, opt);
      } else {
        handle = pool_->submit([counter, failure] {
          counter->fetch_add(1, std::memory_order_release);
          if (failure) throw std::runtime_error("model task failure");
        }, opt);
      }
    } catch (const std::logic_error&) { rejected = true; }
    require(rejected == closed_, "external admission matches close state");
    if (rejected) { tick(Rejected); return; }
    tick(operation == Throw ? FailedTask : (operation == SubmitDetached ? Detached : Accepted));
    obligations_.push_back({counter, handle, failure, operation != SubmitDetached});
  }
  void nested(const Command& c) {
    auto parent = fresh_counter(), child = fresh_counter();
    bool rejected{};
    dagflow::Handle result;
    try {
      dagflow::SubmitOptions options;
      options.priority = c.a & 1 ? dagflow::Priority::High : dagflow::Priority::Normal;
      result = pool_->submit([executor = pool_.get(), parent, child] {
        auto subtask = executor->submit([child] {
          child->fetch_add(1, std::memory_order_release);
        });
        executor->wait_and_rethrow(subtask);
        require(child->load(std::memory_order_acquire) == 1,
                "parent resumes only after child completion");
        parent->fetch_add(1, std::memory_order_release);
      }, options);
    } catch (const std::logic_error&) { rejected = true; }
    require(rejected == closed_, "nested parent still needs external admission");
    if (rejected) { tick(Rejected); return; }
    obligations_.push_back({parent, result, false, true});
    obligations_.push_back({child, {}, false, false});
    tick(WorkerJoin);
  }
  void rebuild() {
    verify_drained();
    for (const auto& obligation : obligations_)
      if (obligation.has_handle) historical_.push_back(obligation.completion);
    for (const auto& join : joins_) historical_.push_back(join);
    pool_.reset();
    pool_ = new_pool();
    obligations_.clear();
    joins_.clear();
    closed_ = false;
    tick(Recreated);
  }

 public:
  Command next(Random& rng) const {
    // Periodically bound the reference history, not just the submission count.
    for (;;) {
      const auto op = obligations_.size() > 50 ? Recreate
                                               : static_cast<Op>(rng.bounded(TotalOps));
      Command command{static_cast<unsigned>(op), rng.bounded(8), rng.bounded(128)};
      if (precondition(command)) return command;
    }
  }
  bool precondition(const Command& c) const {
    if (c.op >= TotalOps || c.a >= 8 || c.b >= 128) return false;
    if (c.op == Combine || c.op == Await) {
      return std::any_of(obligations_.begin(), obligations_.end(),
                         [](const Obligation& o) { return o.has_handle; });
    }
    return true;
  }
  void apply(const Command& c) {
    switch (c.op) {
      case Submit: case Throw: case SubmitDetached:
        submit(c, static_cast<Op>(c.op)); break;
      case Nested: nested(c); break;
      case Combine: {
        std::vector<dagflow::Handle> handles;
        for (const auto& o : obligations_)
          if (o.has_handle && (c.a & 1 || !o.failure)) handles.push_back(o.completion);
        if (handles.empty()) {
          for (const auto& o : obligations_)
            if (o.has_handle) { handles.push_back(o.completion); break; }
        }
        joins_.push_back(pool_->combine(std::span<const dagflow::Handle>(handles)));
        require(joins_.back().valid(), "nonempty fan-in has a completion");
        pool_->wait(joins_.back());
        require(joins_.back().ready(), "fan-in completes after source handles");
        tick(FanIn);
        break;
      }
      case Await: {
        std::vector<std::size_t> candidates;
        for (std::size_t i = 0; i < obligations_.size(); ++i)
          if (obligations_[i].has_handle) candidates.push_back(i);
        const auto& o = obligations_[candidates[c.b % candidates.size()]];
        pool_->wait(o.completion);
        require(o.calls->load(std::memory_order_acquire) == 1,
                "individual handle wait is an effect barrier");
        verify_handle(o);
        tick(WaitOne);
        break;
      }
      case Drain:
        verify_drained(); tick(WaitIdle); break;
      case Close:
        pool_->close(); closed_ = true; break;
      case Recreate:
        rebuild(); break;
    }
    ++histories_;
  }
  void invariant() const {
    require(pool_->closed() == closed_, "pool admission state follows specification");
    for (const auto& o : obligations_)
      require(o.calls->load(std::memory_order_acquire) <= 1,
              "no accepted packet duplicates observable effects");
    for (const auto& h : historical_)
      require(h.ready(), "historical completions remain ready across pool recreation");
    require(histories_ <= 10000, "bounded reference history");
  }
  void finish() { verify_drained(); }
};
}

int main() {
  run<PoolReferenceMachine>("reference pool: admission, effects, join, closure and reuse", 24, 32);
  for (unsigned i = 0; i < Count; ++i) {
    std::printf("pool transition %-15s %u\n", categories[i], coverage[i].load());
  }
}
