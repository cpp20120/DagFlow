#include <algorithm>
#include <array>
#include <cstdint>
#include <dagflow/thread_pool.hpp>
#include <functional>
#include <stdexcept>
#include <utility>

#include "property_support.hpp"

namespace {
using dagflow::detail::CompletionCredit;
using dagflow::test::property::Command;
using dagflow::test::property::Random;
using dagflow::test::property::require;
using dagflow::test::property::run;

// Waiting is observation, not a credit. A Reject worker must never block on an
// unfinished completion, even when the completion originated in another Pool.
// A Help worker may instead execute children recursively and restore its
// thread-local wait frame on every return.
class WaitMachine {
  enum Op {
    Create,
    Fork,
    FinishRoot,
    FinishFork,
    DropObserver,
    Observe,
    RejectOwn,
    RejectOther,
    HelpNested,
    Count
  };
  static constexpr unsigned slots = 4;
  CompletionCredit root_;
  std::array<CompletionCredit, slots> forks_{};
  std::array<bool, slots> fork_live_{};
  dagflow::Handle observer_;
  unsigned credits_ = 0;
  bool root_live_ = false;
  static dagflow::Config config(dagflow::WorkerWaitPolicy policy) {
    dagflow::Config result;
    result.threads = 1;
    result.worker_wait_policy = policy;
    return result;
  }
  dagflow::Pool help_{config(dagflow::WorkerWaitPolicy::Help)};
  dagflow::Pool reject_{config(dagflow::WorkerWaitPolicy::Reject)};

  void settle() {
    root_.finish();
    root_live_ = false;
    for (unsigned i = 0; i < slots; ++i) {
      forks_[i].finish();
      fork_live_[i] = false;
    }
    credits_ = 0;
  }
  void reject_wait(dagflow::Pool& target) {
    const auto observed = observer_;
    auto result =
        reject_.submit([&target, observed] { target.wait(observed); });
    reject_.wait(result);
    bool failed = false;
    try {
      result.rethrow_if_failed();
    } catch (const std::logic_error&) {
      failed = true;
    }
    require(failed == (credits_ != 0),
            "Reject worker diagnoses unfinished own and foreign handles");
  }
  void nested(unsigned depth) {
    unsigned visits = 0, peak = 0;
    std::function<void(unsigned)> work = [&](unsigned remaining) {
      ++visits;
      peak = std::max(peak, dagflow::Pool::worker_wait_depth());
      if (remaining)
        help_.wait_and_rethrow(
            help_.submit([&, remaining] { work(remaining - 1); }));
    };
    help_.wait_and_rethrow(help_.submit([&] { work(depth); }));
    require(visits == depth + 1, "nested help executes descendants once");
    require(peak == depth, "nested help tracks the exact maximum wait depth");
    require(dagflow::Pool::worker_wait_depth() == 0,
            "nested worker depth does not leak to the external caller");
    auto afterwards = help_.submit([&] {
      require(dagflow::Pool::worker_wait_depth() == 0,
              "worker wait context is restored between tasks");
    });
    help_.wait_and_rethrow(afterwards);
  }

 public:
  Command next(Random& random) const {
    for (unsigned attempt = 0; attempt < 96; ++attempt) {
      Command c{static_cast<unsigned>(random.bounded(Count)),
                random.bounded(slots), random.bounded(7)};
      if (precondition(c)) return c;
    }
    return {Create, 0, 0};
  }
  bool precondition(const Command& c) const {
    if (c.op >= Count || c.a >= slots || c.b >= 7) return false;
    switch (c.op) {
      case Fork:
        return root_live_ && !fork_live_[c.a];
      case FinishRoot:
        return root_live_;
      case FinishFork:
        return fork_live_[c.a];
      case RejectOwn:
      case RejectOther:
      case Observe:
        return observer_.valid();
      default:
        return true;
    }
  }
  void apply(const Command& c) {
    switch (c.op) {
      case Create:
        settle();
        root_ = CompletionCredit::create();
        observer_ = root_.handle();
        root_live_ = true;
        credits_ = 1;
        break;
      case Fork:
        forks_[c.a] = root_.fork();
        fork_live_[c.a] = true;
        ++credits_;
        break;
      case FinishRoot:
        root_.finish();
        root_live_ = false;
        --credits_;
        break;
      case FinishFork:
        forks_[c.a].finish();
        fork_live_[c.a] = false;
        --credits_;
        break;
      case DropObserver:
        observer_ = {};
        break;
      case Observe:
        require(observer_.ready() == (credits_ == 0),
                "nonblocking readiness tracks final retirement");
        break;
      case RejectOwn:
        reject_wait(reject_);
        break;
      case RejectOther:
        reject_wait(help_);
        break;
      case HelpNested:
        nested(static_cast<unsigned>(c.b));
        break;
    }
  }
  void invariant() const {
    require(bool(root_) == root_live_, "root credit ownership is exact");
    unsigned expected = unsigned(root_live_);
    for (unsigned i = 0; i < slots; ++i) {
      require(bool(forks_[i]) == fork_live_[i],
              "fork credit ownership is exact");
      expected += fork_live_[i];
    }
    require(expected == credits_, "abstract credit total agrees with custody");
    if (observer_.valid())
      require(observer_.ready() == (credits_ == 0),
              "observers do not influence completion liveness");
  }
  void finish() {
    settle();
    invariant();
  }
};
}  // namespace

int main() {
  run<WaitMachine>("worker wait policy and nested help transitions", 48, 84);
}
