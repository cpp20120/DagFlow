#include <atomic>
#include <chrono>
#include <condition_variable>
#include <dagflow/task_scope.hpp>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "property_support.hpp"

namespace {
using dagflow::test::property::Command;
using dagflow::test::property::Random;
using dagflow::test::property::require;

class ScopeMachine {
  enum Op {
    Spawn,
    StartParent,
    ReleaseParent,
    Close,
    Cancel,
    Wait,
    Join,
    Destroy,
    Recreate,
    Count
  };
  struct Model {
    bool exists = true;
    bool open = true;
    bool cancelled = false;
    bool pending = false;
    bool parent_fails = false;
    bool parent_cancels = false;
    unsigned children = 0;
    unsigned executed = 0;
    unsigned cleaned = 0;
    int error = 0;
  } model_;
  struct Gate {
    std::mutex mutex;
    std::condition_variable cv;
    bool entered = false;
    bool released = false;
    void enter() {
      std::unique_lock lock(mutex);
      entered = true;
      cv.notify_all();
      cv.wait(lock, [&] { return released; });
    }
    bool await_entry() {
      std::unique_lock lock(mutex);
      return cv.wait_for(lock, std::chrono::seconds(5),
                         [&] { return entered; });
    }
    void release() {
      std::lock_guard lock(mutex);
      released = true;
      cv.notify_all();
    }
  };
  struct Capture {
    std::atomic<unsigned>* destroyed;
    ~Capture() { ++*destroyed; }
  };
  std::atomic<unsigned> executed_{0};
  std::atomic<unsigned> cleaned_{0};
  std::atomic<unsigned> child_accepts_{0};
  std::atomic<bool> child_saw_cancel_{false};
  dagflow::Pool pool_{{.threads = 1}};
  std::unique_ptr<dagflow::TaskScope> scope_ =
      std::make_unique<dagflow::TaskScope>(pool_);
  std::shared_ptr<Gate> gate_;
  dagflow::Handle observer_ = scope_->completion();
  std::vector<std::pair<dagflow::Handle, int>> old_results_;

  void record_failure(int error) {
    if (!model_.error) model_.error = error;
    model_.open = false;
    model_.cancelled = true;
  }

  void release_pending(bool destroy = false) {
    if (!model_.pending) return;
    const auto expected_children =
        model_.cancelled || model_.parent_cancels ? 0u : model_.children;
    gate_->release();
    if (destroy) {
      scope_.reset();
      require(observer_.ready(), "scope destruction itself completes its work");
      require(cleaned_.load() == model_.cleaned + 1,
              "scope destructor returns only after capture destruction");
    }
    pool_.wait_idle();
    // The held parent has already passed its invocation cancellation check.
    ++model_.executed;
    // Successful child admission can still be followed by cancellation. With
    // one worker, a failing parent cancels before any of its children run.
    if (!model_.parent_fails) model_.executed += expected_children;
    ++model_.cleaned;
    require(child_accepts_.load() == expected_children,
            "close retains descendant authority; cancellation revokes it");
    require(child_saw_cancel_.load() == model_.cancelled,
            "live context observes cancellation independently of close");
    if (model_.parent_cancels) {
      model_.open = false;
      model_.cancelled = true;
    }
    if (model_.parent_fails) record_failure(23);
    model_.pending = false;
  }

  static int error_of(const dagflow::Handle& handle) {
    try {
      handle.rethrow_if_failed();
    } catch (int error) {
      return error;
    }
    return 0;
  }

 public:
  ~ScopeMachine() {
    if (gate_) gate_->release();
    scope_.reset();
    pool_.wait_idle();
  }

  void finish() {
    if (scope_) {
      release_pending();
      scope_->wait();
      model_.open = false;
    }
    invariant();
  }

  Command next(Random& random) const {
    for (;;) {
      Command c{static_cast<unsigned>(random.bounded(Count)), random.bounded(5),
                random.bounded(4)};
      if (precondition(c)) return c;
    }
  }

  bool precondition(const Command& c) const {
    if (c.op >= Count || c.a >= 5 || c.b >= 4) return false;
    if (c.op == Recreate) return !model_.exists;
    if (!model_.exists) return false;
    if (c.op == StartParent) return model_.open && !model_.pending;
    if (c.op == ReleaseParent) return model_.pending;
    if (c.op == Spawn) return !model_.pending || !model_.open;
    return true;
  }

  void apply(const Command& c) {
    switch (c.op) {
      case Spawn: {
        auto token = std::make_unique<Capture>(&cleaned_);
        auto function = [this, token = std::move(token), fail = bool(c.b)] {
          ++executed_;
          if (fail) throw 17;
        };
        const bool accepted = scope_->spawn(std::move(function));
        require(accepted == model_.open, "external scope admission model");
        if (accepted) {
          ++model_.executed;
          ++model_.cleaned;
          pool_.wait_idle();
          if (c.b) record_failure(17);
        } else {
          // Rejection must not move the caller's callable into task storage.
          require(cleaned_.load() == model_.cleaned,
                  "rejected scope spawn leaves caller-owned captures alive");
          ++model_.cleaned;  // Destroyed at end of this command's local scope.
        }
        break;
      }
      case StartParent:
        gate_ = std::make_shared<Gate>();
        model_.pending = true;
        model_.children = static_cast<unsigned>(c.a);
        model_.parent_fails = c.b & 1;
        model_.parent_cancels = c.b & 2;
        child_accepts_ = 0;
        child_saw_cancel_ = false;
        require(scope_->spawn([this, gate = gate_,
                               token = std::make_unique<Capture>(&cleaned_),
                               children = c.a, fail = bool(c.b & 1),
                               cancel = bool(c.b & 2)](
                                  dagflow::TaskScope::Context& context) {
          gate->enter();
          child_saw_cancel_ = context.cancelled();
          if (cancel) context.cancel();
          ++executed_;
          for (uint64_t i = 0; i < children; ++i)
            if (context.spawn([this] { ++executed_; })) ++child_accepts_;
          if (fail) throw 23;
        }),
                "open scope accepts parent");
        require(gate_->await_entry(), "accepted parent starts within deadline");
        break;
      case ReleaseParent:
        release_pending();
        break;
      case Close:
        observer_ = scope_->close();
        model_.open = false;
        break;
      case Cancel:
        scope_->cancel();
        model_.open = false;
        model_.cancelled = true;
        break;
      case Wait:
      case Join: {
        release_pending();
        model_.open = false;
        int error = 0;
        try {
          if (c.op == Wait)
            scope_->wait();
          else
            scope_->join();
        } catch (int caught) {
          error = caught;
        }
        require(error == (c.op == Join ? model_.error : 0),
                "wait only drains; repeated join rethrows retained error");
        break;
      }
      case Destroy:
        // Callback completion and descendants must drain inside destruction.
        release_pending(true);
        scope_.reset();
        model_.exists = false;
        model_.open = false;
        old_results_.emplace_back(observer_, model_.error);
        break;
      case Recreate:
        scope_ = std::make_unique<dagflow::TaskScope>(pool_);
        observer_ = scope_->completion();
        model_.exists = true;
        model_.open = true;
        model_.cancelled = false;
        model_.error = 0;
        break;
    }
  }

  void invariant() const {
    require(executed_.load() == model_.executed,
            "exact accepted invocations, including cancelled queued children");
    require(cleaned_.load() == model_.cleaned,
            "completion cleanup tracks ownership of every submitted capture");
    require(observer_.ready() == (!model_.open && !model_.pending),
            "scope completion needs both closed sentinel and retired work");
    if (scope_) {
      require(scope_->cancelled() == model_.cancelled, "cancellation state");
      require(bool(scope_->last_error()) == bool(model_.error),
              "cancellation alone does not invent an error");
    }
    if (observer_.ready())
      require(error_of(observer_) == model_.error,
              "completion preserves the first scope failure");
    for (const auto& [handle, error] : old_results_) {
      require(handle.ready(), "observer outlives destroyed scope");
      require(error_of(handle) == error, "old scope result is immutable");
    }
  }
};
}  // namespace

int main() { dagflow::test::property::run<ScopeMachine>("task scope", 48, 80); }
