#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <dagflow/thread_pool.hpp>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <vector>

#include "property_support.hpp"

namespace {
using dagflow::test::property::Command;
using dagflow::test::property::Random;
using dagflow::test::property::require;

// A detached task reports through the configured callback, while a handled
// task retains the exact same exception in its Handle instead.
std::atomic<unsigned> detached_errors{0};
std::atomic<unsigned> unexpected_errors{0};
void detached_error(std::exception_ptr error) noexcept {
  try {
    std::rethrow_exception(error);
  } catch (int value) {
    if (value == 7)
      ++detached_errors;
    else
      ++unexpected_errors;
  } catch (...) {
    ++unexpected_errors;
  }
}

class PoolMachine {
  enum Op {
    Submit,
    Detached,
    StartParent,
    ReleaseParent,
    Close,
    WaitIdle,
    Shutdown,
    Destroy,
    Recreate,
    Count
  };
  enum class State { open, closed, stopped, destroyed };
  struct Model {
    State state = State::open;
    bool pending = false;
    unsigned children = 0;
    unsigned executed = 0;
    unsigned cleaned = 0;
    unsigned reported = 0;
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
    ~Capture() { destroyed->fetch_add(1); }
  };
  std::atomic<unsigned> executed_{0};
  std::atomic<unsigned> cleaned_{0};
  std::atomic<bool> worker_shutdown_rejected_{false};
  std::unique_ptr<dagflow::Pool> pool_ = make_pool();
  std::shared_ptr<Gate> gate_;
  std::vector<dagflow::Handle> results_;
  std::vector<bool> errors_;

  static std::unique_ptr<dagflow::Pool> make_pool() {
    dagflow::Config cfg;
    cfg.threads = 1;
    cfg.detached_error_handler = detached_error;
    return std::make_unique<dagflow::Pool>(cfg);
  }
  auto capture() { return std::make_unique<Capture>(&cleaned_); }

  void release_pending() {
    if (!model_.pending) return;
    gate_->release();
    model_.executed += 1 + model_.children;
    ++model_.cleaned;
    model_.pending = false;
  }

 public:
  PoolMachine() {
    detached_errors.store(0);
    unexpected_errors.store(0);
  }
  ~PoolMachine() {
    if (gate_) gate_->release();
    // Captures borrow counters, so drain while all machine members still live.
    pool_.reset();
  }

  void finish() {
    if (pool_) {
      release_pending();
      pool_->shutdown();
      model_.state = State::stopped;
    }
    invariant();
  }

  Command next(Random& random) const {
    for (;;) {
      Command c{static_cast<unsigned>(random.bounded(Count)), random.bounded(5),
                random.bounded(2)};
      if (precondition(c)) return c;
    }
  }

  bool precondition(const Command& c) const {
    if (c.op >= Count || c.a >= 5 || c.b >= 2) return false;
    if (c.op == Recreate) return model_.state == State::destroyed;
    if (model_.state == State::destroyed) return false;
    if (c.op == StartParent)
      return model_.state == State::open && !model_.pending;
    if (c.op == ReleaseParent) return model_.pending;
    if (c.op == WaitIdle) return !model_.pending;
    // An open pool with a blocked worker could fill tiny ingress queues.
    if (c.op == Submit || c.op == Detached)
      return !model_.pending || model_.state != State::open;
    return true;
  }

  void apply(const Command& c) {
    switch (c.op) {
      case Submit:
      case Detached: {
        bool rejected = false;
        auto function = [this, token = capture(), fail = bool(c.b)] {
          ++executed_;
          if (fail) throw 7;
        };
        try {
          const dagflow::SubmitOptions options{
              .affinity = static_cast<uint32_t>(c.a * 1007),
              .priority = c.a & 1 ? dagflow::Priority::High
                                  : dagflow::Priority::Normal,
              .mode = c.a & 2 ? dagflow::SubmissionMode::Enqueue
                              : dagflow::SubmissionMode::Spawn};
          if (c.op == Submit) {
            results_.push_back(pool_->submit(std::move(function), options));
            errors_.push_back(c.b);
          } else {
            pool_->submit_detached(std::move(function), options);
          }
        } catch (const std::logic_error&) {
          rejected = true;
        }
        require(rejected == (model_.state != State::open),
                "external admission is exactly the open state");
        ++model_.cleaned;  // Failed publication also consumes its capture.
        if (!rejected) {
          ++model_.executed;
          if (c.op == Detached && c.b) ++model_.reported;
          pool_->wait_idle();
        }
        break;
      }
      case StartParent: {
        gate_ = std::make_shared<Gate>();
        model_.pending = true;
        model_.children = static_cast<unsigned>(c.a);
        worker_shutdown_rejected_ = false;
        results_.push_back(pool_->submit([this, executor = pool_.get(),
                                          gate = gate_, token = capture(),
                                          children = c.a, enqueue = bool(c.b)] {
          gate->enter();
          try {
            executor->shutdown();
          } catch (const std::logic_error&) {
            worker_shutdown_rejected_ = true;
          }
          ++executed_;
          dagflow::SubmitOptions options;
          options.mode = enqueue ? dagflow::SubmissionMode::Enqueue
                                 : dagflow::SubmissionMode::Spawn;
          for (uint64_t i = 0; i < children; ++i)
            executor->submit_detached([this] { ++executed_; }, options);
        }));
        errors_.push_back(false);
        require(gate_->await_entry(), "accepted parent starts within deadline");
        break;
      }
      case ReleaseParent:
        release_pending();
        pool_->wait_idle();
        require(worker_shutdown_rejected_, "own-worker shutdown is rejected");
        break;
      case Close:
        pool_->close();
        if (model_.state == State::open) model_.state = State::closed;
        break;
      case WaitIdle:
        pool_->wait_idle();
        break;
      case Shutdown:
        release_pending();
        pool_->shutdown();
        model_.state = State::stopped;
        break;
      case Destroy:
        release_pending();
        pool_.reset();
        model_.state = State::destroyed;
        break;
      case Recreate:
        pool_ = make_pool();
        model_.state = State::open;
        break;
    }
  }

  void invariant() const {
    if (pool_)
      require(pool_->closed() == (model_.state != State::open),
              "close is monotone until a distinct pool is constructed");
    require(executed_.load() == model_.executed,
            "drain executes accepted tasks and all descendants exactly once");
    require(cleaned_.load() == model_.cleaned,
            "drain includes capture destruction, rejection cleans up too");
    require(detached_errors.load() == model_.reported,
            "only accepted detached failures reach the exception reporter");
    require(unexpected_errors.load() == 0,
            "detached error propagation retains the exception identity");
    for (std::size_t i = 0; i < results_.size(); ++i) {
      const bool pending = model_.pending && i + 1 == results_.size();
      require(results_[i].ready() == !pending,
              "observer stays unready through held callback and survives pool");
      if (pending) continue;
      bool failed = false;
      try {
        results_[i].rethrow_if_failed();
      } catch (int error) {
        require(error == 7, "retained exception identity");
        failed = true;
      }
      require(failed == errors_[i], "shutdown preserves task result errors");
    }
  }
};
}  // namespace

int main() {
  dagflow::test::property::run<PoolMachine>("pool lifecycle", 48, 80);
}
