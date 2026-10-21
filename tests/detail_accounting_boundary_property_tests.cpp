#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <dagflow/detail/idle_accounting.hpp>
#include <exception>
#include <limits>
#include <optional>
#include <stdexcept>

#if defined(__linux__) && !defined(__SANITIZE_THREAD__) && \
    !defined(DAGFLOW_TEST_SKIP_DEATH_CASES)
#include <sys/wait.h>
#include <unistd.h>
#endif

#include "property_support.hpp"

namespace dagflow::detail {
// There are no worker threads in this fixture. The calling thread is the sole
// owner of every lane, and history can be installed only in a fresh instance
// before any observer or publication. It represents real completed work, not
// an outstanding-count corruption. In particular retirement may legitimately
// belong to a different lane from publication.
struct IdleAccountingBoundaryAccess {
  static void seed(IdleAccounting& accounting, std::uint64_t history) {
    using dagflow::test::property::require;
    require(accounting.workers_.get_deleter().count == 3,
            "boundary fixture has three executor lanes");
    require(accounting.waiters_.load() == 0 && !accounting.producers_,
            "history is installed before producers and observers exist");
    for (unsigned i = 0; i < 3; ++i)
      require(accounting.workers_[i].published.load() == 0 &&
                  accounting.workers_[i].retired.load() == 0,
              "only fresh accounting accepts a historical prefix");
    auto& external = accounting.producer_lane();
    for (unsigned i = 0; i < 2; ++i) {
      accounting.workers_[i].published.store(history);
      accounting.workers_[i].retired.store(history);
    }
    external.published.store(history);
    accounting.workers_[2].retired.store(history);
  }

  static std::uint64_t published(const IdleAccounting& accounting,
                                 unsigned lane) {
    if (lane < 3) return accounting.workers_[lane].published.load();
    // One main-thread producer; no raw pointer survives instance recreation.
    return accounting.producers_ ? accounting.producers_->lane.published.load()
                                 : 0;
  }
  static std::uint64_t retired(const IdleAccounting& accounting,
                               unsigned lane) {
    return accounting.workers_[lane].retired.load();
  }
};
}  // namespace dagflow::detail

namespace {
using namespace dagflow::test::property;
using Accounting = dagflow::detail::IdleAccounting;
using Access = dagflow::detail::IdleAccountingBoundaryAccess;
constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();

class AccountingBoundaryMachine {
  enum Op {
    Seed,
    PublishWorker,
    PublishExternal,
    Retire,
    Observe,
    Notify,
    Recreate,
    Count
  };
  struct Model {
    std::array<std::uint64_t, 4> published{};
    std::array<std::uint64_t, 3> retired{};
    unsigned pending = 0;
    bool seeded = false;
  } model_;
  std::optional<Accounting> accounting_{std::in_place, 3};

  unsigned retirement_room() const {
    unsigned room = 0;
    for (const auto retired : model_.retired)
      room +=
          static_cast<unsigned>(std::min<std::uint64_t>(16, maximum - retired));
    return room;
  }
  void retire(unsigned worker) {
    accounting_->retire(worker);
    ++model_.retired[worker];
    --model_.pending;
    accounting_->worker_idle();
  }
  void drain() {
    for (unsigned worker = 0; worker < 3; ++worker)
      while (model_.pending && model_.retired[worker] != maximum)
        retire(worker);
  }

 public:
  ~AccountingBoundaryMachine() { drain(); }
  Command next(Random& rng) const {
    if (!model_.seeded) return {Seed, rng.bounded(8), 0};
    for (;;) {
      Command command{static_cast<unsigned>(rng.bounded(Count)), rng.bounded(3),
                      0};
      if (precondition(command)) return command;
    }
  }
  bool precondition(const Command& c) const {
    if (c.op >= Count || c.b != 0) return false;
    if (c.op == Seed) return !model_.seeded && c.a < 8;
    if (!model_.seeded || c.a >= 3) return false;
    if (c.op == Retire) return model_.pending && model_.retired[c.a] != maximum;
    if (c.op == Observe || c.op == Recreate) return model_.pending == 0;
    if (c.op == PublishWorker || c.op == PublishExternal) {
      const auto lane = c.op == PublishWorker ? c.a : 3;
      // Exhausted publication is always safe to attempt: it must not create a
      // task. Successful publication reserves a still-representable retirement.
      return model_.published[lane] == maximum ||
             (model_.pending < 8 && model_.pending < retirement_room());
    }
    return true;
  }
  void apply(const Command& c) {
    if (c.op == Seed) {
      const auto history = maximum - c.a;
      Access::seed(*accounting_, history);
      model_.published = {history, history, 0, history};
      model_.retired = {history, history, history};
      model_.seeded = true;
    } else if (c.op == PublishWorker || c.op == PublishExternal) {
      const unsigned lane = c.op == PublishWorker ? unsigned(c.a) : 3u;
      bool overflow = false;
      try {
        if (c.op == PublishWorker)
          accounting_->publish_worker(lane);
        else
          accounting_->publish_external();
      } catch (const std::overflow_error&) {
        overflow = true;
      }
      require(overflow == (model_.published[lane] == maximum),
              "publication throws exactly at UINT64_MAX, before modifying the "
              "lane");
      if (!overflow) {
        ++model_.published[lane];
        ++model_.pending;
      }
    } else if (c.op == Retire) {
      retire(c.a);
    } else if (c.op == Observe) {
      accounting_->wait();
    } else if (c.op == Notify) {
      accounting_->worker_idle();
    } else if (c.op == Recreate) {
      accounting_->wait();
      accounting_.emplace(3);
      model_ = {};
    }
  }
  void invariant() {
    for (unsigned lane = 0; lane < 4; ++lane)
      require(Access::published(*accounting_, lane) == model_.published[lane],
              "publication counters retain the full monotone lifetime history");
    for (unsigned lane = 0; lane < 3; ++lane)
      require(Access::retired(*accounting_, lane) == model_.retired[lane],
              "retirement counters never wrap, including the final valid "
              "increment");
    require(model_.pending <= retirement_room(),
            "accepted obligations can all retire without counter exhaustion");
    if (!model_.pending) accounting_->wait();
  }
  void finish() {
    drain();
    require(model_.pending == 0,
            "all accepted obligations retire at trace end");
    accounting_->wait();
  }
};

// A saturated executor can steal a task from another publication lane. This
// makes its next retirement a genuine exhaustion case even though publication
// and total outstanding accounting are valid. Never run fail-fast in-process.
void retirement_exhaustion() {
#if defined(__linux__) && !defined(__SANITIZE_THREAD__) && \
    !defined(DAGFLOW_TEST_SKIP_DEATH_CASES)
  const auto child = fork();
  require(child >= 0, "create isolated single-threaded retirement test");
  if (child == 0) {
    // Normal exit from a nonreturning terminate handler flushes GCC coverage.
    // No worker threads or stack objects needing cleanup exist in the parent.
    std::set_terminate([] { std::exit(77); });
    Accounting accounting(3);
    Access::seed(accounting, maximum);
    accounting.wait();
    accounting.publish_worker(
        2);                // One new obligation, published successfully.
    accounting.retire(0);  // Executor 0 has already retired UINT64_MAX.
    std::_Exit(78);
  }
  int status = 0;
  require(waitpid(child, &status, 0) == child,
          "reap retirement overflow child");
  require(WIFEXITED(status) && WEXITSTATUS(status) == 77,
          "physical retirement counter exhaustion invokes terminate");
#else
  std::puts("retirement exhaustion subprocess skipped on this platform/build");
#endif
}
}  // namespace

int main() {
  retirement_exhaustion();
  run<AccountingBoundaryMachine>("physical accounting counter boundaries", 24,
                                 64);
}
