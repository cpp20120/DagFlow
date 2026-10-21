#include <algorithm>
#include <atomic>
#include <dagflow/graph_scope.hpp>
#include <dagflow/task_scope.hpp>
#include <memory>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

#include "property_support.hpp"

namespace {
using namespace dagflow::test::property;

dagflow::Config config() {
  dagflow::Config result;
  result.threads = 1;
  result.pin_threads = false;
  return result;
}

struct CallError {
  unsigned stage;
};

struct Ledger {
  std::atomic<unsigned> live{0}, calls{0}, copies{0}, moves{0};
  unsigned mode = 0;
};

// A throwing move is deliberately copy-like: the caller's object stays valid
// even if any later construction fails. Every completed construction owns one
// lifetime obligation; a throwing constructor never acquires that obligation.
struct Body {
  Ledger* ledger;
  explicit Body(Ledger& value) : ledger(&value) { ++ledger->live; }
  Body(const Body& other) : ledger(other.ledger) {
    ++ledger->copies;
    if (ledger->mode == 1) throw CallError{1};
    ++ledger->live;
  }
  Body(Body&& other) : ledger(other.ledger) {
    ++ledger->moves;
    if (ledger->mode == 2) throw CallError{2};
    ++ledger->live;
  }
  ~Body() { --ledger->live; }
  void operator()() {
    ++ledger->calls;
    if (ledger->mode == 3) throw CallError{3};
  }
};

unsigned error_of(const dagflow::Handle& handle) {
  try {
    handle.rethrow_if_failed();
  } catch (const CallError& error) {
    return error.stage;
  }
  return 0;
}

class PoolPublicationMachine {
  enum Op { Submit, Detached, Close, Recreate, EmptyCredit, Count };
  struct Model {
    bool open = true;
    unsigned calls = 0;
  } model_;
  Ledger ledger_;
  std::unique_ptr<dagflow::Pool> pool_ =
      std::make_unique<dagflow::Pool>(config());
  std::vector<std::pair<dagflow::Handle, unsigned>> results_;

 public:
  ~PoolPublicationMachine() { pool_.reset(); }
  Command next(Random& rng) const {
    return {static_cast<unsigned>(rng.bounded(Count)), rng.bounded(4), 0};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 4 && c.b == 0;
  }
  void apply(const Command& c) {
    if (c.op == Close) {
      pool_->close();
      model_.open = false;
    } else if (c.op == Recreate) {
      pool_.reset();
      pool_ = std::make_unique<dagflow::Pool>(config());
      model_.open = true;
    } else if (c.op == EmptyCredit) {
      dagflow::detail::CompletionCredit empty;
      auto observer = empty.handle();
      require(!observer.valid() && observer.ready(),
              "empty credit creates a completed invalid observer");
      empty.fail(std::make_exception_ptr(CallError{3}));
      pool_->wait_and_rethrow(observer);
      dagflow::detail::ScheduledTaskDeleter{}(nullptr);
    } else {
      ledger_.mode = c.a;
      unsigned construction_error = 0;
      bool rejected = false;
      {
        Body callable(ledger_);
        try {
          if (c.op == Submit) {
            auto result = pool_->submit(callable);
            pool_->wait(result);
            results_.emplace_back(result, c.a == 3 ? 3u : 0u);
          } else {
            pool_->submit_detached(callable);
          }
        } catch (const CallError& error) {
          construction_error = error.stage;
        } catch (const std::logic_error&) {
          rejected = true;
        }
        pool_->wait_idle();
        require(
            ledger_.live == 1,
            "publication cleans every constructed copy before returning idle");
      }
      const bool fails_construction = c.a == 1 || c.a == 2;
      require(construction_error == (fails_construction ? c.a : 0),
              "Pool propagates callable construction failure before admission");
      require(rejected == (!model_.open && !fails_construction),
              "closed admission rejects a successfully constructed packet");
      if (model_.open && !fails_construction) ++model_.calls;
    }
  }
  void invariant() const {
    require(pool_->closed() == !model_.open,
            "Pool admission is unchanged by errors");
    require(ledger_.live == 0,
            "no callable survives publication rollback or idle");
    require(ledger_.calls == model_.calls,
            "only accepted callable bodies execute");
    for (const auto& [result, error] : results_) {
      require(result.ready(), "task results survive pool recreation");
      require(error_of(result) == error,
              "task result preserves invocation error");
    }
  }
  void finish() { pool_->wait_idle(); }
};

class ScopePublicationMachine {
  enum Op {
    Publish,
    PublishChild,
    SkipChild,
    Close,
    Cancel,
    Join,
    Recreate,
    Count
  };
  struct Model {
    bool open = true;
    bool cancelled = false;
    unsigned error = 0;
    unsigned calls = 0;
  } model_;
  Ledger ledger_;
  dagflow::Pool pool_{config()};
  std::unique_ptr<dagflow::TaskScope> scope_ =
      std::make_unique<dagflow::TaskScope>(pool_);
  std::vector<std::pair<dagflow::Handle, unsigned>> old_results_;

  void fail(unsigned error) {
    model_.open = false;
    model_.cancelled = true;
    if (!model_.error) model_.error = error;
  }

 public:
  ~ScopePublicationMachine() {
    scope_.reset();
    pool_.wait_idle();
  }
  Command next(Random& rng) const {
    for (;;) {
      Command c{static_cast<unsigned>(rng.bounded(Count)), rng.bounded(4),
                rng.bounded(2)};
      if (precondition(c)) return c;
    }
  }
  bool precondition(const Command& c) const {
    if (c.op >= Count || c.a >= 4 || c.b >= 2) return false;
    return (c.op != PublishChild && c.op != SkipChild) || model_.open;
  }
  void apply(const Command& c) {
    if (c.op == Close) {
      scope_->close();
      model_.open = false;
    } else if (c.op == Cancel) {
      scope_->cancel();
      model_.open = false;
      model_.cancelled = true;
    } else if (c.op == Join) {
      unsigned error = 0;
      try {
        scope_->join();
      } catch (const CallError& caught) {
        error = caught.stage;
      }
      require(error == model_.error,
              "join retains publication or invocation failure");
      model_.open = false;
    } else if (c.op == Recreate) {
      const auto observer = scope_->completion();
      scope_.reset();
      old_results_.emplace_back(observer, model_.error);
      scope_ = std::make_unique<dagflow::TaskScope>(pool_);
      model_.open = true;
      model_.cancelled = false;
      model_.error = 0;
    } else {
      const unsigned mode = c.op == SkipChild ? 0u : unsigned(c.a);
      ledger_.mode = mode;
      const auto copies_before = ledger_.copies.load();
      const auto moves_before = ledger_.moves.load();
      unsigned construction_error = 0;
      bool accepted = false;
      if (c.op == Publish) {
        Body callable(ledger_);
        try {
          accepted = c.b ? scope_->submit(callable) : scope_->spawn(callable);
        } catch (const CallError& error) {
          construction_error = error.stage;
        }
        pool_.wait_idle();
        require(ledger_.live == 1,
                "scope releases all runtime copies after publication");
      } else {
        // Parent and child have separate lifetimes. Catching a child
        // publication error in the callback must not clear the scope's recorded
        // failure.
        scope_->submit([&](dagflow::TaskScope::Context& context) {
          Body callable(ledger_);
          try {
            accepted = c.b ? context.submit(callable) : context.spawn(callable);
          } catch (const CallError& error) {
            construction_error = error.stage;
          }
          if (c.op == SkipChild) context.cancel();
        });
        pool_.wait_idle();
      }
      if (!model_.open) {
        require(!accepted && !construction_error,
                "closed scope rejects before attempting callable construction");
        require(
            ledger_.copies == copies_before && ledger_.moves == moves_before,
            "rejection preserves the caller's callable without copy or move");
      } else {
        const bool construction_fails = mode == 1 || mode == 2;
        require(
            accepted == !construction_fails,
            "external and Context aliases have identical acceptance semantics");
        require(construction_error == (construction_fails ? mode : 0),
                "construction failure propagates at its publication boundary");
        if (c.op == SkipChild) {
          model_.open = false;
          model_.cancelled = true;
        } else if (construction_fails) {
          fail(mode);
        } else {
          ++model_.calls;
          if (mode == 3) fail(mode);
        }
      }
    }
  }
  void invariant() const {
    require(ledger_.live == 0,
            "all local and runtime callable obligations retire");
    require(
        ledger_.calls == model_.calls,
        "cancelled queued callbacks skip invocation without leaking captures");
    require(scope_->cancelled() == model_.cancelled,
            "scope failure cancels admission");
    require(scope_->completion().ready() == !model_.open,
            "open sentinel owns readiness");
    require(error_of(scope_->completion()) == model_.error,
            "caught publication failure remains visible through the observer");
    for (const auto& [observer, error] : old_results_) {
      require(observer.ready(), "scope destruction completes old observers");
      require(error_of(observer) == error,
              "scope recreation preserves old results");
    }
  }
  void finish() {
    scope_->wait();
    model_.open = false;
  }
};

class GraphRangeConstructionMachine {
  enum Op { Append, Run, Wait, Clear, Count };
  static constexpr unsigned extent = 2 * DAGFLOW_DEFAULT_RANGE_CHUNK + 1;
  static constexpr unsigned lengths[]{0, 1, DAGFLOW_DEFAULT_RANGE_CHUNK + 1,
                                      extent};
  struct CopyLedger {
    unsigned live = 0, copies = 0, fail_copy = 0;
    bool fail_move = false;
  } ledger_;
  struct Visitor {
    CopyLedger* ledger;
    explicit Visitor(CopyLedger& value) : ledger(&value) { ++ledger->live; }
    Visitor(const Visitor& other) : ledger(other.ledger) {
      if (++ledger->copies == ledger->fail_copy) throw CallError{1};
      ++ledger->live;
    }
    Visitor(Visitor&& other) : ledger(other.ledger) {
      if (ledger->fail_move) throw CallError{2};
      ++ledger->live;
    }
    ~Visitor() { --ledger->live; }
    void operator()(unsigned& value) { ++value; }
  };
  struct Model {
    std::vector<unsigned> lengths;
    unsigned copies = 0;
    bool dirty = false;
    std::vector<unsigned> values = std::vector<unsigned>(extent);
  } model_;
  std::vector<unsigned> values_ = std::vector<unsigned>(extent);
  dagflow::Pool pool_{config()};
  dagflow::GraphScope scope_{pool_};
  dagflow::JobHandle previous_;

  void account_run() {
    for (const auto length : model_.lengths)
      for (unsigned i = 0; i < length; ++i) ++model_.values[i];
    model_.dirty = false;
  }

 public:
  ~GraphRangeConstructionMachine() { scope_.clear(); }
  Command next(Random& rng) const {
    auto op = static_cast<unsigned>(rng.bounded(Count));
    if (op == Append && model_.lengths.size() == 6) op = Clear;
    return {op, rng.bounded(4), rng.bounded(5)};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 4 && c.b < 5 &&
           (c.op != Append || model_.lengths.size() < 6);
  }
  void apply(const Command& c) {
    if (c.op == Append) {
      const auto length = lengths[c.a];
      const unsigned chunks = (length + DAGFLOW_DEFAULT_RANGE_CHUNK - 1) /
                              DAGFLOW_DEFAULT_RANGE_CHUNK;
      ledger_.copies = 0;
      ledger_.fail_copy = c.b < 4 ? unsigned(c.b) : 0;
      ledger_.fail_move = c.b == 4;
      const bool should_fail =
          length && (ledger_.fail_move ||
                     (ledger_.fail_copy && ledger_.fail_copy <= chunks + 1));
      unsigned error = 0;
      {
        Visitor visitor(ledger_);
        try {
          auto range = std::span{values_}.first(length);
          auto handle =
              model_.lengths.empty()
                  ? scope_.parallel_for(range, visitor)
                  : scope_.parallel_for_after(previous_, range, visitor);
          previous_ = handle;
        } catch (const CallError& caught) {
          error = caught.stage;
        }
      }
      ledger_.fail_copy = 0;
      ledger_.fail_move = false;
      require(bool(error) == should_fail,
              "partial per-chunk callable construction fails transactionally");
      if (!should_fail) {
        model_.lengths.push_back(length);
        model_.copies += chunks;
        model_.dirty = true;
      }
    } else if (c.op == Clear) {
      scope_.clear();
      model_.lengths.clear();
      model_.copies = 0;
      model_.dirty = false;
    } else {
      if (c.op == Run) {
        scope_.run_and_wait();
        account_run();
      } else {
        scope_.wait();
        if (model_.dirty) account_run();
      }
    }
  }
  void invariant() const {
    require(
        scope_.size() == model_.lengths.size(),
        "failed range construction preserves the previous graph definition");
    require(ledger_.live == model_.copies,
            "exactly one callable per successful chunk survives until clear");
    require(
        values_ == model_.values,
        "rollback executes nothing and subsequent runs retain earlier work");
    require(!scope_.last_error(),
            "builder failure does not become an invocation error");
  }
  void finish() {
    apply({Wait, 0, 0});
    invariant();
    apply({Clear, 0, 0});
  }
};
}  // namespace

int main() {
  run<PoolPublicationMachine>("public Pool callable publication", 16, 48);
  run<ScopePublicationMachine>("public scope callable publication", 24, 64);
  run<GraphRangeConstructionMachine>("public GraphScope construction rollback",
                                     16, 40);
}
