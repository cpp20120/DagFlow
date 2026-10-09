#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <latch>
#include <memory>
#include <new>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include <dagflow/task_scope.hpp>
#include "byte_reader.hpp"
#include "check.hpp"
#include "range_cases.hpp"

namespace {
using dagflow::fuzz::check;
using Scope = dagflow::TaskScope;
struct CallbackError {};
struct ConstructionError {};
struct Payload {
  std::atomic<unsigned>& destroyed;
  ~Payload() { destroyed.fetch_add(1, std::memory_order_release); }
};

dagflow::Config config(dagflow::fuzz::Bytes& bytes) {
  dagflow::Config cfg;
  cfg.threads = 1 + bytes.bound(4);
  cfg.shards = 1 + bytes.bound(6);
  cfg.pin_threads = false;
  // Exercise noncontiguous membership and empty domains in full-pool paths.
  for (unsigned i = 0; i < cfg.threads; ++i) cfg.worker_shards.push_back(bytes.bound(cfg.shards));
  return cfg;
}

struct Ledger {
  static constexpr unsigned limit = 1024;
  std::array<std::atomic<unsigned>, limit> calls{}, destroyed{}, created{};
  std::array<std::atomic<bool>, limit> accepted{};
  std::atomic<unsigned> throws{0};
  unsigned depth, mode, fault_depth;
  dagflow::SubmitOptions opt;
};

struct Descendant {
  Ledger* ledger;
  unsigned id, level;
  std::unique_ptr<Payload> payload;
  Descendant(Ledger& state, unsigned index, unsigned generation)
      : ledger(&state), id(index), level(generation),
        payload(new Payload{state.destroyed[index]}) {
    check(index < Ledger::limit && state.created[index].fetch_add(1) == 0);
  }
  Descendant(Descendant&&) noexcept = default;
  void operator()(Scope::Context& ctx) {
    auto& state = *ledger;
    check(state.calls[id].fetch_add(1) == 0);
    if (level == state.fault_depth) {
      if (state.mode == 2) { ctx.cancel(); check(ctx.cancelled()); }
      if (state.mode == 3) {
        state.throws.fetch_add(1);
        throw CallbackError{};
      }
    }
    if (level < state.depth) {
      const bool accepted = ctx.spawn(Descendant(state, id + 1, level + 1), state.opt);
      state.accepted[id + 1].store(accepted, std::memory_order_release);
      if (state.mode == 0) check(accepted);
      if (state.mode == 2 && level == state.fault_depth) check(!accepted);
    }
    if (level % 2 == 0) std::this_thread::yield();
  }
};

void admission_races(dagflow::fuzz::Bytes& bytes) {
  dagflow::Pool pool(config(bytes));
  const unsigned producers = 2 + bytes.bound(3), each = 1 + bytes.bound(16);
  Ledger ledger;
  ledger.depth = 1 + bytes.bound(8);
  ledger.mode = bytes.bound(4); // close only, external cancel, context cancel, callback error
  ledger.fault_depth = bytes.bound(ledger.depth + 1);
  ledger.opt.priority = bytes.bit() ? dagflow::Priority::High : dagflow::Priority::Normal;
  ledger.opt.mode = bytes.bit() ? dagflow::SubmissionMode::Enqueue : dagflow::SubmissionMode::Spawn;
  const unsigned delay = bytes.bound(32);
  dagflow::Handle completion;
  {
    Scope scope(pool);
    completion = scope.completion();
    // A root admitted before closure ensures the descendant-after-close path
    // is exercised even if every racing external publisher is rejected.
    std::latch root_entered(1), allow_descendants(1), go(1);
    check(scope.spawn([&](Scope::Context& ctx) {
      root_entered.count_down();
      allow_descendants.wait();
      const bool accepted = ctx.spawn(Descendant(ledger, 0, 0), ledger.opt);
      ledger.accepted[0].store(accepted);
      if (ledger.mode == 0) check(accepted);
      if (ledger.mode == 1) check(!accepted);
    }));
    root_entered.wait();
    std::vector<std::thread> threads;
    for (unsigned p = 0; p < producers; ++p) {
      threads.emplace_back([&, p] {
        go.wait();
        for (unsigned i = 0; i < each; ++i) {
          const unsigned id = (1 + p * each + i) * (ledger.depth + 1);
          ledger.accepted[id].store(scope.spawn(Descendant(ledger, id, 0), ledger.opt));
          if ((p + i) % 2) std::this_thread::yield();
        }
      });
    }
    threads.emplace_back([&] {
      go.wait();
      for (unsigned i = 0; i < delay; ++i) std::this_thread::yield();
      scope.close();
      if (ledger.mode == 1) scope.cancel();
      allow_descendants.count_down();
    });
    threads.emplace_back([&] { go.wait(); scope.wait(); });
    threads.emplace_back([&] {
      go.wait();
      try { scope.join(); } catch (const CallbackError&) {}
    });
    go.count_down();
    for (auto& thread : threads) thread.join();
    scope.wait();
    check(completion.ready() && !scope.spawn([] {}));
    bool failed = false;
    try { scope.join(); } catch (const CallbackError&) { failed = true; }
    check(failed == (ledger.throws.load() != 0));
    check(bool(scope.last_error()) == failed);
    if (ledger.mode == 0) check(!scope.cancelled());
  }
  for (unsigned i = 0; i < Ledger::limit; ++i) {
    check(ledger.destroyed[i].load() == ledger.created[i].load());
    check(ledger.calls[i].load() <= unsigned(ledger.accepted[i].load()));
    if (ledger.mode == 0) check(ledger.calls[i].load() == unsigned(ledger.accepted[i].load()));
  }
  pool.wait_idle();
}

struct SlowCopy {
  std::latch *entered, *release;
  std::atomic<unsigned> *calls, *destroyed;
  bool fail;
  std::unique_ptr<Payload> payload;
  SlowCopy(std::latch& e, std::latch& r, std::atomic<unsigned>& c,
           std::atomic<unsigned>& d, bool f)
      : entered(&e), release(&r), calls(&c), destroyed(&d), fail(f) {}
  SlowCopy(const SlowCopy& other)
      : entered(other.entered), release(other.release), calls(other.calls),
        destroyed(other.destroyed), fail(other.fail) {
    entered->count_down();
    release->wait();
    if (fail) throw ConstructionError{};
    payload = std::make_unique<Payload>(*destroyed);
  }
  SlowCopy(SlowCopy&&) noexcept = default;
  void operator()() { calls->fetch_add(1); }
};

void reserved_publication(dagflow::fuzz::Bytes& bytes) {
  dagflow::Pool pool(config(bytes));
  const bool child = bytes.bit(), cancel = bytes.bit(), fail = bytes.bit();
  std::latch entered(1), release(1);
  std::atomic<unsigned> calls{0}, destroyed{0};
  Scope scope(pool);
  SlowCopy function(entered, release, calls, destroyed, fail);
  std::thread publisher([&] {
    try {
      if (child) check(scope.spawn([&](Scope::Context& ctx) { check(ctx.spawn(function)); }));
      else check(scope.spawn(function));
    } catch (const ConstructionError&) { check(fail && !child); }
  });
  entered.wait(); // Admission succeeded, callable construction is still live.
  const auto completion = scope.close();
  if (cancel) scope.cancel();
  check(!completion.ready() && !scope.spawn([] {}));
  release.count_down();
  publisher.join();
  bool caught = false;
  try { scope.join(); } catch (const ConstructionError&) { caught = true; }
  check(caught == fail && bool(scope.last_error()) == fail);
  check(scope.cancelled() == (cancel || fail));
  check(calls.load() == unsigned(!fail && !cancel));
  check(destroyed.load() == unsigned(!fail) && completion.ready());
}

void publication_failure(dagflow::fuzz::Bytes& bytes) {
  dagflow::Pool pool(config(bytes));
  const bool child = bytes.bit();
  const int budget = bytes.bound(12);
  std::atomic<unsigned> calls{0}, destroyed{0};
  std::atomic<bool> failed{false};
  {
    Scope scope(pool);
    auto callback = [&, payload = std::unique_ptr<Payload>(new Payload{destroyed})] { calls.fetch_add(1); };
    if (child) {
      check(scope.spawn([&, fn = std::move(callback)](Scope::Context& ctx) mutable {
        try {
          dagflow::fuzz::AllocationBudget injection(budget);
          check(ctx.spawn(std::move(fn)));
        } catch (const std::bad_alloc&) { failed.store(true); }
      }));
    } else {
      try {
        dagflow::fuzz::AllocationBudget injection(budget);
        check(scope.spawn(std::move(callback)));
      } catch (const std::bad_alloc&) { failed.store(true); }
    }
    bool caught = false;
    try { scope.join(); } catch (const std::bad_alloc&) { caught = true; }
    check(caught == failed.load() && bool(scope.last_error()) == caught);
    check(scope.cancelled() == caught);
    check(calls.load() == unsigned(!caught) && destroyed.load() == 1);
  }
  // Failure must not poison pool admission, TLS accounting or a fresh scope.
  Scope recovery(pool);
  check(recovery.spawn([&] { calls.fetch_add(1); }));
  recovery.join();
  check(calls.load() == unsigned(!failed.load()) + 1);
}

void nested_join(dagflow::Pool& pool, Scope& ancestor, unsigned depth) {
  if (depth == 0) { ancestor.join(); return; }
  Scope child(pool);
  check(child.spawn([&] { nested_join(pool, ancestor, depth - 1); }));
  child.join();
}

void join_rejection(dagflow::fuzz::Bytes& bytes) {
  // Ancestor detection is thread-local: a single worker forces nested helping
  // onto the same active stack instead of constructing a cross-thread cycle.
  dagflow::Config cfg;
  cfg.threads = 1;
  cfg.pin_threads = false;
  dagflow::Pool pool(cfg);
  const unsigned depth = bytes.bound(17);
  {
    Scope ancestor(pool);
    check(ancestor.spawn([&] { nested_join(pool, ancestor, depth); }));
    bool caught = false;
    try { ancestor.join(); } catch (const std::logic_error&) { caught = true; }
    check(caught && ancestor.cancelled());
  }
  struct Cleanup {
    Scope& scope;
    std::atomic<unsigned>& rejected;
    ~Cleanup() {
      try { scope.wait(); }
      catch (const std::logic_error&) { rejected.fetch_add(1); }
    }
  };
  Scope scope(pool);
  std::atomic<unsigned> rejected{0};
  check(scope.spawn([cleanup = std::unique_ptr<Cleanup>(new Cleanup{scope, rejected})] {}));
  scope.join();
  check(rejected.load() == 1 && !scope.last_error());
}
} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  if (size > 4096) return 0;
  dagflow::fuzz::Bytes bytes(data, size);
  switch (bytes.bound(4)) {
    case 0: admission_races(bytes); break;
    case 1: reserved_publication(bytes); break;
    case 2: publication_failure(bytes); break;
    default: join_rejection(bytes); break;
  }
  return 0;
}
