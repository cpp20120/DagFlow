#include <atomic>
#include <barrier>
#include <latch>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "dagflow/dagflow.hpp"
#include "support.hpp"

using Scope = dagflow::TaskScope;

struct Payload {
  std::atomic<int>* destroyed;
  ~Payload() { ++*destroyed; }
};

void empty_and_closed(dagflow::Pool& pool) {
  Scope scope(pool);
  auto result = scope.completion();
  CHECK(result.valid() && !result.ready());
  CHECK(scope.close().ready());
  CHECK(!scope.spawn([] {}) && !scope.submit([] {}));
  scope.wait();
  scope.join();
  scope.join();
  CHECK(!scope.cancelled() && !scope.last_error());
}

void descendants_after_close(dagflow::Pool& pool) {
  Scope scope(pool);
  std::latch entered(1), release(1);
  std::atomic<int> calls{0}, destroyed{0};
  CHECK(scope.spawn([&, owned = std::make_unique<Payload>(&destroyed)](Scope::Context& ctx) {
    entered.count_down();
    release.wait();
    CHECK(!ctx.cancelled());
    CHECK(ctx.spawn([&](Scope::Context& child) {
      ++calls;
      CHECK(child.submit([&] { ++calls; }));
    }));
    ++calls;
  }));
  entered.wait();
  auto result = scope.close();
  CHECK(!result.ready() && !scope.spawn([] {}));
  release.count_down();
  scope.join();
  CHECK(result.ready() && calls == 3 && destroyed == 1);
}

void destructor_drains(dagflow::Pool& pool) {
  std::atomic<int> calls{0}, destroyed{0};
  dagflow::Handle result;
  {
    Scope scope(pool);
    result = scope.completion();
    CHECK(scope.spawn([&, owned = std::make_unique<Payload>(&destroyed)](Scope::Context& ctx) {
      CHECK(ctx.spawn([&] { ++calls; }));
      ++calls;
    }));
  }
  CHECK(result.ready() && calls == 2 && destroyed == 1);
  try {
    Scope scope(pool);
    result = scope.completion();
    scope.spawn([&] { ++calls; throw std::runtime_error("child failure"); });
    throw std::runtime_error("owner failure");
  } catch (const std::runtime_error& error) {
    CHECK(std::string(error.what()) == "owner failure");
  }
  CHECK(result.ready() && calls == 3);
  bool failed = false;
  try { result.rethrow_if_failed(); }
  catch (const std::runtime_error&) { failed = true; }
  CHECK(failed);
}

void submit_close_race(dagflow::Pool& pool) {
  for (int round = 0; round < 32; ++round) {
    Scope scope(pool);
    std::atomic<int> accepted{0}, called{0};
    std::latch go(1), first_submissions(4);
    std::vector<std::thread> producers;
    for (int p = 0; p < 4; ++p) {
      producers.emplace_back([&] {
        CHECK(scope.spawn([&] { ++called; }));
        ++accepted;
        first_submissions.count_down();
        go.wait();
        for (int i = 0; i < 64; ++i)
          if (scope.spawn([&] { ++called; })) ++accepted;
      });
    }
    first_submissions.wait();
    go.count_down();
    scope.join();
    for (auto& producer : producers) producer.join();
    CHECK(accepted == called && accepted >= 4);
    CHECK(scope.completion().ready());
  }
}

// The only worker is occupied, so cancellation deterministically precedes
// every scope callback's entry check.
void cancellation_skips_queued(dagflow::Pool& pool) {
  std::latch entered(1), release(1);
  auto gate = pool.submit([&] { entered.count_down(); release.wait(); });
  entered.wait();
  Scope scope(pool);
  std::atomic<int> called{0}, destroyed{0};
  for (int i = 0; i < 16; ++i)
    CHECK(scope.spawn([&, owned = std::make_unique<Payload>(&destroyed)] { ++called; }));
  scope.cancel();
  CHECK(scope.cancelled() && !scope.spawn([] {}));
  CHECK(!scope.completion().ready());
  release.count_down();
  scope.join();
  pool.wait(gate);
  CHECK(called == 0 && destroyed == 16 && !scope.last_error());

  Scope child_cancel(pool);
  CHECK(child_cancel.spawn([](Scope::Context& ctx) {
    ctx.cancel();
    CHECK(ctx.cancelled() && !ctx.spawn([] {}));
  }));
  child_cancel.join();
  CHECK(child_cancel.cancelled() && !child_cancel.last_error());
}

void failure_and_first_error(dagflow::Pool& pool) {
  Scope scope(pool);
  std::latch running(2);
  std::atomic<int> destroyed{0};
  for (int i = 0; i < 2; ++i) {
    CHECK(scope.spawn([&, i, owned = std::make_unique<Payload>(&destroyed)] {
      running.arrive_and_wait();
      throw std::runtime_error(i == 0 ? "first candidate" : "second candidate");
    }));
  }
  scope.wait();  // Completion-only; failure is retained.
  CHECK(scope.cancelled() && destroyed == 2 && !scope.spawn([] {}));
  std::string first;
  try { std::rethrow_exception(scope.last_error()); }
  catch (const std::runtime_error& error) { first = error.what(); }
  for (int i = 0; i < 2; ++i) {
    bool failed = false;
    try { scope.join(); }
    catch (const std::runtime_error& error) { failed = true; CHECK(first == error.what()); }
    CHECK(failed);
  }
}

void cooperative_join_and_self_join(dagflow::Pool& pool) {
  // Single worker must help its children instead of blocking.
  auto task = pool.submit([&] {
    int calls = 0;
    Scope scope(pool);
    CHECK(scope.spawn([&](Scope::Context& ctx) {
      CHECK(ctx.spawn([&] { ++calls; }));
      ++calls;
    }));
    scope.join();
    CHECK(calls == 2);
  });
  pool.wait(task);
  task.rethrow_if_failed();

  Scope self(pool);
  self.spawn([&] { self.join(); });
  bool rejected = false;
  try { self.join(); }
  catch (const std::logic_error&) { rejected = true; }
  CHECK(rejected);

  Scope ancestor(pool);
  ancestor.spawn([&] {
    Scope child(pool);
    child.spawn([&] { ancestor.wait(); });
    child.join();
  });
  rejected = false;
  try { ancestor.join(); }
  catch (const std::logic_error&) { rejected = true; }
  CHECK(rejected);
}

void destructor_self_join(dagflow::Pool& pool) {
  struct Cleanup {
    Scope* scope;
    std::atomic<int>* rejected;
    ~Cleanup() {
      try { scope->wait(); }
      catch (const std::logic_error&) { ++*rejected; }
    }
  };
  Scope scope(pool);
  std::atomic<int> rejected{0};
  scope.spawn([cleanup = std::make_unique<Cleanup>(&scope, &rejected)] {});
  scope.join();
  CHECK(rejected == 1);
}

void publisher_keeps_completion_pending(dagflow::Pool& pool) {
  struct SlowCopy {
    std::latch* entered;
    std::latch* release;
    std::atomic<int>* called;
    bool fail;
    SlowCopy(std::latch& e, std::latch& r, std::atomic<int>& c, bool f)
        : entered(&e), release(&r), called(&c), fail(f) {}
    SlowCopy(const SlowCopy& other)
        : entered(other.entered), release(other.release), called(other.called), fail(other.fail) {
      entered->count_down();
      release->wait();
      if (fail) throw std::runtime_error("construction");
    }
    SlowCopy(SlowCopy&&) noexcept = default;
    void operator()() { ++*called; }
  };
  for (bool fail : {false, true}) {
    Scope scope(pool);
    std::latch entered(1), release(1);
    std::atomic<int> called{0};
    SlowCopy function(entered, release, called, fail);
    std::thread publisher([&] {
      bool failed = false;
      try { CHECK(scope.spawn(function)); }
      catch (const std::runtime_error&) { failed = true; }
      CHECK(failed == fail);
    });
    entered.wait();
    auto result = scope.close();
    CHECK(!result.ready());
    release.count_down();
    scope.wait();
    publisher.join();
    CHECK(result.ready() && called == (fail ? 0 : 1));
    CHECK(scope.cancelled() == fail);
    CHECK(static_cast<bool>(scope.last_error()) == fail);
  }
}

void child_reservations_survive_close_and_cancel(dagflow::Pool& pool) {
  struct Child {
    std::latch *copying, *release;
    std::atomic<int> *called, *destroyed;
    std::unique_ptr<Payload> ownership;
    Child(std::latch& c, std::latch& r, std::atomic<int>& n, std::atomic<int>& d)
        : copying(&c), release(&r), called(&n), destroyed(&d) {}
    Child(const Child& other)
        : copying(other.copying), release(other.release), called(other.called),
          destroyed(other.destroyed) {
      copying->count_down();
      release->wait();
      ownership = std::make_unique<Payload>(destroyed);
    }
    Child(Child&&) noexcept = default;
    void operator()() { ++*called; }
  };
  const auto parents = pool.thread_count();
  for (bool cancel : {false, true}) {
    Scope scope(pool);
    std::latch entered(parents), allow_children(1), copying(parents), release(1);
    std::atomic<int> called{0}, destroyed{0};
    for (unsigned i = 0; i < parents; ++i) {
      CHECK(scope.spawn([&](Scope::Context& ctx) {
        entered.count_down();
        allow_children.wait();
        Child child(copying, release, called, destroyed);
        CHECK(ctx.spawn(child));
        if (cancel) CHECK(!ctx.spawn(child));  // Rejection cannot copy the callable.
      }));
    }
    entered.wait();
    auto result = scope.close();
    allow_children.count_down();  // All child admissions occur after close.
    copying.wait();              // Publisher credits now protect every child.
    if (cancel) scope.cancel();
    CHECK(!result.ready());
    release.count_down();
    scope.join();
    CHECK(result.ready() && destroyed == static_cast<int>(parents));
    CHECK(called == (cancel ? 0 : static_cast<int>(parents)));
  }
}

void simultaneous_publish_close_cancel(dagflow::Pool& pool) {
  for (unsigned round = 0; round < 32; ++round) {
    Scope scope(pool);
    std::atomic<unsigned> accepted{0}, called{0};
    std::atomic<int> destroyed{0};
    std::barrier start(7);
    std::vector<std::thread> threads;
    for (unsigned producer = 0; producer < 4; ++producer) {
      threads.emplace_back([&] {
        start.arrive_and_wait();
        for (unsigned i = 0; i < 64; ++i)
          if (scope.spawn([&, own = std::make_unique<Payload>(&destroyed)] { ++called; }))
            ++accepted;
      });
    }
    threads.emplace_back([&] { start.arrive_and_wait(); scope.close(); });
    threads.emplace_back([&] { start.arrive_and_wait(); scope.cancel(); });
    start.arrive_and_wait();
    scope.join();
    for (auto& thread : threads) thread.join();
    CHECK(scope.completion().ready() && scope.cancelled());
    CHECK(!scope.spawn([] {}) && !scope.last_error());
    CHECK(called <= accepted && destroyed == 4 * 64);
  }
}

int main() {
  dagflow::Handle survivor;
  for (int workers : {1, 4}) {
    dagflow::Config config;
    config.threads = workers;
    config.pin_threads = false;
    dagflow::Pool pool(config);
    empty_and_closed(pool);
    descendants_after_close(pool);
    destructor_drains(pool);
    submit_close_race(pool);
    destructor_self_join(pool);
    publisher_keeps_completion_pending(pool);
    child_reservations_survive_close_and_cancel(pool);
    simultaneous_publish_close_cancel(pool);
    if (workers == 1) {
      cancellation_skips_queued(pool);
      cooperative_join_and_self_join(pool);
    } else {
      failure_and_first_error(pool);
    }
    Scope scope(pool);
    survivor = scope.completion();
    scope.spawn([] { throw std::runtime_error("survives owners"); });
  }
  CHECK(survivor.ready());
  bool failed = false;
  try { survivor.rethrow_if_failed(); }
  catch (const std::runtime_error&) { failed = true; }
  CHECK(failed);
}
