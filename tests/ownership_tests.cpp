#include <atomic>
#include <barrier>
#include <chrono>
#include <future>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <dagflow/dagflow.hpp>
#include "support.hpp"

using Credit = dagflow::detail::CompletionCredit;
static_assert(!std::is_copy_constructible_v<Credit>);
static_assert(std::is_nothrow_move_constructible_v<Credit>);
static_assert(std::is_copy_constructible_v<dagflow::Handle>);

struct Payload {
  std::atomic<int>* destroyed;
  ~Payload() { ++*destroyed; }
};

void credit_transfer_and_payload() {
  std::atomic<int> destroyed{0};
  auto owner = dagflow::detail::make_owned<Payload>(&destroyed);
  auto publisher = Credit::create(std::move(owner));
  auto handle = publisher.handle();
  auto copy = handle;
  auto accepted = publisher.fork();
  auto moved = std::move(accepted);
  CHECK(!accepted && moved && !handle.ready());
  publisher.finish();
  CHECK(!handle.ready() && destroyed == 0);
  std::thread worker([credit = std::move(moved)]() mutable { credit.finish(); });
  worker.join();
  CHECK(handle.ready() && copy.ready() && destroyed == 1);
  handle = {};
  copy.rethrow_if_failed();  // Result survives operation payload destruction.
}

void dropped_observers_and_rollback() {
  std::atomic<int> destroyed{0};
  auto root = Credit::create(dagflow::detail::make_owned<Payload>(&destroyed));
  auto observer = root.handle();
  { auto unpublished = root.fork(); }  // Failed publication retires only child.
  CHECK(!observer.ready());
  auto packet = root.fork();
  observer = {};
  root.finish();
  CHECK(destroyed == 0);
  packet.finish();
  CHECK(destroyed == 1);  // No observer is needed to keep in-flight work alive.
}

void ordinary_completion_ignores_cold_mutex() {
  auto* state = dagflow::detail::make_owned<dagflow::detail::CompletionState>().release();
  state->retain();  // Inspector keeps storage alive after the final credit.
  std::unique_lock lock(state->mutex);
  auto finisher = std::async(std::launch::async, [state] {
    dagflow::detail::CompletionState::retire(state);
  });
  CHECK(finisher.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  finisher.get();
  state->wait();
  CHECK(state->ready.load() && !state->get_error());
  lock.unlock();
  state->release();
}

void dependent_registration_races_completion() {
  dagflow::Pool pool({.threads = 1, .pin_threads = false});
  for (unsigned round = 0; round < 64; ++round) {
    auto source = Credit::create();
    auto observed = source.handle();
    const bool fail = round % 2;
    if (fail) source.fail(std::make_exception_ptr(std::runtime_error("source")));
    std::barrier start(5);
    std::vector<dagflow::Handle> joined(32);
    std::vector<std::thread> threads;
    for (unsigned t = 0; t < 4; ++t) {
      threads.emplace_back([&, t] {
        start.arrive_and_wait();
        for (unsigned i = t; i < joined.size(); i += 4)
          joined[i] = pool.combine({observed});
      });
    }
    start.arrive_and_wait();
    source.finish();
    for (auto& thread : threads) thread.join();
    joined.push_back(pool.combine({observed}));  // Always exercises late registration.
    for (const auto& result : joined) {
      pool.wait(result);
      CHECK(result.ready());
      bool failed = false;
      try { result.rethrow_if_failed(); }
      catch (const std::runtime_error&) { failed = true; }
      CHECK(failed == fail);
    }
  }
  auto source = Credit::create();
  auto tail = source.handle();
  for (unsigned i = 0; i < 16384; ++i) tail = pool.combine({tail});
  CHECK(!tail.ready());
  source.finish();  // Terminal propagation must stay iterative.
  pool.wait(tail);
  CHECK(tail.ready());
}

void graph_definition_can_die_after_ready() {
  dagflow::Config config;
  config.threads = 4;
  config.pin_threads = false;
  dagflow::Pool pool(config);
  for (int attempt = 0; attempt < 100; ++attempt) {
    dagflow::Handle old;
    {
      dagflow::TaskGraph graph(pool);
      auto previous = graph.emplace([] {});
      for (int i = 0; i < 257; ++i) {
        auto next = graph.emplace([] {});
        graph.add_edge(previous, next);
        previous = next;
      }
      old = graph.run();
      pool.wait(old);
      graph.clear(); // No pool.wait_idle barrier: old epilogues may still run.
      graph.emplace([] {});
      pool.wait(graph.run());
    }
    CHECK(old.ready());
    old.rethrow_if_failed();
  }
}

void graph_repeated_wide_runs_and_old_results() {
  dagflow::Config config;
  config.threads = 4;
  config.pin_threads = false;
  dagflow::Pool pool(config);
  dagflow::TaskGraph graph(pool);
  std::vector<int> values(1024);
  std::vector<dagflow::Handle> history;
  for (std::size_t i = 0; i < values.size(); ++i)
    graph.emplace([&, i] { ++values[i]; });
  for (int repeat = 1; repeat <= 30; ++repeat) {
    auto done = graph.run();
    pool.wait(done);  // Deliberately no wait_idle before storage reuse.
    done.rethrow_if_failed();
    history.push_back(done);
    for (int value : values) CHECK(value == repeat);
    graph.reset();
  }
  graph.clear();
  for (const auto& old : history) {
    CHECK(old.ready());
    old.rethrow_if_failed();
  }
  bool fail = true;
  graph.emplace([&] { if (fail) throw std::runtime_error("old failure"); });
  auto failed = graph.run();
  pool.wait(failed);
  graph.reset();
  CHECK(!graph.last_error());
  fail = false;
  auto success = graph.run();
  pool.wait(success);
  success.rethrow_if_failed();
  graph.clear();
  bool retained_error = false;
  try { failed.rethrow_if_failed(); }
  catch (const std::runtime_error&) { retained_error = true; }
  CHECK(retained_error && failed.ready() && success.ready());
}

void range_payload_released_before_result() {
  dagflow::Config config;
  config.threads = 4;
  config.pin_threads = false;
  dagflow::Pool pool(config);
  std::atomic<int> destroyed{0};
  std::vector<int> values(100000);
  for (bool steal : {false, true}) {
    auto callable = [payload = dagflow::detail::make_owned<Payload>(&destroyed)](int& n) {
      ++n;
    };
    auto handle = steal
        ? pool.for_each_ws(values.begin(), values.end(), std::move(callable), {}, 64)
        : pool.for_each(values.begin(), values.end(), std::move(callable));
    pool.wait(handle);
    handle.rethrow_if_failed();
    CHECK(destroyed == (steal ? 2 : 1));
  }
}

void spilled_callable_ownership() {
  struct alignas(128) Body {
    dagflow::detail::OwnedObject<Payload> payload;
    int* calls;
    const void** address;
    void operator()() {
      CHECK(reinterpret_cast<std::uintptr_t>(this) % alignof(Body) == 0);
      ++*calls;
      *address = this;
    }
    void operator()(int& value) { operator()(); ++value; }
  };
  dagflow::Config config;
  config.threads = 2;
  config.pin_threads = false;
  dagflow::Pool pool(config);
  std::atomic<int> destroyed{0};
  int calls = 0;
  const void* address = nullptr;
  auto handle = pool.submit(Body{
      dagflow::detail::make_owned<Payload>(&destroyed), &calls, &address});
  pool.wait(handle);
  CHECK(calls == 1 && destroyed == 1);  // Spill destroyed before ready().
  dagflow::TaskGraph graph(pool);
  auto node = graph.emplace(Body{
      dagflow::detail::make_owned<Payload>(&destroyed), &calls, &address});
  pool.wait(graph.run());
  const void* first_address = address;
  CHECK(calls == 2 && destroyed == 1);
  graph.set_tokens(node, 2);  // Recompile and move the wrapper, not the body.
  pool.wait(graph.run());
  CHECK(calls == 4 && address == first_address && destroyed == 1);
  graph.clear();
  CHECK(destroyed == 2);

  pool.submit_detached(Body{
      dagflow::detail::make_owned<Payload>(&destroyed), &calls, &address});
  pool.wait_idle();
  CHECK(calls == 5 && destroyed == 3);

  // Exercise every by-reference callable entry point with a move-only,
  // over-aligned target, including both iterator and range overloads.
  std::vector<int> values{0};
  pool.wait_and_rethrow(pool.for_each(values.begin(), values.end(), Body{
      dagflow::detail::make_owned<Payload>(&destroyed), &calls, &address}));
  pool.wait_and_rethrow(pool.for_each(values, Body{
      dagflow::detail::make_owned<Payload>(&destroyed), &calls, &address}));
  pool.wait_and_rethrow(pool.for_each_ws(values.begin(), values.end(), Body{
      dagflow::detail::make_owned<Payload>(&destroyed), &calls, &address}));
  pool.wait_and_rethrow(pool.for_each_ws(values, Body{
      dagflow::detail::make_owned<Payload>(&destroyed), &calls, &address}));
  CHECK(calls == 9 && destroyed == 7 && values[0] == 4);
}

void lvalue_callable_ownership() {
  struct alignas(128) Body {
    int* calls;
    int local_calls = 0;
    void operator()() {
      CHECK(reinterpret_cast<std::uintptr_t>(this) % alignof(Body) == 0);
      CHECK(++local_calls == 1);  // Each submission must own a fresh copy.
      ++*calls;
    }
    void operator()(int&) { operator()(); }
  };
  dagflow::Config config;
  config.threads = 1;
  config.pin_threads = false;
  dagflow::Pool pool(config);
  int calls = 0;
  auto exercise = [&](auto& body) {
    pool.wait_and_rethrow(pool.submit(body));
    pool.submit_detached(body);
    pool.wait_idle();
    std::vector<int> values{0};
    pool.wait_and_rethrow(pool.for_each(values.begin(), values.end(), body));
    pool.wait_and_rethrow(pool.for_each(values, body));
    pool.wait_and_rethrow(pool.for_each_ws(values.begin(), values.end(), body));
    pool.wait_and_rethrow(pool.for_each_ws(values, body));
    CHECK(body.local_calls == 0);
  };
  Body mutable_body{&calls};
  const Body const_body{&calls};
  exercise(mutable_body);
  exercise(const_body);
  CHECK(calls == 12);
}

int main() {
  credit_transfer_and_payload();
  dropped_observers_and_rollback();
  ordinary_completion_ignores_cold_mutex();
  dependent_registration_races_completion();
  graph_definition_can_die_after_ready();
  graph_repeated_wide_runs_and_old_results();
  range_payload_released_before_result();
  spilled_callable_ownership();
  lvalue_callable_ownership();
}
