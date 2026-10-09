#include <atomic>
#include <cstdlib>
#include <new>

#include <dagflow/dagflow.hpp>
#include "support.hpp"

// Inject a single allocation failure only on the submitting thread. Subsequent
// error reporting and cleanup must still be able to allocate.
thread_local int allocation_budget = -1;
thread_local bool fail_aligned_allocation = false;
thread_local bool track_allocations = false;
thread_local std::size_t largest_allocation = 0;
void* operator new(std::size_t size) {
  if (track_allocations && size > largest_allocation) largest_allocation = size;
  if (allocation_budget == 0) {
    allocation_budget = -1;
    throw std::bad_alloc();
  }
  if (allocation_budget > 0) --allocation_budget;
  if (void* memory = std::malloc(size ? size : 1)) return memory;
  throw std::bad_alloc();
}
void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void* memory) noexcept { std::free(memory); }
void operator delete[](void* memory) noexcept { std::free(memory); }
void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }
void operator delete[](void* memory, std::size_t) noexcept {
  std::free(memory);
}
// Producer registration is cache-aligned and therefore bypasses ordinary new.
// Keep this injection separate so existing ordinary-allocation budgets retain
// their meaning; a rejected prepared packet must still retire its child credit.
void* operator new(std::size_t size, std::align_val_t alignment) {
  if (fail_aligned_allocation) {
    fail_aligned_allocation = false;
    allocation_budget = -1;
    throw std::bad_alloc();
  }
  void* memory = nullptr;
  if (posix_memalign(&memory, static_cast<std::size_t>(alignment), size ? size : 1))
    throw std::bad_alloc();
  return memory;
}
void* operator new[](std::size_t size, std::align_val_t alignment) {
  return ::operator new(size, alignment);
}
void operator delete(void* memory, std::align_val_t) noexcept { std::free(memory); }
void operator delete[](void* memory, std::align_val_t) noexcept { std::free(memory); }
void operator delete(void* memory, std::size_t, std::align_val_t) noexcept { std::free(memory); }
void operator delete[](void* memory, std::size_t, std::align_val_t) noexcept { std::free(memory); }

void graph_seal_failures(dagflow::Pool& pool) {
  int failures = 0;
  for (int fail_at = 0; fail_at < 9; ++fail_at) {
    dagflow::TaskGraph graph(pool);
    int observed = 0, joined = 0;
    auto a = graph.emplace(
        [count = std::make_unique<int>(0), &observed] { observed = ++*count; });
    auto b = graph.emplace([&] { joined = observed; });
    graph.add_edge(a, b);
    CHECK(graph.seal());
    pool.wait(graph.run());
    CHECK(joined == 1);
    int tail = 0;
    auto c = graph.emplace([owned = std::make_unique<int>(7), &tail, &joined] {
      tail = *owned + joined;
    });
    graph.add_edge(b, c);
    allocation_budget = fail_at;
    try {
      CHECK(graph.seal());
    } catch (const std::bad_alloc&) {
      ++failures;
    }
    allocation_budget = -1;
    CHECK(graph.seal());
    pool.wait(graph.run());
    CHECK(observed == 2 && joined == 2 && tail == 9);
  }
  // Covers cycle-validation scratch, compiled arrays and growing run storage.
  CHECK(failures > 0 && failures < 9);
}

void graph_builder_failures(dagflow::Pool& pool) {
  for (int fail_at = 0; fail_at < 3; ++fail_at) {
    dagflow::TaskGraph graph(pool);
    int calls = 0;
    graph.emplace([&] { ++calls; });
    CHECK(graph.seal());
    auto owned = std::make_shared<int>(42);
    std::weak_ptr<int> weak = owned;
    allocation_budget = fail_at;
    bool failed = false;
    try {
      graph.emplace([state = std::move(owned)] { CHECK(*state == 42); });
    } catch (const std::bad_alloc&) {
      failed = true;
    }
    allocation_budget = -1;
    CHECK(graph.size() == (failed ? 1 : 2));
    CHECK(weak.expired() == failed);
    pool.wait(graph.run());
    CHECK(calls == 1);
    graph.clear();
    CHECK(weak.expired());
  }
}

void graph_reuses_runtime_storage(dagflow::Pool& pool) {
  dagflow::TaskGraph graph(pool);
  std::atomic<int> calls{0};
  auto previous = graph.emplace([&] { ++calls; });
  for (int i = 1; i < 1024; ++i) {
    auto next = graph.emplace([&] { ++calls; });
    graph.add_edge(previous, next);
    previous = next;
  }
  CHECK(graph.seal());
  allocation_budget = 0;
  CHECK(graph.seal());  // Already compiled: no allocations at all.
  allocation_budget = -1;
  for (int run = 0; run < 3; ++run) {
    largest_allocation = 0;
    track_allocations = true;
    auto handle = graph.run();
    track_allocations = false;
    pool.wait(handle);
    CHECK(calls == 1024 * (run + 1));
    // RunState, completion and task packets are small. NodeState[1024] and
    // compiled topology buffers would exceed this bound if allocated by run().
    CHECK(largest_allocation < 4096);
    graph.reset();
  }
}

void graph_continuation_failure(dagflow::Pool& pool) {
  // One worker makes the failure injection belong to the lane's continuation.
  // Exact-budget completion must not attempt an empty publication. One more
  // token really needs a continuation and must report its publication failure.
  for (std::size_t tokens : {64, 65}) {
    dagflow::TaskGraph graph(pool);
    int calls = 0;
    auto node = graph.emplace([&] {
      if (++calls == 64) allocation_budget = 0;
    });
    graph.set_tokens(node, tokens);
    auto result = graph.run();
    pool.wait(result);
    CHECK(calls == 64);
    CHECK(bool(graph.last_error()) == (tokens == 65));
    // Clear the worker-local injection before unrelated work. This packet is
    // allocated on the external thread, where no failure is armed.
    pool.wait(pool.submit([] { allocation_budget = -1; }));
    bool failed = false;
    try { result.rethrow_if_failed(); }
    catch (const std::bad_alloc&) { failed = true; }
    CHECK(failed == (tokens == 65));
  }
}

void scope_publication_failures(dagflow::Pool& pool) {
  struct Body {
    std::shared_ptr<int> payload;
    char large[256]{};
    void operator()() { CHECK(false); }
  };
  // The concrete callable now lives inline in the variable-sized task packet,
  // so publication has one allocation failure point instead of callable spill
  // plus packet allocation. Failure must still close admission, retain the
  // error, and release captures before the completion observer becomes ready.
  for (int fail_at : {0}) {
    dagflow::TaskScope scope(pool);
    auto payload = std::make_shared<int>(42);
    std::weak_ptr<int> weak = payload;
    auto result = scope.completion();
    allocation_budget = fail_at;
    bool failed = false;
    try { scope.spawn(Body{std::move(payload)}); }
    catch (const std::bad_alloc&) { failed = true; }
    allocation_budget = -1;
    CHECK(failed && scope.cancelled() && weak.expired());
    CHECK(result.ready() && scope.last_error());
    failed = false;
    try { scope.join(); }
    catch (const std::bad_alloc&) { failed = true; }
    CHECK(failed);
  }
  {
    dagflow::TaskScope scope(pool);
    scope.close();
    allocation_budget = 0;
    CHECK(!scope.spawn([] {}));
    CHECK(allocation_budget == 0);  // Rejection constructs/allocates nothing.
    allocation_budget = -1;
  }
  {
    dagflow::TaskScope scope(pool);
    std::atomic<bool> caught{false};
    CHECK(scope.spawn([&](dagflow::TaskScope::Context& ctx) {
      allocation_budget = 0;  // Injection on the worker, not the owner thread.
      try { ctx.spawn([] {}); }
      catch (const std::bad_alloc&) { caught = true; }
      allocation_budget = -1;
      CHECK(ctx.cancelled() && !ctx.spawn([] {}));
    }));
    scope.wait();
    CHECK(caught && scope.cancelled() && scope.last_error());
    bool failed = false;
    try { scope.join(); }
    catch (const std::bad_alloc&) { failed = true; }
    CHECK(failed);  // Catching spawn failure in a child doesn't erase it.
  }
}

void pool_construction_failures() {
  int failures = 0, successes = 0;
  dagflow::Config config;
  config.threads = 3;
  config.shards = 2;
  config.pin_threads = false;
  for (int at = 0; at < 16; ++at) {
    allocation_budget = at;
    try {
      dagflow::Pool pool(config);
      allocation_budget = -1;
      auto handle = pool.submit([] {});
      pool.wait(handle);
      handle.rethrow_if_failed();
      ++successes;
    } catch (const std::bad_alloc&) { ++failures; }
    allocation_budget = -1;
  }
  CHECK(failures > 0 && successes > 0);
}

void batch_publication_failures(dagflow::Pool& pool) {
  // Prime external registration so the budget counts packet allocations.
  pool.submit_detached([] {});
  pool.wait_idle();
  for (int fail_at : {0, 1, 63, 64, 70, 136, 137}) {
    std::atomic<unsigned> calls{0};
    auto payload = std::make_shared<int>(42);
    std::weak_ptr<int> weak = payload;
    auto make = [&] { return [&, payload] { CHECK(*payload == 42); ++calls; }; };
    std::vector<decltype(make())> jobs;
    jobs.reserve(137);
    for (int i = 0; i < 137; ++i) jobs.push_back(make());
    payload.reset();
    allocation_budget = fail_at;
    bool failed = false;
    try { pool.submit_batch_detached(std::span{jobs}); }
    catch (const std::bad_alloc&) { failed = true; }
    allocation_budget = -1;
    CHECK(failed == (fail_at < 137));
    jobs.clear();  // Also release the inputs not consumed before failure.
    pool.wait_idle();
    CHECK(calls == (failed ? unsigned(fail_at / 64 * 64) : 137));
    CHECK(weak.expired());
  }
}

int main() {
  pool_construction_failures();
  {
    dagflow::Config config;
    config.threads = 1;
    config.pin_threads = false;
    dagflow::Pool pool(config);
    graph_seal_failures(pool);
    graph_builder_failures(pool);
    graph_reuses_runtime_storage(pool);
    graph_continuation_failure(pool);
    scope_publication_failures(pool);
    batch_publication_failures(pool);
  }
  int synchronous_failures = 0, publication_failures = 0;
  for (int fail_at = 0; fail_at < 20; ++fail_at) {
    dagflow::Config config;
    config.threads = 1;
    config.pin_threads = false;
    dagflow::Pool pool(config);
    std::atomic<bool> started{false}, release{false};
    auto gate = pool.submit([&] {
      started.store(true, std::memory_order_release);
      while (!release.load(std::memory_order_acquire))
        std::this_thread::yield();
    });
    while (!started.load(std::memory_order_acquire)) std::this_thread::yield();
    dagflow::TaskGraph graph(pool);
    std::atomic<int> calls{0};
    for (int i = 0; i < 256; ++i) graph.emplace([&] { ++calls; });
    CHECK(graph.seal());
    dagflow::Handle result;
    // A fresh external producer also exercises registration failure after a
    // prepared graph packet has acquired its credit. Warm producers no longer
    // allocate once per root; injection must cover rejection at the real
    // publication boundary, not depend on packet allocation being present.
    std::thread submitter([&] {
      allocation_budget = fail_at;
      fail_aligned_allocation = fail_at == 2;
      try {
        result = graph.run();
      } catch (const std::bad_alloc&) {
        ++synchronous_failures;
      }
      allocation_budget = -1;
      fail_aligned_allocation = false;
    });
    submitter.join();
    release.store(true, std::memory_order_release);
    pool.wait(gate);
    pool.wait(result);
    pool.wait_idle();
    if (graph.last_error()) {
      ++publication_failures;
      bool observed = false;
      try {
        result.rethrow_if_failed();
      } catch (const std::bad_alloc&) {
        observed = true;
      }
      CHECK(observed);
    }
    calls = 0;
    result = graph.run();
    pool.wait(result);
    result.rethrow_if_failed();
    CHECK(calls == 256 && !graph.last_error());
  }
  CHECK(synchronous_failures > 0 && publication_failures > 0);
}
