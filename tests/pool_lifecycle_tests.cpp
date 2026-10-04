#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include "dagflow/dagflow.hpp"
#include "support.hpp"

using namespace std::chrono_literals;

dagflow::Config config(unsigned workers = 1) {
  dagflow::Config cfg;
  cfg.threads = workers;
  cfg.shards = workers;
  cfg.pin_threads = false;
  cfg.idle_us_min = cfg.idle_us_max = 5'000'000;
  return cfg;
}

template <class F>
void rejects(F&& call) {
  bool caught = false;
  try {
    call();
  } catch (const std::logic_error&) {
    caught = true;
  }
  CHECK(caught);
}

void signal(std::atomic<bool>& flag) {
  flag.store(true, std::memory_order_release);
  flag.notify_all();
}

// Moving the capture transfers responsibility; temporary destruction cannot
// publish. The final capture destructor runs inside the parent's retirement.
struct SpawnOnDestroy {
  dagflow::Pool* pool;
  std::atomic<unsigned>* calls;
  SpawnOnDestroy(dagflow::Pool& p, std::atomic<unsigned>& c)
      : pool(&p), calls(&c) {}
  SpawnOnDestroy(SpawnOnDestroy&& other) noexcept
      : pool(std::exchange(other.pool, nullptr)), calls(other.calls) {}
  ~SpawnOnDestroy() {
    if (pool) pool->submit_detached([count = calls] { ++*count; });
  }
  void operator()() { ++*calls; }
};

void destructor_drains() {
  std::atomic<unsigned> calls{0};
  dagflow::Handle failure;
  {
    dagflow::Pool pool(config(4));
    for (unsigned i = 0; i < 1024; ++i)
      pool.submit_detached([&] {
        while (!pool.closed()) std::this_thread::yield();
        ++calls;
        pool.submit_detached(SpawnOnDestroy(pool, calls));
      });
    failure = pool.submit(
        [] { throw std::runtime_error("retained after shutdown"); });
    // No wait_idle and no handle wait: destruction owns the drain.
  }
  CHECK(calls == 3 * 1024);
  CHECK(failure.ready());
  bool caught = false;
  try {
    failure.rethrow_if_failed();
  } catch (const std::runtime_error&) {
    caught = true;
  }
  CHECK(caught);
}

void overflow_does_not_recurse() {
  dagflow::Pool pool(config());
  unsigned depth = 0, calls = 0;
  struct Chain {
    dagflow::Pool& pool;
    unsigned& depth;
    unsigned& calls;
    unsigned remaining;
    void operator()() {
      CHECK(++depth == 1);
      ++calls;
      if (remaining > 1)
        pool.submit_detached(Chain{pool, depth, calls, remaining - 1});
      --depth;
    }
  };
  constexpr unsigned chain_length = 50'000;
  auto root = pool.submit([&] {
    CHECK(++depth == 1);
    for (unsigned i = 0;
         i < DAGFLOW_LOCAL_QUEUE_CAPACITY + DAGFLOW_CENTRAL_QUEUE_CAPACITY; ++i)
      pool.submit_detached([] {});
    pool.submit_detached(Chain{pool, depth, calls, chain_length});
    CHECK(calls == 0);  // The root's queues are full and it is the only worker.
    --depth;
  });
  pool.shutdown();
  root.rethrow_if_failed();
  CHECK(calls == chain_length && depth == 0);
}

void close_and_descendants() {
  dagflow::Pool pool(config());
  CHECK(!pool.closed());
  std::atomic<bool> entered{false}, release{false};
  unsigned calls = 0;
  auto parent = pool.submit([&] {
    signal(entered);
    release.wait(false, std::memory_order_acquire);
    rejects([&] { pool.shutdown(); });  // Must not wait for shutdown_mutex_.
    pool.close();                       // Worker-side close is allowed.
    auto child = pool.submit([&] {
      ++calls;
      pool.submit_detached([&] { ++calls; });
    });
    pool.wait_and_rethrow(child);
  });
  entered.wait(false, std::memory_order_acquire);
  pool.close();
  CHECK(pool.closed());
  rejects([&] { pool.submit([] {}); });
  rejects([&] { pool.submit_detached([] {}); });
  std::array jobs{[] {}};
  rejects([&] { pool.submit_batch_detached(std::span{jobs}); });
  // Two simultaneous shutdowns are both barriers and must serialize joining.
  auto first = std::async(std::launch::async, [&] { pool.shutdown(); });
  auto second = std::async(std::launch::async, [&] { pool.shutdown(); });
  CHECK(first.wait_for(20ms) == std::future_status::timeout);
  CHECK(second.wait_for(20ms) == std::future_status::timeout);
  signal(release);
  first.get();
  second.get();
  CHECK(parent.ready() && calls == 2);
  parent.rethrow_if_failed();
  pool.shutdown();
  pool.wait_idle();
  rejects([&] { pool.submit([] {}); });
}

void close_races_producers() {
  for (unsigned round = 0; round < 20; ++round) {
    dagflow::Pool pool(config(4));
    std::atomic<unsigned> accepted{0}, executed{0}, started{0};
    std::vector<std::thread> producers;
    for (unsigned p = 0; p < 4; ++p)
      producers.emplace_back([&] {
        // Make closure race live producers, not just their thread startup.
        pool.submit_detached([&] { ++executed; });
        ++accepted;
        ++started;
        started.notify_one();
        for (;;) {
          try {
            pool.submit_detached([&] { ++executed; });
          } catch (const std::logic_error&) {
            break;
          }
          ++accepted;
        }
      });
    auto count = started.load();
    while (count != 4) {
      started.wait(count);
      count = started.load();
    }
    pool.shutdown();
    for (auto& producer : producers) producer.join();
    CHECK(accepted == executed);
  }
}

void shutdown_during_backpressure() {
  for (bool batch : {false, true}) {
    dagflow::Pool pool(config());
    std::atomic<bool> entered{false}, release{false}, publishing{false};
    std::atomic<unsigned> calls{0};
    pool.submit_detached([&] {
      signal(entered);
      release.wait(false, std::memory_order_acquire);
      // Descendants must remain legal while a publisher is blocked on capacity.
      pool.submit_detached([&] { ++calls; });
    });
    entered.wait(false, std::memory_order_acquire);
    for (unsigned i = 0; i < DAGFLOW_CENTRAL_QUEUE_CAPACITY; ++i)
      pool.submit_detached([&] { ++calls; });
    auto producer = std::async(std::launch::async, [&] {
      std::array jobs{[&] { ++calls; }};
      signal(publishing);
      try {
        if (batch)
          pool.submit_batch_detached(std::span{jobs});
        else
          pool.submit_detached(jobs[0]);
        return 1u;
      } catch (const std::logic_error&) {
        return 0u;
      }
    });
    publishing.wait(false, std::memory_order_acquire);
    CHECK(producer.wait_for(20ms) == std::future_status::timeout);
    auto shutdown = std::async(std::launch::async, [&] { pool.shutdown(); });
    CHECK(shutdown.wait_for(20ms) == std::future_status::timeout);
    signal(release);
    const auto accepted = producer.get();
    shutdown.get();
    CHECK(calls == DAGFLOW_CENTRAL_QUEUE_CAPACITY + 1 + accepted);
  }
}

void shutdown_waits_for_capture_destruction() {
  dagflow::Pool pool(config());
  std::atomic<bool> destroying{false}, release{false};
  struct Capture {
    std::atomic<bool>& entered;
    std::atomic<bool>& release;
    ~Capture() {
      signal(entered);
      release.wait(false, std::memory_order_acquire);
    }
  };
  auto capture = std::unique_ptr<Capture>(new Capture{destroying, release});
  auto handle = pool.submit([owned = std::move(capture)] {});
  destroying.wait(false, std::memory_order_acquire);
  auto shutdown = std::async(std::launch::async, [&] { pool.shutdown(); });
  CHECK(!handle.ready());
  CHECK(shutdown.wait_for(20ms) == std::future_status::timeout);
  signal(release);
  shutdown.get();
  CHECK(handle.ready());
}

void overflow_visible_to_other_worker(dagflow::Priority priority) {
  auto cfg = config(2);
  cfg.shards = 3;  // Spill into a domain with no resident worker.
  cfg.worker_shards = {0, 1};
  dagflow::Pool pool(cfg);
  std::atomic<bool> owner_entered{false}, thief_entered{false}, build{false};
  std::atomic<bool> prepared{false}, steal{false}, release_owner{false};
  dagflow::Handle child, dependent;
  std::atomic<unsigned> calls{0};
  auto owner = pool.submit([&] {
    signal(owner_entered);
    build.wait(false, std::memory_order_acquire);
    // Enqueue distributes over all three shards, filling each priority ring.
    const dagflow::SubmitOptions opt{.priority = priority,
                                     .mode = dagflow::SubmissionMode::Enqueue};
    for (unsigned i = 0; i < 3 * DAGFLOW_CENTRAL_QUEUE_CAPACITY; ++i)
      pool.submit_detached([&] { ++calls; }, opt);
    dependent = pool.submit([&] { pool.wait_and_rethrow(child); }, opt);
    child = pool.submit([&] { ++calls; }, opt);
    signal(prepared);
    release_owner.wait(false, std::memory_order_acquire);
  });
  owner_entered.wait(false, std::memory_order_acquire);
  auto blocker = pool.submit([&] {
    signal(thief_entered);
    steal.wait(false, std::memory_order_acquire);
  });
  thief_entered.wait(false, std::memory_order_acquire);
  signal(build);
  prepared.wait(false, std::memory_order_acquire);
  signal(steal);
  pool.wait_and_rethrow(
      dependent);  // Owner is blocked: only the thief can help.
  signal(release_owner);
  pool.shutdown();
  CHECK(owner.ready() && blocker.ready() && child.ready());
  CHECK(calls == 3 * DAGFLOW_CENTRAL_QUEUE_CAPACITY + 1);
}

void graph_and_scope_after_close() {
  dagflow::Pool pool(config());
  unsigned calls = 0;
  // Both kinds of structured descendants are created from already accepted
  // work.
  auto parent = pool.submit([&] {
    pool.close();
    dagflow::TaskScope scope(pool);
    CHECK(scope.spawn([&](dagflow::TaskScope::Context& ctx) {
      CHECK(ctx.spawn([&] { ++calls; }));
    }));
    scope.join();
    dagflow::TaskGraph graph(pool);
    constexpr unsigned width =
        DAGFLOW_LOCAL_QUEUE_CAPACITY + DAGFLOW_CENTRAL_QUEUE_CAPACITY + 65;
    for (unsigned i = 0; i < width; ++i) graph.emplace([&] { ++calls; });
    // Graph-owned initial packets also participate in intrusive overflow and
    // must be safe to reuse immediately after the result becomes ready.
    for (unsigned run = 0; run < 2; ++run) pool.wait_and_rethrow(graph.run());
    CHECK(calls == 1 + 2 * width);
  });
  pool.shutdown();
  CHECK(parent.ready());
  parent.rethrow_if_failed();

  dagflow::TaskScope rejected_scope(pool);
  rejects([&] { rejected_scope.spawn([] {}); });
  CHECK(rejected_scope.completion().ready() && rejected_scope.cancelled());
  dagflow::TaskGraph rejected_graph(pool);
  rejected_graph.emplace([] {});
  auto result = rejected_graph.run();
  CHECK(result.ready());
  rejects([&] { result.rethrow_if_failed(); });
}

void cross_pool_is_external() {
  dagflow::Pool first(config()), second(config());
  second.close();
  first.wait_and_rethrow(
      first.submit([&] { rejects([&] { second.submit([] {}); }); }));
}

void close_between_batch_groups() {
  dagflow::Pool pool(config());
  std::atomic<unsigned> calls{0};
  struct Job {
    dagflow::Pool& pool;
    std::atomic<unsigned>& calls;
    unsigned index;
    Job(dagflow::Pool& p, std::atomic<unsigned>& c, unsigned i)
        : pool(p), calls(c), index(i) {}
    Job(Job&& other) noexcept
        : pool(other.pool), calls(other.calls), index(other.index) {
      if (index == 64) pool.close();
    }
    void operator()() { ++calls; }
  };
  std::vector<Job> jobs;
  jobs.reserve(128);
  for (unsigned i = 0; i < 128; ++i) jobs.emplace_back(pool, calls, i);
  rejects([&] { pool.submit_batch_detached(std::span{jobs}); });
  pool.shutdown();
  CHECK(calls == 64);  // Only the first group obtained external admission.
}

int main() {
  destructor_drains();
  overflow_does_not_recurse();
  close_and_descendants();
  close_races_producers();
  shutdown_during_backpressure();
  shutdown_waits_for_capture_destruction();
  overflow_visible_to_other_worker(dagflow::Priority::Normal);
  overflow_visible_to_other_worker(dagflow::Priority::High);
  graph_and_scope_after_close();
  cross_pool_is_external();
  close_between_batch_groups();
}
