#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstring>
#include <iostream>
#include <latch>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#include <dagflow/dagflow.hpp>
#include <dagflow/detail/idle_accounting.hpp>
#include <dagflow/detail/parking_lot.hpp>
#include <dagflow/detail/small_function.hpp>
#include <dagflow/detail/small_vector.hpp>
#include "support.hpp"

namespace {
using namespace dagflow;
using namespace dagflow::detail;
using Clock = std::chrono::steady_clock;
unsigned workers = 4;
constexpr unsigned items = 32768;

template <class F>
void parallel(F fn) {
  std::vector<std::thread> threads;
  for (unsigned id = 0; id < workers; ++id)
    threads.emplace_back([&, id] { fn(id); });
  for (auto& thread : threads) thread.join();
}
Config config() {
  Config cfg;
  cfg.threads = workers;
  cfg.shards = workers + 1;  // Include a domain without a resident worker.
  cfg.pin_threads = false;
  return cfg;
}
void memory_containers() {
  constexpr unsigned count = 1024;
  constexpr std::array<std::size_t, 4> sizes{24, 96, 256, 1025};
  constexpr std::array<std::size_t, 4> alignments{8, 16, 64, 256};
  std::vector<void*> blocks(workers * count);
  parallel([&](unsigned id) {
    for (unsigned i = 0; i < count; ++i) {
      auto* ptr = allocate_bytes(sizes[i % 4], alignments[i % 4]);
      CHECK(reinterpret_cast<std::uintptr_t>(ptr) % alignments[i % 4] == 0);
      std::memset(ptr, (id + i) % 251, sizes[i % 4]);
      blocks[id * count + i] = ptr;
    }
    for (unsigned round = 0; round < 256; ++round) {
      small_vector<unsigned, 4> values;
      for (unsigned i = 0; i < 35; ++i) values.emplace_back(i + id);
      small_function<unsigned(), 16> fn(
          [v = std::move(values)] { return v[34]; });
      auto moved = std::move(fn);
      CHECK(!fn && moved() == 34 + id);
    }
  });
  // Every allocating thread has exited before another thread frees its blocks.
  parallel([&](unsigned id) {
    const auto owner = (id + 1) % workers;
    for (unsigned i = 0; i < count; ++i) {
      auto* ptr = static_cast<unsigned char*>(blocks[owner * count + i]);
      CHECK(ptr[0] == (owner + i) % 251);
      CHECK(ptr[sizes[i % 4] - 1] == ptr[0]);
      deallocate_bytes(ptr, alignments[i % 4]);
    }
  });
}
void ring() {
  ring_mpmc<unsigned, 1024> queue;
  std::vector<std::atomic<unsigned>> seen(items);
  std::atomic<unsigned> consumed{0};
  std::barrier start(workers + 1);
  std::vector<std::thread> consumers;
  for (unsigned id = 0; id < workers; ++id)
    consumers.emplace_back([&] {
      start.arrive_and_wait();
      while (consumed.load(std::memory_order_relaxed) < items) {
        unsigned value;
        if (queue.try_pop(value)) {
          CHECK(value < items && seen[value].fetch_add(1) == 0);
          consumed.fetch_add(1, std::memory_order_relaxed);
        } else
          std::this_thread::yield();
      }
    });
  start.arrive_and_wait();
  parallel([&](unsigned id) {
    for (unsigned i = id; i < items; i += workers)
      while (!queue.try_push(i)) std::this_thread::yield();
  });
  for (auto& thread : consumers) thread.join();
  for (auto& count : seen) CHECK(count == 1);
}
void deque() {
  chase_lev_deque<unsigned, 64> queue;
  std::vector<std::atomic<unsigned>> seen(items);
  std::atomic<unsigned> consumed{0};
  auto consume = [&](unsigned value) {
    CHECK(value < items && seen[value].fetch_add(1) == 0);
    consumed.fetch_add(1, std::memory_order_relaxed);
  };
  std::vector<std::thread> thieves;
  for (unsigned id = 1; id < workers; ++id)
    thieves.emplace_back([&] {
      while (consumed.load(std::memory_order_relaxed) < items) {
        unsigned value;
        if (queue.try_steal(value))
          consume(value);
        else
          std::this_thread::yield();
      }
    });
  for (unsigned i = 0; i < items; ++i) {
    unsigned value;
    while (!queue.try_push(i))
      if (queue.try_pop(value)) consume(value);
    if (i % 3 == 0 && queue.try_pop(value)) consume(value);
  }
  while (consumed.load(std::memory_order_relaxed) < items) {
    unsigned value;
    if (queue.try_pop(value))
      consume(value);
    else
      std::this_thread::yield();
  }
  for (auto& thread : thieves) thread.join();
  for (auto& count : seen) CHECK(count == 1);
}
void scheduler() {
  Scheduler scheduler(workers, workers + 1, 32);
  auto packets = make_owned_array<ScheduledTask>(items);
  std::vector<std::atomic<unsigned>> seen(items);
  // Preload both sources and priorities, including the workerless domain.
  for (unsigned i = 0; i < items; ++i) {
    packets[i].prio = i % 2 ? Priority::High : Priority::Normal;
    if (i % 3 == 0)
      scheduler.submit_overflow(i % (workers + 1), &packets[i]);
    else
      CHECK(scheduler.try_submit_external(i % (workers + 1), &packets[i]));
  }
  std::atomic<unsigned> consumed{0};
  parallel([&](unsigned id) {
    while (consumed.load(std::memory_order_relaxed) < items) {
      if (auto* task = scheduler.try_acquire(id)) {
        const auto index = task - packets.get();
        CHECK(index >= 0 && index < items && seen[index].fetch_add(1) == 0);
        consumed.fetch_add(1, std::memory_order_relaxed);
      } else
        std::this_thread::yield();
    }
  });
  for (auto& count : seen) CHECK(count == 1);
}
void completion_accounting() {
  auto root = CompletionCredit::create();
  auto handle = root.handle();
  auto credits = make_owned_array<CompletionCredit>(workers);
  for (unsigned id = 0; id < workers; ++id) credits[id] = root.fork();
  root.finish();
  IdleAccounting accounting(workers);
  std::barrier published(workers + 1);
  std::vector<std::thread> threads;
  for (unsigned id = 0; id < workers; ++id)
    threads.emplace_back([&, id] {
      for (unsigned i = 0; i < 1024; ++i) {
        accounting.publish_worker(id);
        accounting.publish_external();
      }
      published.arrive_and_wait();
      for (unsigned i = 0; i < 2048; ++i) accounting.retire(id);
      credits[id].finish();
      accounting.worker_idle();
    });
  published.arrive_and_wait();
  accounting.wait();
  for (auto& thread : threads) thread.join();
  CHECK(handle.ready());
}
void parking() {
  Scheduler scheduler(workers, workers + 1, 32);
  ParkingLot parking(scheduler);
  std::atomic<bool> stop{false};
  std::barrier phase(workers + 1);
  std::vector<std::thread> threads;
  for (unsigned id = 0; id < workers; ++id)
    threads.emplace_back([&, id] {
      for (unsigned round = 0; round < 32; ++round) {
        const auto epoch = parking.prepare(id);
        phase.arrive_and_wait();
        const auto start = Clock::now();
        parking.wait(id, epoch, 5'000'000, stop);
        CHECK(Clock::now() - start < std::chrono::seconds(4));
        phase.arrive_and_wait();
      }
    });
  for (unsigned round = 0; round < 32; ++round) {
    phase.arrive_and_wait();
    parking.wake_one(workers);  // Empty domain must recruit elsewhere.
    for (unsigned id = 1; id < workers; ++id)
      parking.wake_one(scheduler.home_shard(id));
    phase.arrive_and_wait();
  }
  for (auto& thread : threads) thread.join();
}
void api() {
  Pool pool(config());
  std::vector<unsigned> values(items);
  auto first = pool.for_each(values, [](unsigned& v) { ++v; });
  pool.wait_and_rethrow(first);
  auto second = pool.for_each_ws(values, [](unsigned& v) { ++v; }, {}, 64);
  pool.wait_and_rethrow(pool.combine({first, second}));
  for (auto value : values) CHECK(value == 2);
  std::atomic<unsigned> calls{0};
  {
    TaskScope scope(pool);
    for (unsigned i = 0; i < 256; ++i)
      CHECK(scope.spawn([&](TaskScope::Context& ctx) {
        CHECK(ctx.spawn([&] { ++calls; }));
      }));
    scope.join();
  }
  CHECK(calls == 256);
  GraphScope graph(pool);
  auto root = graph.emplace([] {});
  std::vector<JobHandle> leaves;
  for (unsigned i = 0; i < 512; ++i) {
    auto leaf = graph.then(root, [&] { ++calls; });
    leaves.push_back(leaf);
  }
  graph.when_all(leaves, [&] { CHECK(calls == 256 + 512); });
  graph.run_and_wait();
  pool.wait_idle();
  auto failure =
      pool.submit([] { throw std::runtime_error("user exception"); });
  bool caught = false;
  try {
    pool.wait_and_rethrow(failure);
  } catch (const std::runtime_error&) {
    caught = true;
  }
  CHECK(caught);
}
void api_shutdown_overflow() {
  constexpr unsigned children =
      DAGFLOW_LOCAL_QUEUE_CAPACITY + DAGFLOW_CENTRAL_QUEUE_CAPACITY + 64;
  std::vector<std::atomic<unsigned>> seen(workers * children);
  std::latch entered(workers), release(1);
  std::barrier filled(workers);
  Pool pool(config());
  std::vector<Handle> roots;
  for (unsigned id = 0; id < workers; ++id)
    roots.push_back(pool.submit([&, id] {
      entered.count_down();
      release.wait();
      for (unsigned i = 0; i < children; ++i)
        pool.submit_detached([&, index = id * children + i] { ++seen[index]; });
      // All workers keep publishing before any can drain; submit must not
      // recurse or wait for capacity. shutdown concurrently waits for these
      // descendants.
      filled.arrive_and_wait();
    }));
  entered.wait();
  pool.close();
  release.count_down();
  pool.shutdown();
  for (auto& root : roots) {
    CHECK(root.ready());
    root.rethrow_if_failed();
  }
  for (auto& count : seen) CHECK(count == 1);
  bool rejected = false;
  try {
    pool.submit_detached([] {});
  } catch (const std::logic_error&) {
    rejected = true;
  }
  CHECK(rejected);
  pool.shutdown();
}
}  // namespace
int main(int argc, char** argv) {
  if (argc > 1) workers = std::stoul(argv[1]);
  CHECK(workers && workers <= 256);
  const std::array cases{
      std::pair{"memory_containers", &memory_containers},
      std::pair{"mpmc", &ring},
      std::pair{"chase_lev", &deque},
      std::pair{"scheduler", &scheduler},
      std::pair{"completion_accounting", &completion_accounting},
      std::pair{"parking", &parking},
      std::pair{"public_api", &api},
      std::pair{"shutdown_overflow_api", &api_shutdown_overflow}};
  for (auto [name, run] : cases) {
    const auto start = Clock::now();
    run();
    std::cout << "{\"status\":\"ok\",\"layer\":\"" << name
              << "\",\"workers\":" << workers << ",\"validation_ms\":"
              << std::chrono::duration<double, std::milli>(Clock::now() - start)
                     .count()
              << "}" << std::endl;
  }
}
