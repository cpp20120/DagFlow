#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <dagflow/detail/scheduler.hpp>
#include <memory>
#include <thread>
#include <vector>

#include "support.hpp"

// Each worker owns exactly one Scheduler worker ID. Multiple producers race
// against four independent owners for central ingress and overflow packets.
// The oracle observes custody only: every published packet must be acquired
// by exactly one worker, regardless of ordering or which worker steals it.
static void contested_ingress(unsigned n, unsigned distribution) {
  constexpr unsigned workers = 4;
  dagflow::detail::Scheduler scheduler(workers, 2, 4);
  constexpr unsigned per_owner = 2;
  const unsigned total = n + workers * per_owner;
  auto tasks = std::make_unique<dagflow::detail::ScheduledTask[]>(total);
  auto visits = std::make_unique<std::atomic<unsigned>[]>(total);
  std::atomic<unsigned> completed{0}, producers_done{0}, duplicates{0};
  std::atomic<unsigned> stolen_from_first_owner{0};
  std::barrier start(workers + 2);
  std::barrier local_ready(workers);
  std::vector<std::thread> threads;
  threads.reserve(workers + 2);
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(10);

  for (unsigned worker = 0; worker < workers; ++worker) {
    threads.emplace_back([&, worker] {
      start.arrive_and_wait();
      // Only the owner publishes to its Chase-Lev deque. Work is ready for
      // theft before owner zero starts its own acquisition loop.
      for (unsigned j = 0; j < per_owner; ++j) {
        auto& task = tasks[n + worker * per_owner + j];
        task.prio = j ? dagflow::Priority::Normal : dagflow::Priority::High;
        if (!scheduler.try_submit_local(worker, &task))
          scheduler.submit_overflow(scheduler.home_shard(worker), &task);
      }
      local_ready.arrive_and_wait();
      if (worker == 0) {
        // Force theft of the two owner-zero packets rather than trusting a
        // lucky sampling of the scheduler's randomized victim selection.
        while ((visits[n].load() == 0 || visits[n + 1].load() == 0) &&
               std::chrono::steady_clock::now() < deadline)
          std::this_thread::yield();
      }
      while (completed.load(std::memory_order_acquire) < total &&
             std::chrono::steady_clock::now() < deadline) {
        // Competing workers steal from independent owner queues; each caller
        // maintains one stable worker ID for the entire history.
        auto* task = scheduler.try_acquire(worker);
        if (!task) {
          std::this_thread::yield();
          continue;
        }
        auto index = static_cast<unsigned>(task - tasks.get());
        if (index >= total || visits[index].fetch_add(1) != 0)
          duplicates.fetch_add(1);
        if (worker != 0 && index >= n && index < n + per_owner)
          stolen_from_first_owner.fetch_add(1);
        completed.fetch_add(1, std::memory_order_release);
      }
    });
  }
  for (unsigned producer = 0; producer < 2; ++producer) {
    threads.emplace_back([&, producer] {
      start.arrive_and_wait();
      for (unsigned i = producer; i < n; i += 2) {
        auto& task = tasks[i];
        task.prio = (i & 1) ? dagflow::Priority::High
                            : dagflow::Priority::Normal;
        const unsigned shard = (i + distribution) % 2;
        if ((i + distribution) % 3 == 0 ||
            !scheduler.try_submit_external(shard, &task))
          scheduler.submit_overflow(shard, &task);
        if ((i & 7) == 0) std::this_thread::yield();
      }
      producers_done.fetch_add(1, std::memory_order_release);
    });
  }
  for (auto& thread : threads) thread.join();
  CHECK(producers_done.load() == 2);
  CHECK(completed.load() == total);
  CHECK(stolen_from_first_owner.load() == per_owner);
  CHECK(duplicates.load() == 0);
  for (unsigned i = 0; i < total; ++i) CHECK(visits[i].load() == 1);
  // All workers can perform an empty final scan without resurrecting a task.
  for (unsigned i = 0; i < workers; ++i)
    CHECK(scheduler.try_acquire(i) == nullptr);
}

int main() {
  for (unsigned n : {63u, 64u, 65u, 127u, 128u, 257u, 513u})
    for (unsigned round = 0; round < 5; ++round)
      contested_ingress(n, round);
}
