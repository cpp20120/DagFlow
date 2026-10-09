#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <future>
#include <stdexcept>
#include <thread>
#include <vector>

#include <dagflow/detail/parking_lot.hpp>
#include <dagflow/detail/scheduler.hpp>
#include "support.hpp"
#include <dagflow/task_scope.hpp>

using dagflow::detail::ParkingLot;
using dagflow::detail::ScheduledTask;
using dagflow::detail::Scheduler;

void membership_and_stealing() {
  const std::array<uint32_t, 4> map{1, 0, 1, 0};
  Scheduler scheduler(4, 3, 32, map);
  CHECK(scheduler.members(0).size() == 2 && scheduler.members(2).empty());
  CHECK(scheduler.members(0)[0] == 1 && scheduler.members(0)[1] == 3);
  for (uint32_t id = 0; id < 4; ++id) {
    CHECK(scheduler.home_shard(id) == map[id]);
    CHECK(scheduler.select_shard(id) == map[id]);
  }
  CHECK(scheduler.select_shard(UINT32_MAX) == map[UINT32_MAX % 4]);
  ScheduledTask local_peer, remote;
  // Even remote high priority doesn't supersede this locality tier.
  remote.prio = dagflow::Priority::High;
  CHECK(scheduler.try_submit_local(2, &local_peer));
  CHECK(scheduler.try_submit_local(1, &remote));
  CHECK(scheduler.try_acquire(0) == &local_peer);
  CHECK(scheduler.try_acquire(0) == &remote);
  CHECK(scheduler.try_submit_external(2, &remote));  // No resident worker.
  CHECK(scheduler.try_acquire(0) == &remote);
  CHECK(scheduler.try_acquire(0) == nullptr);
  Scheduler balanced(5, 2, 32);
  CHECK(balanced.members(0).size() == 3 && balanced.members(1).size() == 2);
  bool rejected = false;
  try {
    Scheduler bad(3, 2, 1, map);
  } catch (const std::invalid_argument&) {
    rejected = true;
  }
  CHECK(rejected);
  rejected = false;
  try {
    Scheduler bad(4, 1, 1, map);
  } catch (const std::invalid_argument&) {
    rejected = true;
  }
  CHECK(rejected);
}

void central_batch_visible() {
  Scheduler scheduler(1, 1, 3);
  ScheduledTask first, second, third;
  CHECK(scheduler.try_submit_external(0, &first));
  CHECK(scheduler.try_submit_external(0, &second));
  CHECK(scheduler.try_submit_external(0, &third));
  CHECK(scheduler.try_acquire(0) == &first);
  CHECK(scheduler.has_local_work(0));
  CHECK(scheduler.try_acquire(0) == &third);
  CHECK(scheduler.try_acquire(0) == &second);
}

void acquisition_publication() {
  Scheduler scheduler(2, 1, 3);
  ScheduledTask tasks[5];
  bool published = true;
  CHECK(!scheduler.try_acquire(0, published) && !published);
  CHECK(scheduler.try_submit_external(0, &tasks[0]));
  CHECK(scheduler.try_acquire(0, published) == &tasks[0] && published);
  for (auto& task : tasks) CHECK(scheduler.try_submit_external(0, &task));
  CHECK(scheduler.try_acquire(0, published) == &tasks[0] && published);
  CHECK(scheduler.try_acquire(0, published) == &tasks[2] && !published);
  CHECK(scheduler.try_acquire(0, published) == &tasks[1] && !published);
  CHECK(scheduler.try_acquire(0, published) == &tasks[3] && published);
  CHECK(scheduler.try_acquire(0, published) == &tasks[4] && !published);
  for (auto& task : tasks) CHECK(scheduler.try_submit_local(0, &task));
  CHECK(scheduler.try_acquire(1, published) == &tasks[0] && published);
  CHECK(scheduler.try_acquire(1, published) == &tasks[3] && !published);
  CHECK(scheduler.try_acquire(1, published) == &tasks[2] && !published);
  CHECK(scheduler.try_acquire(1, published) == &tasks[1] && !published);
  CHECK(scheduler.try_acquire(1, published) == &tasks[4] && published);
  Scheduler polling(1, 1, 32);
  for (unsigned i = 0; i < 31; ++i) {
    CHECK(polling.try_submit_local(0, &tasks[0]));
    CHECK(polling.try_acquire(0, published) == &tasks[0] && !published);
  }
  CHECK(polling.try_submit_external(0, &tasks[0]));
  // The fairness probe takes one ingress task without staging local siblings.
  // It must still relay recruitment; other queues may have available work.
  CHECK(polling.try_acquire(0, published) == &tasks[0] && published);
}

void expect_signal(ParkingLot& parking, uint32_t worker, uint64_t epoch);

void transferred_batch_wakes_waiter() {
  Scheduler scheduler(2, 1, 32);
  ParkingLot parking(scheduler);
  ScheduledTask tasks[4];
  for (auto& task : tasks) CHECK(scheduler.try_submit_external(0, &task));
  const auto epoch = parking.prepare(1);
  bool published = false;
  CHECK(scheduler.try_acquire(0, published) == &tasks[0] && published);
  parking.wake_one(0);
  expect_signal(parking, 1, epoch);
  CHECK(scheduler.try_acquire(1, published) == &tasks[1] && published);
  CHECK(scheduler.try_acquire(1) == &tasks[3]);
  CHECK(scheduler.try_acquire(1) == &tasks[2]);
}

void batch_recruits_sleeping_workers() {
  for (uint32_t workers : {2u, 4u, 8u}) {
    dagflow::Config cfg;
    cfg.threads = workers;
    cfg.shards = 1;
    cfg.pin_threads = false;
    cfg.idle_us_min = cfg.idle_us_max = 5'000'000;
    dagflow::Pool pool(cfg);
    for (unsigned round = 0; round < 6; ++round) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      std::barrier together(workers);
      auto body = [&] { together.arrive_and_wait(); };
      std::vector<decltype(body)> jobs(workers, body);
      // The external batch emits one wake. Every body must run at once:
      // transfers must recruit sleepers even beyond the four-task steal size.
      const auto priority = round % 2 ? dagflow::Priority::High
                                     : dagflow::Priority::Normal;
      pool.submit_batch_detached(std::span{jobs}, {.priority = priority});
      auto done = std::async(std::launch::async, [&] { pool.wait_idle(); });
      CHECK(done.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
      done.get();  // Cannot rely on the five-second parking timeout for progress.
    }
  }
}

void expect_signal(ParkingLot& parking, uint32_t worker, uint64_t epoch) {
  std::atomic<bool> stop{false};
  auto waiter = std::async(std::launch::async, [&] {
    parking.wait(worker, epoch, 5'000'000, stop);
  });
  CHECK(waiter.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
  waiter.get();  // Notification before entering wait must remain observable.
}

void parking_membership() {
  Scheduler topology(130, 2, 32);
  ParkingLot parking(topology);
  const auto first = parking.prepare(64);    // Second mask word in domain zero.
  const auto second = parking.prepare(129);  // Second mask word in domain one.
  parking.wake_one(0);
  expect_signal(parking, 64, first);
  parking.wake_one(1);
  expect_signal(parking, 129, second);
  const std::array<uint32_t, 2> map{4, 4};
  Scheduler sparse(2, 5, 32, map);
  ParkingLot sparse_parking(sparse);
  const auto epoch = sparse_parking.prepare(1);
  sparse_parking.wake_one(
      0);  // Empty injection domain must wake a remote worker.
  expect_signal(sparse_parking, 1, epoch);
}

void parking_publication_race() {
  Scheduler topology(1, 1, 32);
  ParkingLot parking(topology);
  std::atomic<unsigned> ready{0}, published{0};
  std::atomic<bool> stop{false};
  std::thread worker([&] {
    for (unsigned round = 1; round <= 2000; ++round) {
      do {
        const auto epoch = parking.prepare(0);
        ready.store(round, std::memory_order_release);
        ready.notify_one();
        if (published.load(std::memory_order_acquire) < round)
          parking.wait(0, epoch, 5'000'000, stop);
        else
          parking.cancel(0);
        // A previous publisher's delayed signal is a valid spurious wake.
      } while (published.load(std::memory_order_acquire) < round);
    }
  });
  for (unsigned round = 1; round <= 2000; ++round) {
    auto value = ready.load(std::memory_order_acquire);
    while (value < round) {
      ready.wait(value, std::memory_order_acquire);
      value = ready.load(std::memory_order_acquire);
    }
    published.store(round, std::memory_order_release);
    parking.wake_one(0);
  }
  worker.join();
}

void packed_domain_boundaries() {
  // Domain boundaries share words; the second domain spans two partial words.
  Scheduler topology(130, 3, 32);
  ParkingLot parking(topology);
  const std::array<uint32_t, 9> workers{43, 44, 63, 64, 86, 87, 127, 128, 129};
  std::array<uint64_t, workers.size()> epochs{};
  for (std::size_t i = 0; i < workers.size(); ++i)
    epochs[i] = parking.prepare(workers[i]);
  for (std::size_t i = 1; i < workers.size(); ++i) {
    parking.wake_one(topology.home_shard(workers[i]));
    expect_signal(parking, workers[i], epochs[i]);
  }
  parking.wake_one(0);
  expect_signal(parking, workers[0], epochs[0]);

  const std::array<uint32_t, 4> map{1, 0, 1, 0};
  Scheduler interleaved(4, 3, 32, map);
  ParkingLot compact(interleaved);
  const auto remote = compact.prepare(0);
  const auto local = compact.prepare(3);
  compact.wake_one(0);  // A packed bit is a CSR position, not a worker ID.
  expect_signal(compact, 3, local);
  compact.wake_one(2);  // Empty domain recruits the remaining remote waiter.
  expect_signal(compact, 0, remote);
}

void queue_publication_race(bool local) {
  Scheduler topology(2, 1, 32);
  ParkingLot parking(topology);
  std::atomic<unsigned> start{0}, consumed{0};
  std::atomic<bool> stop{false};
  ScheduledTask task;
  auto publish = [&] {
    CHECK(local ? topology.try_submit_local(0, &task)
                : topology.try_submit_external(0, &task));
  };
  // Publication before registration: no persistent epoch is necessary if the
  // final acquisition sees work. Repeat across a complete register/cancel cycle.
  for (unsigned i = 0; i < 10; ++i) {
    publish();
    parking.wake_one(0);
    (void)parking.prepare(1);
    CHECK(topology.try_acquire(1) == &task);
    parking.cancel(1);
  }
  std::thread worker([&] {
    for (unsigned round = 1; round <= 5000; ++round) {
      auto observed = start.load(std::memory_order_acquire);
      while (observed < round) {
        start.wait(observed, std::memory_order_acquire);
        observed = start.load(std::memory_order_acquire);
      }
      for (;;) {
        const auto epoch = parking.prepare(1);
        if (auto* acquired = topology.try_acquire(1)) {
          CHECK(acquired == &task);
          parking.cancel(1);
          break;
        }
        const auto before = std::chrono::steady_clock::now();
        parking.wait(1, epoch, 5'000'000, stop);
        CHECK(std::chrono::steady_clock::now() - before <
              std::chrono::seconds(4));  // Progress must not depend on timeout.
      }
      consumed.store(round, std::memory_order_release);
      consumed.notify_one();
    }
  });
  for (unsigned round = 1; round <= 5000; ++round) {
    // Deliberately no synchronization from publication to waiter registration:
    // either the final queue scan or the notifying publisher must win the race.
    start.store(round, std::memory_order_release);
    start.notify_one();
    if (round % 2) std::this_thread::yield();
    publish();
    parking.wake_one(0);
    auto observed = consumed.load(std::memory_order_acquire);
    while (observed < round) {
      consumed.wait(observed, std::memory_order_acquire);
      observed = consumed.load(std::memory_order_acquire);
    }
  }
  worker.join();
}

void pool_domains() {
  dagflow::Config config;
  config.threads = 4;
  config.shards = 3;
  config.worker_shards = {1, 0, 1, 0};
  config.pin_threads = false;
  config.idle_us_min = config.idle_us_max = 1'000'000;
  dagflow::Pool pool(config);
  for (unsigned round = 0; round < 60; ++round) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
    std::atomic<int> called{0};
    dagflow::TaskScope scope(pool);
    for (uint32_t i = 0; i < 16; ++i) {
      CHECK(scope.spawn(
          [&, i](dagflow::TaskScope::Context& ctx) {
            ++called;
            CHECK(ctx.spawn([&] { ++called; }, {.affinity = (i + 1) % 4}));
          },
          {.affinity = i % 4}));
    }
    scope.join();
    CHECK(called == 32);
  }
}

void overflow_batch_visible() {
  for (auto priority : {dagflow::Priority::High, dagflow::Priority::Normal}) {
    dagflow::detail::Scheduler scheduler(1, 1, 1024);
    dagflow::detail::ScheduledTask first, child;
    first.prio = child.prio = priority;
    scheduler.submit_overflow(0, &first);
    scheduler.submit_overflow(0, &child);
    bool recruit = false;
    CHECK(scheduler.try_acquire(0, recruit) == &first && recruit);
    CHECK(scheduler.has_local_work(0));  // No hidden siblings across user code.
    CHECK(scheduler.try_acquire(0, recruit) == &child);
    CHECK(!scheduler.try_acquire(0));
    CHECK(!first.overflow_next && !child.overflow_next);
  }
}

void shared_sources_receive_service() {
  for (auto priority : {dagflow::Priority::High, dagflow::Priority::Normal}) {
    dagflow::detail::Scheduler scheduler(1, 2, 1);
    dagflow::detail::ScheduledTask local, ingress, overflow, remote;
    local.prio = ingress.prio = overflow.prio = remote.prio = priority;
    scheduler.submit_overflow(1, &remote);
    CHECK(scheduler.try_acquire(0) == &remote);  // Seed drain preference to ingress.
    scheduler.submit_overflow(1, &remote);
    scheduler.submit_overflow(0, &overflow);
    CHECK(scheduler.try_submit_external(0, &ingress));
    CHECK(scheduler.try_submit_local(0, &local));
    unsigned ingress_calls = 0, overflow_calls = 0, remote_calls = 0;
    for (unsigned attempt = 0; attempt < 256; ++attempt) {
      auto* task = scheduler.try_acquire(0);
      CHECK(task);
      if (task == &local) {
        CHECK(scheduler.try_submit_local(0, task));
      } else if (task == &ingress) {
        ++ingress_calls;
        CHECK(scheduler.try_submit_external(0, task));
      } else if (task == &overflow) {
        ++overflow_calls;
        scheduler.submit_overflow(0, task);
      } else {
        CHECK(task == &remote);
        ++remote_calls;
        scheduler.submit_overflow(1, task);
      }
    }
    CHECK(ingress_calls && overflow_calls && remote_calls);
  }
}

int main() {
  overflow_batch_visible();
  shared_sources_receive_service();
  membership_and_stealing();
  central_batch_visible();
  acquisition_publication();
  transferred_batch_wakes_waiter();
  batch_recruits_sleeping_workers();
  parking_membership();
  parking_publication_race();
  packed_domain_boundaries();
  queue_publication_race(false);
  queue_publication_race(true);
  pool_domains();
}
