#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <thread>
#include <vector>

#include "dagflow/detail/idle_accounting.hpp"
#include "support.hpp"
#include "dagflow/thread_pool.hpp"

using namespace std::chrono_literals;

void pending_descendant() {
  dagflow::detail::IdleAccounting accounting(1);
  accounting.publish_external();
  auto wait = std::async(std::launch::async, [&] { accounting.wait(); });
  CHECK(wait.wait_for(10ms) == std::future_status::timeout);
  accounting.publish_worker(0);
  accounting.retire(0);
  accounting.worker_idle();
  CHECK(wait.wait_for(10ms) == std::future_status::timeout);
  accounting.retire(0);
  accounting.worker_idle();
  CHECK(wait.wait_for(1s) == std::future_status::ready);
  wait.get();
}

void registration_and_multiple_waiters() {
  dagflow::Config config;
  config.threads = 4;
  config.pin_threads = false;
  config.idle_us_min = config.idle_us_max = 5'000'000;
  dagflow::Pool pool(config);
  std::atomic<bool> release{false};
  auto anchor = pool.submit([&] { release.wait(false); });
  std::array<std::future<void>, 3> waiters;
  for (auto& waiter : waiters)
    waiter = std::async(std::launch::async, [&] { pool.wait_idle(); });
  constexpr int producers = 12, tasks = 128;
  std::array<int, producers * tasks> values{};
  std::vector<std::thread> threads;
  for (int p = 0; p < producers; ++p)
    threads.emplace_back([&, p] {
      for (int i = 0; i < tasks; ++i)
        pool.submit_detached([&, slot = p * tasks + i] {
          pool.submit_detached([&, slot] { values[slot] = slot + 1; });
        });
    });
  for (auto& thread : threads) thread.join();
  for (auto& waiter : waiters)
    CHECK(waiter.wait_for(1ms) == std::future_status::timeout);
  release.store(true);
  release.notify_one();
  for (auto& waiter : waiters) {
    CHECK(waiter.wait_for(2s) == std::future_status::ready);
    waiter.get();
  }
  for (std::size_t i = 0; i < values.size(); ++i) CHECK(values[i] == int(i + 1));
}

void destructor_publication() {
  dagflow::Config config;
  config.threads = 2;
  config.pin_threads = false;
  dagflow::Pool pool(config);
  std::atomic<bool> child_started{false}, release{false};
  int result = 0;
  struct Payload {
    dagflow::Pool& pool;
    std::atomic<bool>& started;
    std::atomic<bool>& release;
    int& result;
    ~Payload() {
      pool.submit_detached([&started = started, &release = release, &result = result] {
        started.store(true);
        started.notify_one();
        release.wait(false);
        result = 42;
      });
    }
  };
  auto payload = std::make_unique<Payload>(pool, child_started, release, result);
  pool.submit_detached([payload = std::move(payload)] {});
  child_started.wait(false);
  auto waiter = std::async(std::launch::async, [&] { pool.wait_idle(); });
  CHECK(waiter.wait_for(10ms) == std::future_status::timeout);
  release.store(true);
  release.notify_one();
  waiter.get();
  CHECK(result == 42);
}

void reused_address_and_cache_eviction() {
  dagflow::Config config;
  config.threads = 1;
  config.pin_threads = false;
  alignas(dagflow::Pool) std::byte storage[sizeof(dagflow::Pool)];
  for (int i = 0; i < 40; ++i) {
    auto* pool = std::construct_at(reinterpret_cast<dagflow::Pool*>(storage), config);
    int result = 0;
    pool->submit_detached([&] { result = i + 1; });
    pool->wait_idle();
    CHECK(result == i + 1);
    std::destroy_at(pool);
  }
  std::array<std::unique_ptr<dagflow::Pool>, 8> pools;
  std::array<int, 8> values{};
  for (auto& pool : pools) pool = std::make_unique<dagflow::Pool>(config);
  for (int round = 0; round < 20; ++round) {
    for (std::size_t i = 0; i < pools.size(); ++i)
      pools[i]->submit_detached([&, i] { ++values[i]; });
    for (auto& pool : pools) pool->wait_idle();
  }
  for (auto value : values) CHECK(value == 20);
}

void retire_wait_race() {
  dagflow::Config config;
  config.threads = 2;
  config.pin_threads = false;
  config.idle_us_min = config.idle_us_max = 5'000'000;
  dagflow::Pool pool(config);
  for (unsigned round = 0; round < 3000; ++round) {
    int result = 0;
    pool.submit_detached([&] { result = 1; });
    const auto before = std::chrono::steady_clock::now();
    pool.wait_idle();
    CHECK(result == 1);
    CHECK(std::chrono::steady_clock::now() - before < 2s);
  }
}

void handoff(dagflow::Pool& pool, unsigned remaining,
             std::atomic<bool>& entered, std::atomic<bool>& release) {
  if (remaining) {
    pool.submit_detached(
        [&pool, remaining, &entered, &release] {
          handoff(pool, remaining - 1, entered, release);
        },
        {.affinity = remaining % 2});
  } else {
    entered.store(true);
    entered.notify_one();
    release.wait(false);
  }
}

void moving_work_cannot_look_idle() {
  dagflow::Config config;
  config.threads = 2;
  config.pin_threads = false;
  dagflow::Pool pool(config);
  for (unsigned round = 0; round < 30; ++round) {
    std::atomic<bool> entered{false}, release{false};
    // There is always a published descendant, but no long-lived root task
    // anchoring accounting while ownership moves between worker lanes.
    pool.submit_detached([&] { handoff(pool, 1000, entered, release); });
    auto waiter = std::async(std::launch::async, [&] { pool.wait_idle(); });
    entered.wait(false);
    CHECK(waiter.wait_for(1ms) == std::future_status::timeout);
    release.store(true);
    release.notify_one();
    CHECK(waiter.wait_for(1s) == std::future_status::ready);
    waiter.get();
  }
}
int main() {
  pending_descendant();
  registration_and_multiple_waiters();
  destructor_publication();
  reused_address_and_cache_eviction();
  retire_wait_race();
  moving_work_cannot_look_idle();
}
