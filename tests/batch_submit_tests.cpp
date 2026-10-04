#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#include "support.hpp"
#include "dagflow/thread_pool.hpp"

using namespace std::chrono_literals;

dagflow::Config config(unsigned workers = 1) {
  dagflow::Config cfg;
  cfg.threads = workers;
  cfg.shards = workers + 1;  // Includes an empty domain requiring recruitment.
  cfg.pin_threads = false;
  cfg.idle_us_min = cfg.idle_us_max = 5'000'000;
  return cfg;
}

void move_only_and_descendants() {
  dagflow::Pool pool(config(4));
  std::atomic<unsigned> calls{0};
  auto make = [&] {
    return [&, owned = std::make_unique<int>(7)] {
      CHECK(owned && *owned == 7);
      auto child = pool.submit([&] { ++calls; });
      pool.wait(child);
      child.rethrow_if_failed();
      pool.submit_detached([&] { ++calls; });
    };
  };
  std::vector<decltype(make())> jobs;
  pool.submit_batch_detached(std::span{jobs});
  for (unsigned i = 0; i < 137; ++i) jobs.push_back(make());
  pool.submit_batch_detached(std::span{jobs}, {.priority = dagflow::Priority::High});
  pool.wait_idle();
  CHECK(calls == 274);
}

void progress_and_idle() {
  dagflow::Pool pool(config());
  for (unsigned round = 0; round < 100; ++round) {
    std::promise<void> ran;
    auto ready = ran.get_future();
    std::array jobs{[&] { ran.set_value(); }};
    pool.submit_batch_detached(std::span{jobs});
    CHECK(ready.wait_for(2s) == std::future_status::ready);
    pool.wait_idle();
  }
  std::atomic<bool> entered{false}, release{false};
  std::array jobs{[&] {
    entered.store(true, std::memory_order_release);
    entered.notify_one();
    release.wait(false, std::memory_order_acquire);
  }};
  pool.submit_batch_detached(std::span{jobs});
  entered.wait(false, std::memory_order_acquire);
  auto idle = std::async(std::launch::async, [&] { pool.wait_idle(); });
  CHECK(idle.wait_for(20ms) == std::future_status::timeout);
  release.store(true, std::memory_order_release);
  release.notify_one();
  CHECK(idle.wait_for(2s) == std::future_status::ready);
}

void worker_overflow_and_cross_pool() {
  dagflow::Pool first(config()), second(config());
  std::vector<unsigned> seen(DAGFLOW_LOCAL_QUEUE_CAPACITY + DAGFLOW_CENTRAL_QUEUE_CAPACITY + 65);
  auto parent = first.submit([&] {
    auto make = [&](std::size_t i) { return [&, i] { ++seen[i]; }; };
    std::vector<decltype(make(0))> jobs;
    jobs.reserve(seen.size());
    for (std::size_t i = 0; i < seen.size(); ++i) jobs.push_back(make(i));
    first.submit_batch_detached(std::span{jobs});
    std::promise<void> ran;
    auto ready = ran.get_future();
    std::array cross{[&] { ran.set_value(); }};
    second.submit_batch_detached(std::span{cross});
    CHECK(ready.wait_for(2s) == std::future_status::ready);
    second.wait_idle();
  });
  first.wait(parent);
  parent.rethrow_if_failed();
  first.wait_idle();
  second.wait_idle();
  for (auto n : seen) CHECK(n == 1);
}

void concurrent_producers() {
  dagflow::Pool pool(config(4));
  std::vector<std::atomic<unsigned>> seen(8192);
  std::vector<std::thread> producers;
  for (unsigned p = 0; p < 4; ++p) producers.emplace_back([&, p] {
    auto make = [&](unsigned i) { return [&, i] { ++seen[i]; }; };
    std::vector<decltype(make(0))> jobs;
    for (unsigned i = p; i < seen.size(); i += 4) jobs.push_back(make(i));
    pool.submit_batch_detached(std::span{jobs}, {.affinity = 0});
  });
  for (auto& producer : producers) producer.join();
  pool.wait_idle();
  for (const auto& n : seen) CHECK(n == 1);
}

void backpressure() {
  auto cfg = config();
  cfg.shards = 1;
  dagflow::Pool pool(cfg);
  std::atomic<bool> entered{false}, release{false};
  auto gate = pool.submit([&] {
    entered.store(true, std::memory_order_release);
    entered.notify_one();
    release.wait(false, std::memory_order_acquire);
  });
  entered.wait(false, std::memory_order_acquire);
  std::atomic<unsigned> calls{0};
  auto task = [&] { ++calls; };
  std::vector<decltype(task)> jobs(DAGFLOW_CENTRAL_QUEUE_CAPACITY + 65, task);
  auto producer = std::async(std::launch::async, [&] {
    pool.submit_batch_detached(std::span{jobs});
  });
  CHECK(producer.wait_for(20ms) == std::future_status::timeout);
  release.store(true, std::memory_order_release);
  release.notify_one();
  CHECK(producer.wait_for(2s) == std::future_status::ready);
  producer.get();
  pool.wait(gate);
  pool.wait_idle();
  CHECK(calls == jobs.size());
}

struct ThrowingMove {
  unsigned id;
  std::atomic<unsigned>* calls;
  ThrowingMove(unsigned i, std::atomic<unsigned>& c) : id(i), calls(&c) {}
  ThrowingMove(ThrowingMove&& other) : id(other.id), calls(other.calls) {
    if (id == 70) throw std::runtime_error("construction failure");
  }
  void operator()() { ++*calls; }
};

void partial_failure_and_task_exceptions() {
  dagflow::Pool pool(config());
  std::atomic<unsigned> calls{0};
  std::vector<ThrowingMove> jobs;
  jobs.reserve(100);
  for (unsigned i = 0; i < 100; ++i) jobs.emplace_back(i, calls);
  bool threw = false;
  try { pool.submit_batch_detached(std::span{jobs}); }
  catch (const std::runtime_error&) { threw = true; }
  CHECK(threw);
  pool.wait_idle();
  CHECK(calls == 64);  // The failed group's prepared packets were never accepted.
  std::array failures{[&] { ++calls; throw std::runtime_error("detached"); }};
  pool.submit_batch_detached(std::span{failures});
  pool.wait_idle();
  CHECK(calls == 65);
}

int main() {
  move_only_and_descendants();
  progress_and_idle();
  worker_overflow_and_cross_pool();
  concurrent_producers();
  backpressure();
  partial_failure_and_task_exceptions();
}
