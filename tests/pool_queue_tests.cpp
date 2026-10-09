#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <limits>
#include <stdexcept>
#include <thread>
#include <vector>

#include "support.hpp"
#include <dagflow/thread_pool.hpp>

void worker_overflow(dagflow::Priority priority) {
  constexpr std::size_t queued =
      DAGFLOW_LOCAL_QUEUE_CAPACITY + DAGFLOW_CENTRAL_QUEUE_CAPACITY;
  constexpr std::size_t extra = 32;
  std::vector<unsigned> seen(queued + extra);
  std::size_t completed = 0;
  dagflow::Config cfg;
  cfg.threads = 1;
  cfg.pin_threads = false;
  cfg.central_batch = std::numeric_limits<uint32_t>::max();
  dagflow::Pool pool(cfg);
  auto parent = pool.submit([&] {
    std::vector<dagflow::Handle> children;
    children.reserve(seen.size());
    for (std::size_t i = 0; i < seen.size(); ++i) {
      children.push_back(pool.submit(
          [&, i] {
            ++seen[i];
            ++completed;
          },
          {.priority = priority}));
    }
    // With one worker, the queues cannot drain until parent helps them.
    CHECK(completed == 0);  // Saturation never invokes a callable from submit.
    bool invoked = false;
    auto failed = pool.submit(
        [&] {
          invoked = true;
          throw std::runtime_error("overflow task exception");
        },
        {.priority = priority});
    CHECK(!invoked);
    pool.wait(failed);  // Helping can reach the shared overflow queue.
    CHECK(invoked);
    bool threw = false;
    try { failed.rethrow_if_failed(); }
    catch (const std::runtime_error&) { threw = true; }
    CHECK(threw);
    for (const auto& h : children) pool.wait(h);
    CHECK(completed == seen.size());
  });
  pool.wait(parent);
  for (auto n : seen) CHECK(n == 1);
}

void external_backpressure() {
  using namespace std::chrono_literals;
  std::atomic<bool> entered{false}, release{false};
  std::atomic<unsigned> completed{0};
  dagflow::Config cfg;
  cfg.threads = 1;
  cfg.pin_threads = false;
  cfg.central_batch = std::numeric_limits<uint32_t>::max();
  dagflow::Pool pool(cfg);
  auto blocker = pool.submit([&] {
    entered.store(true, std::memory_order_release);
    entered.notify_one();
    release.wait(false, std::memory_order_acquire);
  });
  entered.wait(false, std::memory_order_acquire);
  std::vector<dagflow::Handle> pending;
  pending.reserve(DAGFLOW_CENTRAL_QUEUE_CAPACITY);
  for (std::size_t i = 0; i < DAGFLOW_CENTRAL_QUEUE_CAPACITY; ++i)
    pending.push_back(pool.submit([&] { completed.fetch_add(1); }));

  std::atomic<bool> submitting{false};
  auto producer = std::async(std::launch::async, [&] {
    submitting.store(true, std::memory_order_release);
    submitting.notify_one();
    return pool.submit([&] { completed.fetch_add(1); });
  });
  submitting.wait(false, std::memory_order_acquire);
  CHECK(producer.wait_for(20ms) == std::future_status::timeout);
  release.store(true, std::memory_order_release);
  release.notify_one();
  auto last = producer.get();
  pool.wait(blocker);
  for (const auto& h : pending) pool.wait(h);
  pool.wait(last);
  CHECK(completed.load() == DAGFLOW_CENTRAL_QUEUE_CAPACITY + 1);
}

void cross_pool_submission() {
  dagflow::Config cfg;
  cfg.threads = 1;
  cfg.pin_threads = false;
  dagflow::Pool first(cfg), second(cfg);
  std::thread::id second_id;
  auto identify =
      second.submit([&] { second_id = std::this_thread::get_id(); });
  second.wait(identify);
  auto parent = first.submit([&] {
    const auto first_id = std::this_thread::get_id();
    auto child = second.submit([&] {
      CHECK(std::this_thread::get_id() == second_id);
      CHECK(std::this_thread::get_id() != first_id);
    });
    second.wait(child);
  });
  first.wait(parent);
  // A worker submitting into another pool must use that pool's producer lane,
  // while its own retirement still uses the worker lane of the first pool.
  first.wait_idle();
  second.wait_idle();
}

void concurrent_submission() {
  constexpr std::size_t count = 20000;
  std::vector<std::atomic<unsigned>> seen(count);
  dagflow::Config cfg;
  cfg.threads = 4;
  cfg.pin_threads = false;
  cfg.central_batch = 4096;
  dagflow::Pool pool(cfg);
  std::vector<std::thread> producers;
  for (std::size_t p = 0; p < 4; ++p)
    producers.emplace_back([&, p] {
      std::vector<dagflow::Handle> handles;
      handles.reserve(count / 4);
      for (std::size_t i = p; i < count; i += 4)
        handles.push_back(pool.submit([&, i] { seen[i].fetch_add(1); }));
      for (const auto& h : handles) pool.wait(h);
    });
  for (auto& producer : producers) producer.join();
  for (const auto& n : seen) CHECK(n.load() == 1);
}

void stolen_batch_dependencies() {
  std::atomic<bool> owner_entered{false}, thief_entered{false};
  std::atomic<bool> build{false}, prepared{false}, steal{false},
      release_owner{false};
  std::atomic<bool> child_ran{false};
  dagflow::Handle child, dependent;
  dagflow::Config cfg;
  cfg.threads = 2;
  cfg.shards = 1;
  cfg.pin_threads = false;
  dagflow::Pool pool(cfg);
  auto owner = pool.submit([&] {
    owner_entered.store(true, std::memory_order_release);
    owner_entered.notify_one();
    build.wait(false, std::memory_order_acquire);
    dependent = pool.submit([&] {
      pool.wait(child);
      CHECK(child_ran.load());
    });
    child = pool.submit([&] { child_ran.store(true); });
    prepared.store(true, std::memory_order_release);
    prepared.notify_one();
    release_owner.wait(false, std::memory_order_acquire);
  });
  owner_entered.wait(false, std::memory_order_acquire);
  auto blocker = pool.submit([&] {
    thief_entered.store(true, std::memory_order_release);
    thief_entered.notify_one();
    steal.wait(false, std::memory_order_acquire);
  });
  thief_entered.wait(false, std::memory_order_acquire);
  build.store(true, std::memory_order_release);
  build.notify_one();
  prepared.wait(false, std::memory_order_acquire);
  // Only the thief can execute these tasks. It steals the dependent first,
  // followed by the child, and must publish the child before waiting on it.
  steal.store(true, std::memory_order_release);
  steal.notify_one();
  pool.wait(dependent);
  pool.wait(child);
  release_owner.store(true, std::memory_order_release);
  release_owner.notify_one();
  pool.wait(owner);
  pool.wait(blocker);
}

void expect_failure(const dagflow::Handle& handle) {
  bool threw = false;
  try {
    handle.rethrow_if_failed();
  } catch (const std::runtime_error&) {
    threw = true;
  }
  CHECK(threw);
}

void algorithm_exceptions() {
  dagflow::Config cfg;
  cfg.threads = 4;
  cfg.pin_threads = false;
  dagflow::Pool pool(cfg);
  auto direct = pool.submit([] { throw std::runtime_error("task failure"); });
  pool.wait(direct);
  expect_failure(direct);

  std::vector<int> input(100000, 1);
  for (bool stealing : {false, true}) {
    auto fail = [](int&) { throw std::runtime_error("range failure"); };
    auto result =
        stealing ? pool.for_each_ws(input.begin(), input.end(), fail, {}, 64)
                 : pool.for_each(input.begin(), input.end(), fail);
    pool.wait(result);
    expect_failure(result);
  }
  std::vector<int> tiny(3);
  auto zero_hint = pool.for_each_ws(
      tiny.begin(), tiny.end(), [](int& value) { ++value; }, {}, 0);
  pool.wait(zero_hint);
  zero_hint.rethrow_if_failed();
  for (int value : tiny) CHECK(value == 1);
}

// Fail while locating the second chunk, after the first has been published.
// Advancing iterators inside executing chunks remains valid.
struct FailingAdvanceIterator {
  using iterator_category = std::random_access_iterator_tag;
  using value_type = int;
  using difference_type = std::ptrdiff_t;
  using pointer = int*;
  using reference = int&;
  int* ptr;
  int& operator*() const { return *ptr; }
  FailingAdvanceIterator& operator++() {
    ++ptr;
    return *this;
  }
  FailingAdvanceIterator& operator--() {
    --ptr;
    return *this;
  }
  FailingAdvanceIterator& operator+=(difference_type amount) {
    if (amount != 0) throw std::runtime_error("iterator advance failure");
    return *this;
  }
  difference_type operator-(FailingAdvanceIterator other) const {
    return ptr - other.ptr;
  }
};

void partial_range_submission() {
  dagflow::Config cfg;
  cfg.threads = 1;
  cfg.pin_threads = false;
  dagflow::Pool pool(cfg);
  // Exercise rollback from both an external thread and a helping worker.
  for (bool nested : {false, true}) {
    std::vector<int> input(32768);
    auto operation = [&] {
      bool threw = false;
      try {
        (void)pool.for_each(FailingAdvanceIterator{input.data()},
                            FailingAdvanceIterator{input.data() + input.size()},
                            [](int& value) { ++value; });
      } catch (const std::runtime_error&) {
        threw = true;
      }
      CHECK(threw);
      // Returning by exception must have drained all published range users.
      for (std::size_t i = 0; i < input.size(); ++i)
        CHECK(input[i] == (i < 16384 ? 1 : 0));
    };
    if (nested) {
      auto h = pool.submit(operation);
      pool.wait(h);
      h.rethrow_if_failed();
    } else {
      operation();
    }
  }
}

void completion_edges() {
  dagflow::Config cfg;
  cfg.threads = 1;
  cfg.pin_threads = false;
  dagflow::Pool pool(cfg);
  auto source = dagflow::detail::CompletionCredit::create();
  auto first = source.handle();
  auto combined = pool.combine({first, {}, first});
  CHECK(!combined.ready());
  source.fail(std::make_exception_ptr(std::runtime_error("source")));
  source.finish();
  pool.wait(combined);
  expect_failure(combined);
  auto completed = pool.combine({first, combined, {}});
  pool.wait(completed);
  expect_failure(completed);

  // Propagation through arbitrary composition depth uses bounded stack space.
  auto root = dagflow::detail::CompletionCredit::create();
  auto tail = root.handle();
  for (int i = 0; i < 20000; ++i) tail = pool.combine({tail});
  root.finish();
  pool.wait(tail);
  tail.rethrow_if_failed();

  // Race publication of the last decrement with completion-edge registration.
  for (int i = 0; i < 200; ++i) {
    auto h = pool.submit([] {});
    auto joined = pool.combine({h, h, {}});
    pool.wait(joined);
    CHECK(joined.ready());
  }
}

void handles_outlive_pool() {
  dagflow::Handle handle;
  {
    dagflow::Config cfg;
    cfg.threads = 1;
    cfg.pin_threads = false;
    dagflow::Pool pool(cfg);
    handle = pool.submit([] { throw std::runtime_error("retained error"); });
    pool.wait(handle);
  }
  expect_failure(handle);
  std::thread release(
      [retained = std::move(handle)]() mutable { retained = {}; });
  release.join();
}

int main() {
  worker_overflow(dagflow::Priority::Normal);
  worker_overflow(dagflow::Priority::High);
  external_backpressure();
  cross_pool_submission();
  concurrent_submission();
  stolen_batch_dependencies();
  algorithm_exceptions();
  partial_range_submission();
  completion_edges();
  handles_outlive_pool();
  std::puts("pool queue tests passed");
}
