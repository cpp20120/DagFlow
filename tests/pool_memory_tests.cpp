#include <atomic>
#include <functional>
#include <memory>
#include <dagflow/dagflow.hpp>

#include "support.hpp"

void external_task_lifetime() {
  dagflow::Config config;
  config.threads = 1;
  config.pin_threads = false;
  dagflow::Pool pool(config);
  pool.wait(pool.submit([] {}));

  std::atomic<bool> started{false}, release{false};
  auto gate = pool.submit([&] {
    started.store(true, std::memory_order_release);
    while (!release.load(std::memory_order_acquire)) std::this_thread::yield();
  });
  while (!started.load(std::memory_order_acquire)) std::this_thread::yield();
  std::atomic<int> calls{0};
  auto lifetime = std::make_shared<int>(7);
  std::weak_ptr<int> weak = lifetime;
  for (int i = 0; i < 1000; ++i)
    pool.submit_detached([&, lifetime] {
      CHECK(*lifetime == 7);
      ++calls;
    });
  lifetime.reset();
  release.store(true, std::memory_order_release);
  pool.wait(gate);
  pool.wait_idle();
  CHECK(calls == 1000 && weak.expired());
}

void independent_worker_submission() {
  dagflow::Config config;
  config.threads = 1;
  config.pin_threads = false;
  dagflow::Pool pool(config);
  auto parent = pool.submit([&] {
    int calls = 0;
    auto finished = dagflow::detail::CompletionCredit::create();
    auto finished_handle = finished.handle();
    std::function<void()> refill;
    refill = [&] {
      if (++calls < 256)
        pool.submit_detached([&] { refill(); });
      else
        finished.finish();
    };
    auto independent =
        pool.submit([] {}, {.mode = dagflow::SubmissionMode::Enqueue});
    pool.submit_detached([&] { refill(); });
    pool.wait(independent);
    CHECK(calls < 256);  // Shared ingress progresses despite local refill.
    pool.wait(finished_handle);
    CHECK(calls == 256);
  });
  pool.wait(parent);
  parent.rethrow_if_failed();
  pool.wait_idle();
}

int main() {
  external_task_lifetime();
  independent_worker_submission();
}
