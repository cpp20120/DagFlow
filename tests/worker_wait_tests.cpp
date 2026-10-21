#include <atomic>
#include <dagflow/thread_pool.hpp>
#include <exception>
#include <functional>
#include <latch>
#include <stdexcept>
#include <vector>

#include "support.hpp"

namespace {
void shared_completion() {
  dagflow::Config cfg;
  cfg.threads = 4;
  cfg.idle_us_min = cfg.idle_us_max = 5'000'000;
  dagflow::Pool pool(cfg);
  for (unsigned iteration = 0; iteration < 128; ++iteration) {
    auto completion = dagflow::detail::CompletionCredit::create();
    auto handle = completion.handle();
    std::latch entered(cfg.threads);
    std::atomic<unsigned> finished{0};
    std::vector<dagflow::Handle> observers;
    for (unsigned i = 0; i < cfg.threads; ++i) {
      observers.push_back(pool.submit([&] {
        entered.count_down();
        entered
            .wait();  // One physical worker per observer, not nested helping.
        pool.wait(handle);
        CHECK(handle.ready());
        finished.fetch_add(1);
      }));
    }
    entered.wait();
    completion.finish();  // Races worker registration and removal.
    for (const auto& observer : observers) pool.wait_and_rethrow(observer);
    CHECK(finished.load() == cfg.threads);
  }
}

void depth_and_rejection() {
  dagflow::Pool pool({.threads = 1});
  unsigned peak = 0;
  std::function<void(unsigned)> nested = [&](unsigned left) {
    peak = std::max(peak, dagflow::Pool::worker_wait_depth());
    if (left)
      pool.wait_and_rethrow(pool.submit([&, left] { nested(left - 1); }));
  };
  pool.wait_and_rethrow(pool.submit([&] { nested(32); }));
  CHECK(peak == 32);
  pool.wait_and_rethrow(
      pool.submit([] { CHECK(dagflow::Pool::worker_wait_depth() == 0); }));
  CHECK(dagflow::Pool::worker_wait_depth() == 0);

  dagflow::Config cfg;
  cfg.threads = 1;
  cfg.worker_wait_policy = dagflow::WorkerWaitPolicy::Reject;
  dagflow::Pool strict(cfg);
  auto credit = dagflow::detail::CompletionCredit::create();
  auto pending = credit.handle();
  for (auto* target : {&strict, &pool}) {
    auto task = strict.submit([&] { target->wait(pending); });
    strict.wait(task);
    bool rejected = false;
    try {
      task.rethrow_if_failed();
    } catch (const std::logic_error&) {
      rejected = true;
    }
    CHECK(rejected);
  }
  credit.finish();
  strict.wait_and_rethrow(strict.submit([&] {
    strict.wait({});
    strict.wait(pending);  // Ready observations never block.
  }));
}

std::atomic<unsigned> detached_errors{0};
void report_error(std::exception_ptr error) noexcept {
  try {
    std::rethrow_exception(error);
  } catch (const std::runtime_error&) {
    detached_errors.fetch_add(1);
  } catch (...) {
    std::terminate();
  }
}

void detached_reporting() {
  dagflow::Config cfg;
  cfg.threads = 2;
  cfg.detached_error_handler = report_error;
  dagflow::Pool pool(cfg);
  for (unsigned i = 0; i < 64; ++i)
    pool.submit_detached([] { throw std::runtime_error("detached"); });
  auto handled = pool.submit([] { throw std::runtime_error("handled"); });
  pool.wait_idle();
  CHECK(detached_errors == 64);
  bool caught = false;
  try {
    handled.rethrow_if_failed();
  } catch (const std::runtime_error&) {
    caught = true;
  }
  CHECK(caught);
}
}  // namespace

int main() {
  shared_completion();
  depth_and_rejection();
  detached_reporting();
}
