#include <atomic>
#include <chrono>
#include <dagflow/detail/fuzz_points.hpp>
#include <dagflow/handle.hpp>
#include <dagflow/thread_pool.hpp>
#include <thread>

#include "support.hpp"

// The waiter's final scan must see publication *after* the parking bit has
// been announced. The hook is compiled only into the fault-test runtime;
// production wait/park paths remain untouched.
namespace {
namespace hooks = dagflow::detail::fuzz_points;
struct Exchange {
  std::atomic<bool> armed{false};
  std::atomic<unsigned> phase{0};
};
void checkpoint(hooks::Point point, void* context) noexcept {
  auto& exchange = *static_cast<Exchange*>(context);
  if (point != hooks::Point::idle_announce ||
      !exchange.armed.load(std::memory_order_acquire)) return;
  // A nested idle announcement must not register a second handshake.
  if (exchange.phase.exchange(1, std::memory_order_acq_rel) != 0) return;
  while (exchange.phase.load(std::memory_order_acquire) != 2)
    std::this_thread::yield();
}
}  // namespace

int main() {
  for (unsigned repeat = 0; repeat < 24; ++repeat) {
    dagflow::Config config;
    config.threads = 1;
    config.shards = 1;
    config.idle_us_max = 250;
    dagflow::Pool pool(config);
    auto credit = dagflow::detail::CompletionCredit::create();
    auto pending = credit.handle();
    Exchange exchange;
    std::atomic<unsigned> observed{0};
    hooks::install(checkpoint, &exchange);
    auto parent = pool.submit([&] {
      exchange.armed.store(true, std::memory_order_release);
      pool.wait(pending);  // no runnable child before parking prepare
      observed.fetch_add(1, std::memory_order_relaxed);
    });
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(5);
    while (exchange.phase.load(std::memory_order_acquire) != 1 &&
           std::chrono::steady_clock::now() < deadline)
      std::this_thread::yield();
    const bool announced = exchange.phase.load(std::memory_order_acquire) == 1;
    // Never leave the callback holding the worker after a failed assertion.
    if (announced) {
      pool.submit_detached([&] { credit.finish(); });
    } else {
      credit.finish();
    }
    exchange.phase.store(2, std::memory_order_release);
    pool.wait_and_rethrow(parent);
    hooks::clear();
    pool.wait_idle();
    CHECK(announced);
    CHECK(observed == 1);
    CHECK(pending.ready());
  }
}
