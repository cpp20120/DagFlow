#include <atomic>
#include <latch>
#include <memory>
#include <thread>
#include <vector>

#include "adversarial_allocation.hpp"
#include "adversarial_support.hpp"

namespace {
void exercise(bool stealing, bool strict_worker = false) {
  auto cfg = adversarial::config(2);
  if (strict_worker) cfg.worker_wait_policy = dagflow::WorkerWaitPolicy::Reject;
  dagflow::Pool pool(cfg);
  std::latch entered(1), allocation_failed(1), release(1);
  std::atomic<unsigned> calls{0}, destroyed{0};
  std::atomic<bool> returned{false};
  const unsigned size = 4 * DAGFLOW_DEFAULT_RANGE_CHUNK;
  const unsigned accepted = stealing ? size / 2 : DAGFLOW_DEFAULT_RANGE_CHUNK;
  struct Capture {
    std::atomic<unsigned>&calls, &destroyed;
    unsigned expected;
    ~Capture() {
      CHECK(calls == expected);
      CHECK(destroyed.fetch_add(1) == 0);
    }
  };
  struct FailureGate {
    std::latch &entered, &failed;
  } gate{entered, allocation_failed};
  const auto publish = [&] {
    // Prime this thread's accounting registration before counting allocations.
    if (!strict_worker) pool.wait(pool.submit([] {}));
    std::vector<unsigned> values(size);
    auto fn = [&, owner = std::unique_ptr<Capture>(new Capture{
                      calls, destroyed, accepted})](unsigned& value) {
      if (&value == values.data()) {
        entered.count_down();
        release.wait();
      }
      ++value;
      ++calls;
    };
    // Callable owner, completion state and first packet succeed. The next
    // packet fails while its accepted predecessor is borrowing local storage.
    adversarial::allocation::fault = {3,
                                      [](void* context) noexcept {
                                        auto& gate =
                                            *static_cast<FailureGate*>(context);
                                        gate.entered.wait();
                                        gate.failed.count_down();
                                      },
                                      &gate};
    bool failed = false;
    try {
      if (stealing)
        (void)pool.for_each_ws(values, std::move(fn));
      else
        (void)pool.for_each(values, std::move(fn));
    } catch (const std::bad_alloc&) {
      failed = true;
      CHECK(calls == accepted && destroyed == 1);
    }
    adversarial::allocation::fault = {};
    CHECK(failed);
    for (unsigned i = 0; i < size; ++i)
      CHECK(values[i] == unsigned(i < accepted));
    returned.store(true);
    // The backing vector dies here, immediately after publication throws.
  };
  std::thread publisher;
  dagflow::Handle worker_publisher;
  if (strict_worker)
    worker_publisher = pool.submit(publish);
  else
    publisher = std::thread(publish);
  allocation_failed.wait();
  CHECK(!returned && destroyed == 0 && calls == 0);
  release.count_down();
  if (strict_worker)
    pool.wait_and_rethrow(worker_publisher);
  else
    publisher.join();
  CHECK(returned && destroyed == 1 && calls == accepted);
  pool.wait(pool.submit([] {}));  // Recovery after a partially accepted range.
}
}  // namespace

int main() {
  exercise(false);
  CHECK(adversarial::allocation::live == 0);
  exercise(true);
  CHECK(adversarial::allocation::live == 0);
  exercise(false, true);
  CHECK(adversarial::allocation::live == 0);
  exercise(true, true);
  CHECK(adversarial::allocation::live == 0);
}
