
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <forward_list>
#include <iterator>
#include <limits>
#include <memory>
#include <new>
#include <numeric>
#include <ranges>
#include <span>
#include <thread>
#include <vector>

#include <dagflow/detail/fuzz_points.hpp>
#include <dagflow/thread_pool.hpp>
#include "byte_reader.hpp"
#include "check.hpp"
#include "range_cases.hpp"

namespace {

using dagflow::fuzz::check;

struct CallbackError {};

struct Payload {
  std::atomic<unsigned>& destroyed;

  ~Payload() {
    destroyed.fetch_add(1, std::memory_order_release);
  }
};

void exercise(dagflow::fuzz::Bytes& bytes) {
  dagflow::Config cfg;
  cfg.threads = 1 + bytes.bound(4);
  cfg.shards = bytes.bound(7);
  cfg.pin_threads = false;

  dagflow::Pool pool(cfg);

  const auto n = dagflow::fuzz::range_size(bytes);
  const auto kind = bytes.bound(5);
  const bool stealing = bytes.bit() && kind != 3;
  const unsigned mode = bytes.bound(3);
  const int budget = bytes.bound(16);
  const std::size_t fail_at =
      n ? (std::size_t(bytes.next()) * 257) % n : 0;

  constexpr std::array<std::size_t, 8> grains{
      0, 1, 2, 31, 32, 33, DAGFLOW_DEFAULT_RANGE_CHUNK,
      std::numeric_limits<std::size_t>::max()};

  const auto grain = grains[bytes.bound(grains.size())];

  dagflow::SubmitOptions opt;
  opt.priority = bytes.bit()
      ? dagflow::Priority::High
      : dagflow::Priority::Normal;
  opt.mode = bytes.bit()
      ? dagflow::SubmissionMode::Spawn
      : dagflow::SubmissionMode::Enqueue;

  if (bytes.bit()) opt.affinity = bytes.bound(9);

  const bool yielding = bytes.bit();

  std::vector<unsigned> ids(n);
  std::iota(ids.begin(), ids.end(), 0u);

  std::forward_list<unsigned> linked;
  if (kind == 3) linked.assign(ids.begin(), ids.end());

  std::vector<std::atomic<unsigned>> visits(n);
  std::atomic<unsigned> destroyed{0};

  // Second pass always recovers with a fresh callable, no failure injection.
  for (unsigned pass = 0; pass < 2; ++pass) {
    for (auto& visit : visits)
      visit.store(0, std::memory_order_relaxed);

    dagflow::Handle handle;
    bool allocation_failed = false;

    {
      auto callback =
          [&, fail = pass == 0 && mode == 1,
           payload = std::unique_ptr<Payload>(
               new Payload{destroyed})](unsigned id) {
            check(id < n &&
                  visits[id].fetch_add(
                      1, std::memory_order_relaxed) == 0);

            if (yielding && id % 1024 == 0)
              std::this_thread::yield();

            if (fail && id == fail_at)
              throw CallbackError{};
          };

      try {
        dagflow::fuzz::AllocationBudget injection(
            pass == 0 && mode == 2 ? budget : -1);

        auto submit_range = [&](auto&& range) {
          if constexpr (
              std::ranges::random_access_range<decltype(range)>) {
            if (stealing)
              return pool.for_each_ws(
                  range, std::move(callback), opt, grain);
          }

          return pool.for_each(
              range, std::move(callback), opt);
        };

        switch (kind) {
          case 0:
            handle = stealing
                ? pool.for_each_ws(
                      ids.begin(), ids.end(),
                      std::move(callback), opt, grain)
                : pool.for_each(
                      ids.begin(), ids.end(),
                      std::move(callback), opt);
            break;

          case 1:
            handle = submit_range(std::span(ids));
            break;

          case 2: {
            auto prefix = std::ranges::subrange(
                std::counted_iterator(ids.begin(), n),
                std::default_sentinel);
            handle = submit_range(prefix);
            break;
          }

          case 3:
            handle = submit_range(linked);
            break;

          default:
            handle = submit_range(std::views::iota(
                0u, static_cast<unsigned>(n)));
            break;
        }
      } catch (const std::bad_alloc&) {
        allocation_failed = true;
      }
    }

    // The local callback is now destroyed if ownership was not transferred.
    // This includes empty ranges and failed publication before the move.

    if (allocation_failed) {
      check(pass == 0 && mode == 2);

      // Publication failure must drain borrowed users before throwing.
      // Check destruction BEFORE wait_idle(), which could hide an early return.
      check(destroyed.load(std::memory_order_acquire) == pass + 1);
    } else {
      pool.wait(handle);
      check(handle.ready());

      check(destroyed.load(std::memory_order_acquire) == pass + 1);

      bool callback_failed = false;
      try {
        handle.rethrow_if_failed();
      } catch (const CallbackError&) {
        callback_failed = true;
      }

      check(callback_failed == (pass == 0 && mode == 1 && n != 0));
    }

    // Also catch detached stragglers or duplicated split ranges after error.
    pool.wait_idle();

    for (const auto& visit : visits) {
      const auto count = visit.load(std::memory_order_acquire);

      check(count <= 1);

      if (pass == 1 || (mode != 1 && !allocation_failed))
        check(count == 1);
    }
  }
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(
    const std::uint8_t* data, std::size_t size) {
  if (size > 4096) return 0;

  dagflow::fuzz::Bytes bytes(data, size);
  exercise(bytes);

  return 0;
}
