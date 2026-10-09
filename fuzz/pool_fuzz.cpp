#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <thread>
#include <vector>

#include <dagflow/thread_pool.hpp>

#include "byte_reader.hpp"

namespace {
[[noreturn]] void violation() { std::abort(); }

struct Ledger {
  static constexpr std::size_t max_jobs = 4096;
  std::array<std::atomic<unsigned>, max_jobs> visited{};
  std::array<bool, max_jobs> expected{};
  std::size_t next_id = 0;

  std::size_t allocate() {
    if (next_id >= max_jobs) violation();
    expected[next_id] = true;
    return next_id++;
  }
  void hit(std::size_t id) {
    if (visited[id].fetch_add(1, std::memory_order_relaxed) != 0) violation();
  }
  void verify() const {
    for (std::size_t i = 0; i < next_id; ++i)
      if (visited[i].load(std::memory_order_acquire) != unsigned(expected[i]))
        violation();
  }
};

dagflow::SubmitOptions options(dagflow::fuzz::Bytes& bytes) {
  dagflow::SubmitOptions opt;
  opt.priority = bytes.bit() ? dagflow::Priority::High : dagflow::Priority::Normal;
  opt.mode = bytes.bit() ? dagflow::SubmissionMode::Enqueue
                         : dagflow::SubmissionMode::Spawn;
  if (bytes.bit()) opt.affinity = bytes.bound(9);
  return opt;
}

void exercise(const std::uint8_t* data, std::size_t size) {
  dagflow::fuzz::Bytes bytes(data, size);
  dagflow::Config cfg;
  cfg.threads = 1 + bytes.bound(4);
  cfg.shards = bytes.bound(7);
  cfg.central_batch = 1 + bytes.bound(16);
  cfg.pin_threads = false;
  cfg.idle_us_min = 0;
  cfg.idle_us_max = 100;
  dagflow::Pool pool(cfg);
  Ledger ledger;
  std::vector<dagflow::Handle> handles;
  // for_each borrows its input; preserve the underlying buffers until drained.
  std::vector<std::shared_ptr<std::vector<unsigned>>> ranges;
  const unsigned op_count = 1 + bytes.bound(32);

  for (unsigned step = 0; step < op_count; ++step) {
    const unsigned op = bytes.bound(12);
    auto opt = options(bytes);
    if (op == 0) {
      const auto id = ledger.allocate();
      handles.push_back(pool.submit([&ledger, id] { ledger.hit(id); }, opt));
    } else if (op == 1) {
      const auto id = ledger.allocate();
      pool.submit_detached([&ledger, id] { ledger.hit(id); }, opt);
    } else if (op == 2) {
      const unsigned count = 1 + bytes.bound(80);  // Also crosses 64-packet groups.
      std::vector<std::function<void()>> tasks;
      tasks.reserve(count);
      for (unsigned i = 0; i < count; ++i) {
        const auto id = ledger.allocate();
        tasks.emplace_back([&ledger, id] { ledger.hit(id); });
      }
      pool.submit_batch_detached(std::span{tasks}, opt);
    } else if (op == 3 || op == 4) {
      const auto parent = ledger.allocate(), child = ledger.allocate();
      handles.push_back(pool.submit([&pool, &ledger, parent, child, opt, op] {
        ledger.hit(parent);
        if (op == 3) {
          auto h = pool.submit([&ledger, child] { ledger.hit(child); }, opt);
          pool.wait_and_rethrow(h);  // Cooperative wait on a worker.
        } else {
          pool.submit_detached([&ledger, child] { ledger.hit(child); }, opt);
        }
      }, opt));
    } else if (op == 5 || op == 6) {
      const std::size_t count = bytes.bound(100);
      auto range = std::make_shared<std::vector<unsigned>>(count, 0u);
      ranges.push_back(range);
      auto work = [](unsigned& value) { ++value; };
      auto h = op == 5 ? pool.for_each(range->begin(), range->end(), work, opt)
                       : pool.for_each_ws(range->begin(), range->end(), work, opt,
                                          1 + bytes.bound(12));
      handles.push_back(std::move(h));
    } else if (op == 7) {
      const unsigned producers = 2 + bytes.bound(3);
      std::vector<std::vector<std::size_t>> jobs(producers);
      for (auto& ids : jobs) {
        const unsigned count = 1 + bytes.bound(20);
        for (unsigned k = 0; k < count; ++k) ids.push_back(ledger.allocate());
      }
      std::vector<std::thread> threads;
      threads.reserve(producers);
      for (unsigned i = 0; i < producers; ++i) {
        threads.emplace_back([&, i, opt] {
          for (const auto id : jobs[i])
            pool.submit_detached([&ledger, id] { ledger.hit(id); }, opt);
        });
      }
      for (auto& thread : threads) thread.join();
    } else if (op == 8) {
      // Quiescence oracle: producers have joined, so every accepted packet and
      // every worker-published descendant must have retired at this boundary.
      pool.wait_idle();
      ledger.verify();
      for (const auto& range : ranges)
        for (auto value : *range) if (value != 1) violation();
    } else if (op == 9) {
      if (!handles.empty()) {
        auto combined = pool.combine(std::span<const dagflow::Handle>(handles));
        pool.wait_and_rethrow(combined);
      }
    } else if (op == 10) {
      pool.close();  // May race in-flight worker submissions (allowed).
      if (!pool.closed()) violation();
      pool.wait_idle();
      ledger.verify();
      bool rejected = false;
      try {
        (void)pool.submit([] {});  // External admission must now fail.
      } catch (const std::logic_error&) {
        rejected = true;
      }
      if (!rejected) violation();
      break;
    } else {
      if (!handles.empty()) {
        pool.wait_and_rethrow(handles[bytes.bound(static_cast<unsigned>(handles.size()))]);
      }
    }
  }

  pool.wait_idle();
  ledger.verify();
  for (const auto& range : ranges)
    for (auto value : *range) if (value != 1) violation();

  // Closed admission and idempotent shutdown are intentionally exercised only
  // after all external publishers have joined, never concurrently with destroy.
  if (bytes.bit()) {
    pool.close();
    if (!pool.closed()) violation();
  }
  pool.shutdown();
  pool.shutdown();
  ledger.verify();
}
}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data,
                                      std::size_t size) {
  if (size > 4096) return 0;
  exercise(data, size);
  return 0;
}
