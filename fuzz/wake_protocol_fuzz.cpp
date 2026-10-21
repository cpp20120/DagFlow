#include <atomic>
#include <cstddef>
#include <cstdint>
#include <latch>
#include <limits>
#include <memory>
#include <span>
#include <thread>
#include <vector>

#include <dagflow/detail/fuzz_points.hpp>
#include <dagflow/thread_pool.hpp>
#include "byte_reader.hpp"
#include "check.hpp"

namespace {
using dagflow::fuzz::check;
using Point = dagflow::detail::fuzz_points::Point;

// This gate intentionally waits for a protocol peer, rather than expiring
// after a scheduling perturbation. Only unlocked checkpoints are selected.
// A broken protocol is a libFuzzer per-input timeout, not a silently skipped
// interleaving. Install before pool construction; destroy after worker joins.
struct Gate {
  const Point target;
  std::latch arrived, release{1}, first_wake{1};
  std::atomic<bool> armed{true}, released{false}, saw_wake{false};
  std::atomic<unsigned> wake_count{0};
  const bool helping_only;

  Gate(Point point, unsigned participants, bool helping = false)
      : target(point), arrived(participants), helping_only(helping) {
    dagflow::detail::fuzz_points::install(&hit, this);
  }
  ~Gate() { dagflow::detail::fuzz_points::clear(); }
  void open() {
    armed.store(false, std::memory_order_release);
    released.store(true, std::memory_order_release);
    release.count_down();
  }
  static void hit(Point point, void* context) noexcept {
    auto& self = *static_cast<Gate*>(context);
    if (point == Point::wake_claim) {
      self.wake_count.fetch_add(1, std::memory_order_relaxed);
      if (!self.saw_wake.exchange(true)) self.first_wake.count_down();
    }
    if (point == self.target && self.armed.load(std::memory_order_acquire) &&
        (!self.helping_only || dagflow::Pool::worker_wait_depth() != 0)) {
      self.arrived.count_down();
      self.release.wait();
    }
  }
};

dagflow::Config config(dagflow::fuzz::Bytes& bytes) {
  dagflow::Config cfg;
  cfg.threads = 1 + bytes.bound(8);
  cfg.shards = 1 + bytes.bound(9);
  cfg.central_batch = bytes.bit() ? 1 : std::numeric_limits<std::uint32_t>::max();
  cfg.pin_threads = false;
  // No periodic polling rescue inside libFuzzer's normal per-input timeout:
  // progress must come from final scanning or an actual wake/relay.
  cfg.idle_us_min = cfg.idle_us_max = std::numeric_limits<std::uint32_t>::max();
  const bool clustered = bytes.bit();
  for (unsigned i = 0; i < cfg.threads; ++i)
    cfg.worker_shards.push_back(clustered ? cfg.shards - 1 : bytes.bound(cfg.shards));
  return cfg;
}

struct Payload {
  std::atomic<unsigned>& destroyed;
  ~Payload() { destroyed.fetch_add(1, std::memory_order_release); }
};

void publication_and_relay(dagflow::fuzz::Bytes& bytes, bool before_cv) {
  auto cfg = config(bytes);
  const bool children = bytes.bit();
  dagflow::SubmitOptions opt;
  opt.priority = bytes.bit() ? dagflow::Priority::High : dagflow::Priority::Normal;
  if (bytes.bit()) opt.affinity = bytes.bound(17);
  Gate gate(before_cv ? Point::before_cv : Point::idle_announce, cfg.threads);
  dagflow::Pool pool(cfg);
  gate.arrived.wait(); // Every real worker is now registered as idle.

  std::latch entered(cfg.threads), finish(1);
  std::vector<std::atomic<unsigned>> seen(cfg.threads), descendants(cfg.threads);
  std::vector<std::thread::id> executors(cfg.threads);
  std::atomic<unsigned> destroyed{0};
  auto make_job = [&](unsigned i) {
    return [&, i, payload = std::unique_ptr<Payload>(new Payload{destroyed})] {
      check(seen[i].fetch_add(1) == 0);
      executors[i] = std::this_thread::get_id();
      entered.count_down();
      finish.wait(); // Needs one physical worker per body; helping cannot fake it.
      if (children)
        pool.submit_detached([&, i] { check(descendants[i].fetch_add(1) == 0); }, opt);
    };
  };
  std::vector<decltype(make_job(0))> jobs;
  for (unsigned i = 0; i < cfg.threads; ++i) jobs.push_back(make_job(i));
  // A <=64-packet external batch emits exactly one initial wake, including
  // when it is routed to a shard with no resident workers.
  pool.submit_batch_detached(std::span{jobs}, opt);
  check(gate.wake_count.load() == 1);
  gate.open();
  entered.wait();
  for (unsigned i = 0; i < cfg.threads; ++i)
    for (unsigned j = 0; j < i; ++j) check(executors[i] != executors[j]);
  // At before_cv all workers have already finished their final acquisition
  // scan. Each must receive a remembered or live signal before its callback.
  // No callback has returned to announce another wait, so claims are unique.
  if (before_cv) check(gate.wake_count.load() == cfg.threads);
  check(destroyed.load() == 0);
  pool.close(); // Running parents may still publish their descendants.
  finish.count_down();
  pool.wait_idle();
  check(destroyed.load() == cfg.threads);
  for (unsigned i = 0; i < cfg.threads; ++i) {
    check(seen[i].load() == 1);
    check(descendants[i].load() == unsigned(children));
  }
  pool.shutdown(); // Hook context is still alive until every worker exits.
}

void shutdown_wake(dagflow::fuzz::Bytes& bytes) {
  auto cfg = config(bytes);
  Gate gate(bytes.bit() ? Point::before_cv : Point::idle_announce, cfg.threads);
  dagflow::Pool pool(cfg);
  gate.arrived.wait();
  std::thread closer([&] { pool.shutdown(); });
  gate.first_wake.wait(); // Stop has been published before wake_all signals.
  gate.open();
  closer.join();
  check(pool.closed() && gate.wake_count.load() >= cfg.threads);
  pool.shutdown();
}

void physical_retirement(dagflow::fuzz::Bytes& bytes) {
  auto cfg = config(bytes);
  const unsigned waiters = 1 + bytes.bound(4), yields = bytes.bound(64);
  Gate gate(Point::before_retire, 1);
  dagflow::Pool pool(cfg);
  std::atomic<unsigned> destroyed{0}, calls{0};
  auto handle = pool.submit([&, payload = std::unique_ptr<Payload>(new Payload{destroyed})] {
    calls.fetch_add(1);
  });
  gate.arrived.wait(); // Packet is destroyed and ready; its accounting is live.
  check(handle.ready() && calls.load() == 1 && destroyed.load() == 1);
  pool.wait_and_rethrow(handle);
  std::latch started(waiters);
  std::atomic<bool> premature{false};
  std::vector<std::thread> observers;
  for (unsigned i = 0; i < waiters; ++i) {
    observers.emplace_back([&] {
      started.count_down();
      pool.wait_idle();
      if (!gate.released.load(std::memory_order_acquire)) premature.store(true);
    });
  }
  started.wait();
  for (unsigned i = 0; i < yields; ++i) std::this_thread::yield();
  gate.open();
  for (auto& observer : observers) observer.join();
  check(!premature.load());
  pool.shutdown();
}

void worker_completion(dagflow::fuzz::Bytes& bytes) {
  auto cfg = config(bytes);
  const bool via_task = bytes.bit();
  Gate gate(bytes.bit() ? Point::before_cv : Point::idle_announce, cfg.threads,
            true);
  auto completion = dagflow::detail::CompletionCredit::create();
  auto handle = completion.handle();
  dagflow::Pool pool(cfg);
  std::latch entered(cfg.threads);
  std::atomic<unsigned> finished{0};
  for (unsigned i = 0; i < cfg.threads; ++i) {
    pool.submit_detached([&] {
      entered.count_down();
      entered.wait();
      pool.wait(handle);
      check(handle.ready());
      finished.fetch_add(1);
    });
  }
  gate.arrived.wait();  // Every worker is suspended inside Pool::wait.
  if (via_task) {
    // Wake for new work must resume helping even though the handle is pending.
    pool.submit_detached(
        [credit = std::move(completion)]() mutable { credit.finish(); });
  } else {
    // No queue publication: only completion can wake the suspended workers.
    completion.finish();
  }
  gate.open();
  pool.wait_idle();
  check(finished.load() == cfg.threads);
}
void nested_worker_completion(dagflow::fuzz::Bytes& bytes) {
  struct NestedGate {
    std::latch outer{1}, inner{1}, release_outer{1}, release_inner{1};
    std::atomic<bool> first_outer{true}, first_inner{true};
    NestedGate() { dagflow::detail::fuzz_points::install(&hit, this); }
    ~NestedGate() { dagflow::detail::fuzz_points::clear(); }
    static void hit(Point point, void* context) noexcept {
      if (point != Point::before_cv) return;
      auto& gate = *static_cast<NestedGate*>(context);
      const auto depth = dagflow::Pool::worker_wait_depth();
      if (depth == 1 && gate.first_outer.exchange(false)) {
        gate.outer.count_down();
        gate.release_outer.wait();
      } else if (depth == 2 && gate.first_inner.exchange(false)) {
        gate.inner.count_down();
        gate.release_inner.wait();
      }
    }
  } gate;
  auto outer = dagflow::detail::CompletionCredit::create();
  auto inner = dagflow::detail::CompletionCredit::create();
  auto outer_handle = outer.handle(), inner_handle = inner.handle();
  dagflow::Config cfg;
  cfg.threads = 1;
  cfg.idle_us_min = cfg.idle_us_max = UINT32_MAX;
  dagflow::Pool pool(cfg);
  pool.submit_detached([&] { pool.wait(outer_handle); });
  gate.outer.wait();
  pool.submit_detached([&] { pool.wait(inner_handle); });
  gate.release_outer.count_down();
  gate.inner.wait();
  // The outer completion signals the worker while the inner wait owns its
  // parking slot. The inner registration must still receive its own wake.
  outer.finish();
  if (bytes.bit())
    pool.submit_detached([&] { inner.finish(); });
  else
    inner.finish();
  gate.release_inner.count_down();
  pool.wait_idle();
  check(outer_handle.ready() && inner_handle.ready());
}
} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  if (size > 4096) return 0;
  dagflow::fuzz::Bytes bytes(data, size);
  switch (bytes.bound(6)) {
    case 0: publication_and_relay(bytes, false); break;
    case 1: publication_and_relay(bytes, true); break;
    case 2: shutdown_wake(bytes); break;
    case 3:
      physical_retirement(bytes);
      break;
    case 4:
      worker_completion(bytes);
      break;
    default:
      nested_worker_completion(bytes);
      break;
  }
  return 0;
}
