#include <algorithm>
#include <array>
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
#include <dagflow/detail/scheduler.hpp>
#include "byte_reader.hpp"
#include "check.hpp"

namespace {
using dagflow::fuzz::check;
using dagflow::detail::ScheduledTask;
constexpr std::size_t local_capacity = DAGFLOW_LOCAL_QUEUE_CAPACITY;
constexpr std::size_t central_capacity = DAGFLOW_CENTRAL_QUEUE_CAPACITY;

std::uint32_t batch_size(dagflow::fuzz::Bytes& bytes) {
  constexpr std::array<std::uint32_t, 8> sizes{
      0, 1, 2, 31, 32, 1024, 1025, std::numeric_limits<std::uint32_t>::max()};
  return sizes[bytes.bound(sizes.size())];
}

std::size_t packet_index(ScheduledTask* task, const std::vector<ScheduledTask>& packets) {
  const auto address = reinterpret_cast<std::uintptr_t>(task);
  const auto begin = reinterpret_cast<std::uintptr_t>(packets.data());
  check(address >= begin && address - begin < packets.size() * sizeof(ScheduledTask));
  check((address - begin) % sizeof(ScheduledTask) == 0);
  return (address - begin) / sizeof(ScheduledTask);
}

void scheduler_saturation(dagflow::fuzz::Bytes& bytes) {
  const unsigned workers = 1 + bytes.bound(4), shards = 1 + bytes.bound(5);
  const auto batch = batch_size(bytes);
  const unsigned home = bytes.bound(shards);
  std::vector<std::uint32_t> mapping(workers, home); // Other shards can be empty.
  dagflow::detail::Scheduler scheduler(workers, shards, batch, mapping);
  const unsigned priorities = 1 + bytes.bound(2);
  const unsigned extra = 1 + bytes.bound(65);
  const unsigned rounds = 2 + bytes.bound(2); // Reuse ring slots across wraparound.
  const std::size_t per_priority = local_capacity + central_capacity + extra;
  std::vector<ScheduledTask> packets(priorities * per_priority);
  std::vector<std::atomic<unsigned>> seen(packets.size());
  for (unsigned round = 0; round < rounds; ++round) {
    for (auto& n : seen) n.store(0, std::memory_order_relaxed);
    for (unsigned p = 0; p < priorities; ++p) {
      for (std::size_t i = 0; i < per_priority; ++i) {
        auto* packet = &packets[p * per_priority + i];
        packet->prio = p == 0 ? dagflow::Priority::Normal : dagflow::Priority::High;
        const bool accepted = scheduler.try_submit_local(0, packet);
        check(accepted == (i < local_capacity + central_capacity));
        if (!accepted) {
          check(!scheduler.try_submit_external(home, packet));
          scheduler.submit_overflow(home, packet);
        }
      }
    }
    // Ownership of local[0] transfers from this thread to executor zero only
    // after publication is finished; each deque always has exactly one owner.
    std::vector<std::thread> executors;
    for (unsigned w = 0; w < workers; ++w) {
      executors.emplace_back([&, w] {
        while (auto* packet = scheduler.try_acquire(w)) {
          const auto i = packet_index(packet, packets);
          check(seen[i].fetch_add(1, std::memory_order_relaxed) == 0);
          if ((i & 255) == 0) std::this_thread::yield();
        }
      });
    }
    for (auto& thread : executors) thread.join();
    for (auto& n : seen) check(n.load() == 1);
    for (unsigned w = 0; w < workers; ++w) check(scheduler.try_acquire(w) == nullptr);
  }
}

struct Payload {
  std::atomic<unsigned>& destroyed;
  ~Payload() { destroyed.fetch_add(1, std::memory_order_release); }
};
struct CallbackError {};

void worker_overflow(dagflow::fuzz::Bytes& bytes) {
  dagflow::Config cfg;
  cfg.threads = 1;
  cfg.shards = 1;
  cfg.pin_threads = false;
  cfg.central_batch = batch_size(bytes);
  dagflow::Pool pool(cfg);
  dagflow::SubmitOptions opt;
  opt.priority = bytes.bit() ? dagflow::Priority::High : dagflow::Priority::Normal;
  opt.mode = bytes.bit() ? dagflow::SubmissionMode::Enqueue : dagflow::SubmissionMode::Spawn;
  const std::size_t count = local_capacity + central_capacity + 1 + bytes.bound(65);
  const bool batched = bytes.bit();
  std::vector<std::atomic<unsigned>> seen(count);
  std::atomic<unsigned> destroyed{0}, completed{0};
  auto parent = pool.submit([&] {
    auto make_job = [&](std::size_t i) {
      return [&, i, payload = std::unique_ptr<Payload>(new Payload{destroyed})] {
        check(seen[i].fetch_add(1) == 0);
        completed.fetch_add(1);
      };
    };
    if (batched) {
      std::vector<decltype(make_job(0))> jobs;
      jobs.reserve(count);
      for (std::size_t i = 0; i < count; ++i) jobs.push_back(make_job(i));
      pool.submit_batch_detached(std::span{jobs}, opt);
    } else {
      for (std::size_t i = 0; i < count; ++i) pool.submit_detached(make_job(i), opt);
    }
    // Full queues may spill, but submit must never execute children inline.
    check(completed.load() == 0 && destroyed.load() == 0);
    auto failed = pool.submit([] { throw CallbackError{}; }, opt);
    check(!failed.ready());
    pool.wait(failed); // Cooperative helping must also see overflow packets.
    bool caught = false;
    try { failed.rethrow_if_failed(); } catch (const CallbackError&) { caught = true; }
    check(caught);
  });
  pool.wait_and_rethrow(parent);
  pool.wait_idle();
  for (auto& n : seen) check(n.load() == 1);
  check(completed.load() == count && destroyed.load() == count);
}

// Observe a genuine rejected ingress push, without a sleep-based assertion
// that the producer happened to run while the bounded queue was full.
struct RetryObserver {
  std::atomic<bool> observed{false};
  std::latch retry{1};
  RetryObserver() { dagflow::detail::fuzz_points::install(&hit, this); }
  ~RetryObserver() { dagflow::detail::fuzz_points::clear(); }
  static void hit(dagflow::detail::fuzz_points::Point point, void* context) noexcept {
    auto& self = *static_cast<RetryObserver*>(context);
    if (point == dagflow::detail::fuzz_points::Point::external_retry && !self.observed.exchange(true))
      self.retry.count_down();
  }
};

[[maybe_unused]] void external_backpressure(dagflow::fuzz::Bytes& bytes) {
  RetryObserver observer; // Outlives the pool and every publisher.
  dagflow::Config cfg;
  cfg.threads = 1;
  cfg.shards = 1;
  cfg.pin_threads = false;
  cfg.central_batch = batch_size(bytes);
  dagflow::Pool pool(cfg);
  dagflow::SubmitOptions opt;
  opt.priority = bytes.bit() ? dagflow::Priority::High : dagflow::Priority::Normal;
  const unsigned extra = 1 + bytes.bound(65);
  const bool batched = bytes.bit(), close = bytes.bit();
  std::latch entered(1), release(1);
  auto blocker = pool.submit([&] { entered.count_down(); release.wait(); });
  entered.wait();
  std::vector<std::atomic<unsigned>> seen(central_capacity + extra);
  for (std::size_t i = 0; i < central_capacity; ++i)
    pool.submit_detached([&, i] { check(seen[i].fetch_add(1) == 0); }, opt);
  std::atomic<unsigned> accepted{0};
  std::thread producer([&] {
    // One external admission covers each <=64-packet batch. Use a single
    // batch so close cannot split admission across groups after the retry.
    const unsigned jobs = batched ? std::min(extra, 64u) : 1u;
    auto make_job = [&](unsigned i) {
      return [&, i] { check(seen[central_capacity + i].fetch_add(1) == 0); };
    };
    if (batched) {
      std::vector<decltype(make_job(0))> pending;
      for (unsigned i = 0; i < jobs; ++i) pending.push_back(make_job(i));
      pool.submit_batch_detached(std::span{pending}, opt);
    } else pool.submit_detached(make_job(0), opt);
    accepted.store(jobs, std::memory_order_release);
  });
  observer.retry.wait();
  check(accepted.load() == 0);
  if (close) pool.close(); // An already reserved publisher must still finish.
  release.count_down();
  producer.join();
  pool.wait_and_rethrow(blocker);
  pool.shutdown();
  for (std::size_t i = 0; i < seen.size(); ++i)
    check(seen[i].load() == unsigned(i < central_capacity + accepted.load()));
}

void fairness(dagflow::fuzz::Bytes& bytes) {
  const unsigned shards = 1 + bytes.bound(7);
  const auto batch = batch_size(bytes);
  const bool high = bytes.bit();
  const bool local_normal = bytes.bit();
  dagflow::detail::Scheduler scheduler(1, shards, batch);
  std::vector<ScheduledTask> packets(1 + 2 * shards);
  std::vector<unsigned> visits(packets.size());
  for (auto& packet : packets) packet.prio = high ? dagflow::Priority::High : dagflow::Priority::Normal;
  if (local_normal) packets[0].prio = dagflow::Priority::Normal;
  check(scheduler.try_submit_local(0, &packets[0]));
  for (unsigned s = 0; s < shards; ++s) {
    check(scheduler.try_submit_external(s, &packets[1 + 2 * s]));
    scheduler.submit_overflow(s, &packets[2 + 2 * s]);
  }
  // Keep BOTH sources in every shard and the local queue permanently busy.
  // Same-priority ingress/overflow must progress in bounded acquisitions.
  // No promise is made for Normal tasks under endless High-priority traffic.
  for (unsigned i = 0; i < 128 * shards; ++i) {
    auto* task = scheduler.try_acquire(0);
    const auto id = packet_index(task, packets);
    ++visits[id];
    if (id == 0) check(scheduler.try_submit_local(0, task));
    else if (id % 2) check(scheduler.try_submit_external((id - 1) / 2, task));
    else scheduler.submit_overflow((id - 2) / 2, task);
  }
  for (std::size_t i = 1; i < visits.size(); ++i) check(visits[i] != 0);
  std::vector<bool> drained(packets.size());
  for (std::size_t i = 0; i < packets.size(); ++i) {
    const auto id = packet_index(scheduler.try_acquire(0), packets);
    check(!drained[id]);
    drained[id] = true;
  }
  check(scheduler.try_acquire(0) == nullptr);
}
} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  if (size > 4096) return 0;
  dagflow::fuzz::Bytes bytes(data, size);
  switch (bytes.bound(4)) {
    case 0: scheduler_saturation(bytes); break;
    case 1: worker_overflow(bytes); break;
    // Keep the input layout stable, but do not wait on commented-out hooks.
    // Restore external_backpressure(bytes) when runtime checkpoints return.
    case 2: break;
    default: fairness(bytes); break;
  }
  return 0;
}
