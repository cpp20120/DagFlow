#include <algorithm>
#include <atomic>
#include <latch>
#include <stdexcept>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <thread>
#include <stdexcept>
#include <cstdlib>
#include <vector>

#include <dagflow/task_graph.hpp>

#include "byte_reader.hpp"

namespace {
[[noreturn]] void violation() { std::abort(); }

// The reference model knows exactly which predecessors must finish before a
// node begins. Each node's token executions complete before successors start.
void exercise(const std::uint8_t* data, std::size_t size) {
  dagflow::fuzz::Bytes bytes(data, size);
  dagflow::Config cfg;
  cfg.threads = 1 + bytes.bound(4);
  cfg.shards = bytes.bound(7);  // Includes zero/default and empty shards.
  cfg.pin_threads = false;
  cfg.idle_us_min = 0;
  cfg.idle_us_max = 100;
  dagflow::Pool pool(cfg);

  const std::size_t n = bytes.bound(25);
  const unsigned runs = 1 + bytes.bound(3);
  const bool introduce_cycle = n != 0 && bytes.bound(8) == 7;
  std::vector<std::atomic<unsigned>> finished(n);
  std::vector<std::atomic<unsigned>> started(n);
  std::vector<std::atomic<unsigned>> in_flight(n);
  std::vector<unsigned> expected(n);
  std::vector<unsigned> allowed_concurrency(n);
  std::vector<bool> yield_after_start(n);
  std::vector<std::vector<std::size_t>> predecessors(n);
  dagflow::TaskGraph graph(pool);
  std::vector<dagflow::TaskGraph::NodeId> ids;
  ids.reserve(n);

  for (std::size_t i = 0; i < n; ++i) {
    expected[i] = 1 + bytes.bound(6);
    dagflow::TaskGraph::NodeOptions opt;
    opt.priority = bytes.bit() ? dagflow::Priority::High : dagflow::Priority::Normal;
    opt.affinity = bytes.bit() ? std::optional<std::uint32_t>{bytes.bound(8)}
                               : std::nullopt;
    opt.concurrency = 1 + static_cast<int>(bytes.bound(5));
    opt.capacity = 1 + bytes.bound(4);
    opt.overflow = dagflow::TaskGraph::Overflow::Block;
    allowed_concurrency[i] = static_cast<unsigned>(opt.concurrency);
    yield_after_start[i] = bytes.bit();
    ids.push_back(graph.emplace([&, i] {
      for (const auto predecessor : predecessors[i]) {
        if (finished[predecessor].load(std::memory_order_acquire) !=
            expected[predecessor]) violation();
      }
      if (started[i].fetch_add(1, std::memory_order_relaxed) >= expected[i])
        violation();
      const auto active = in_flight[i].fetch_add(1, std::memory_order_acq_rel) + 1;
      if (active > allowed_concurrency[i]) violation();
      if (yield_after_start[i]) std::this_thread::yield();
      in_flight[i].fetch_sub(1, std::memory_order_acq_rel);
      finished[i].fetch_add(1, std::memory_order_release);
    }, opt));
    graph.set_tokens(ids.back(), expected[i]);
  }

  // Only add edges to later vertices: the generated graph is a DAG, with an
  // explicit separate branch for a cycle. Duplicates are safe to check too.
  for (std::size_t i = 0; i < n; ++i) {
    const unsigned edges = bytes.bound(5);
    for (unsigned k = 0; k < edges && i + 1 < n; ++k) {
      const auto to = i + 1 + bytes.bound(static_cast<unsigned>(n - i - 1));
      graph.add_edge(ids[i], ids[to]);
      if (std::find(predecessors[to].begin(), predecessors[to].end(), i) ==
          predecessors[to].end()) predecessors[to].push_back(i);
    }
  }
  // Force an actual cycle for the invalid-topology branch. Adding a single
  // back-edge without a forward path is not necessarily cyclic.
  if (introduce_cycle) {
    for (std::size_t i = 1; i < n; ++i)
      graph.add_edge(ids[i - 1], ids[i]);
    graph.add_edge(ids.back(), ids.front());
  }
  const bool valid = graph.seal();
  if (valid == introduce_cycle) violation();
  if (!valid) return;

  for (unsigned r = 0; r < runs; ++r) {
    for (std::size_t i = 0; i < n; ++i) {
      finished[i].store(0, std::memory_order_relaxed);
      started[i].store(0, std::memory_order_relaxed);
      in_flight[i].store(0, std::memory_order_relaxed);
    }
    auto handle = graph.run();
    pool.wait_and_rethrow(handle);
    if (graph.last_error()) violation();
    for (std::size_t i = 0; i < n; ++i) {
      if (finished[i].load(std::memory_order_acquire) != expected[i] ||
          started[i].load(std::memory_order_acquire) != expected[i] ||
          in_flight[i].load(std::memory_order_acquire) != 0) violation();
    }
    if (r + 1 < runs) {
      if (n >= 2 && bytes.bound(4) == 0) {
        const auto from = bytes.bound(static_cast<unsigned>(n - 1));
        const auto to = from + 1 + bytes.bound(static_cast<unsigned>(n - from - 1));
        graph.add_edge(ids[from], ids[to]);
        if (std::find(predecessors[to].begin(), predecessors[to].end(), from) ==
            predecessors[to].end()) predecessors[to].push_back(from);
      }
      if (bytes.bit()) graph.reset();
    }
  }
  // A second, independently checked graph stresses Overflow::Drop / Fail
  // without conflating cancellation with the exactly-once DAG oracle above.
  if (bytes.bit()) {
    dagflow::TaskGraph bounded(pool);
    dagflow::TaskGraph::NodeOptions opt;
    const unsigned policy = bytes.bound(3);
    opt.overflow = policy == 0 ? dagflow::TaskGraph::Overflow::Block
                 : policy == 1 ? dagflow::TaskGraph::Overflow::Drop
                               : dagflow::TaskGraph::Overflow::Fail;
    opt.capacity = bytes.bound(7);
    if (policy == 0 && opt.capacity == 0) opt.capacity = 1;
    opt.concurrency = 1 + static_cast<int>(bytes.bound(5));
    const unsigned tokens = 1 + bytes.bound(30);
    std::atomic<unsigned> calls{0};
    std::atomic<unsigned> joins{0};
    auto root = bounded.emplace([&] { calls.fetch_add(1, std::memory_order_relaxed); }, opt);
    auto join = bounded.emplace([&] { joins.fetch_add(1, std::memory_order_relaxed); });
    bounded.add_edge(root, join);
    bounded.set_tokens(root, tokens);
    const bool fail = policy == 2 && tokens > opt.capacity;
    const unsigned done = policy == 1 ? std::min<unsigned>(tokens, opt.capacity) : tokens;
    const unsigned repeat = 1 + bytes.bound(3);
    for (unsigned i = 0; i < repeat; ++i) {
      calls.store(0, std::memory_order_relaxed);
      joins.store(0, std::memory_order_relaxed);
      auto h = bounded.run();
      pool.wait(h);  // Expected Fail policy errors must not be rethrown.
      if (bool(bounded.last_error()) != fail) violation();
      if (fail) {
        if (joins.load(std::memory_order_acquire) != 0) violation();
      } else {
        if (calls.load(std::memory_order_acquire) != done ||
            joins.load(std::memory_order_acquire) != 1) violation();
      }
      if (i + 1 < repeat) bounded.reset();
    }
  }
  // Cancellation is requested only after a root has actually started.
  // It must not activate its successor after the active callable completes.
  if (bytes.bit()) {
    dagflow::TaskGraph cancelled(pool);
    std::atomic<bool> entered{false}, release{false};
    std::atomic<unsigned> successor{0};
    auto a=cancelled.emplace([&] {
      entered.store(true,std::memory_order_release);
      while (!release.load(std::memory_order_acquire)) std::this_thread::yield();
    });
    auto c=cancelled.emplace([&] { successor.fetch_add(1); });
    cancelled.add_edge(a,c);
    auto h=cancelled.run();
    while(!entered.load(std::memory_order_acquire))std::this_thread::yield();
    cancelled.cancel();
    release.store(true,std::memory_order_release);
    pool.wait(h);
    if(successor.load()!=0 || cancelled.last_error()) violation();
  }
  // Throw from the user callback, then reuse the same compiled definition.
  // Error propagation and graph topology storage must remain independent.
  if (bytes.bit()) {
    dagflow::TaskGraph errors(pool);
    std::atomic<bool> fail{true};
    std::atomic<unsigned> successors{0};
    auto a=errors.emplace([&] { if(fail.exchange(false)) throw std::runtime_error("fuzz error"); });
    auto c=errors.emplace([&] { successors.fetch_add(1); });
    errors.add_edge(a,c);
    pool.wait(errors.run());
    if(!errors.last_error() || successors.load()!=0)violation();
    errors.reset();
    pool.wait_and_rethrow(errors.run());
    if(errors.last_error() || successors.load()!=1)violation();
  }
  // Controlled cancellation while an execution path is provably inside user
  // code. A cancelled run need not execute every token, but it must quiesce,
  // never activate its dependent successor, and be reusable afterward.
  if (bytes.bit()) {
    dagflow::TaskGraph cancellable(pool);
    std::latch entered(1), release(1);
    std::atomic<unsigned> visits{0};
    dagflow::TaskGraph::NodeOptions options;
    options.concurrency = 1 + static_cast<int>(bytes.bound(4));
    auto root = cancellable.emplace([&] {
      if (visits.fetch_add(1, std::memory_order_relaxed) == 0) {
        entered.count_down();
        release.wait();
      }
    }, options);
    std::atomic<unsigned> successor{0};
    auto next = cancellable.emplace([&] { successor.fetch_add(1); });
    cancellable.add_edge(root, next);
    cancellable.set_tokens(root, 16 + bytes.bound(48));
    auto run = cancellable.run();
    entered.wait();
    cancellable.cancel();
    release.count_down();
    pool.wait(run);
    if (successor.load() != 0 || visits.load() == 0 || cancellable.last_error())
      violation();
    cancellable.reset();
  }
  // An exception cancels graph activation; its result must be observable,
  // and subsequent runs must not retain the preceding run's error state.
  if (bytes.bit()) {
    dagflow::TaskGraph faulted(pool);
    std::atomic<unsigned> calls{0}, successor{0};
    auto root = faulted.emplace([&] {
      calls.fetch_add(1);
      throw std::runtime_error("graph fuzz error");
    });
    auto next = faulted.emplace([&] { successor.fetch_add(1); });
    faulted.add_edge(root, next);
    for (unsigned repeat = 0; repeat < 2; ++repeat) {
      auto h = faulted.run();
      pool.wait(h);
      if (!faulted.last_error() || successor.load() != 0 || calls.load() != repeat + 1)
        violation();
      bool propagated = false;
      try { h.rethrow_if_failed(); } catch (const std::runtime_error&) { propagated = true; }
      if (!propagated) violation();
      if (repeat == 0) faulted.reset();
    }
  }
  pool.wait_idle();
}
}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data,
                                      std::size_t size) {
  if (size > 4096) return 0;
  exercise(data, size);
  return 0;
}
