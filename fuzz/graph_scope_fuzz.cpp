#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <forward_list>
#include <iterator>
#include <memory>
#include <ranges>
#include <span>
#include <vector>

#include <dagflow/graph_scope.hpp>
#include "byte_reader.hpp"
#include "check.hpp"
#include "range_cases.hpp"

namespace {
using dagflow::fuzz::check;
struct CallbackError {};
struct Payload {
  std::atomic<unsigned>& destroyed;
  ~Payload() { destroyed.fetch_add(1, std::memory_order_release); }
};

void exercise(dagflow::fuzz::Bytes& bytes) {
  dagflow::Config cfg;
  cfg.threads = 1 + bytes.bound(4);
  cfg.shards = bytes.bound(7);
  cfg.pin_threads = false;
  dagflow::Pool pool(cfg);
  const auto n = dagflow::fuzz::range_size(bytes);
  const auto mode = bytes.bound(6);
  const unsigned repeats = 1 + bytes.bound(4);
  const unsigned delta = 1 + bytes.bound(17);
  dagflow::ScheduleOptions opt;
  opt.concurrency = 1 + bytes.bound(4);
  opt.capacity = 1 + bytes.bound(3);
  opt.priority = bytes.bit() ? dagflow::Priority::High : dagflow::Priority::Normal;
  if (bytes.bit()) opt.affinity = bytes.bound(9);
  std::vector<unsigned> values(n + 2, 0);
  // Guard elements expose overrun and off-by-one chunk partitioning.
  values.front() = 0x1234;
  values.back() = 0x5678;
  std::forward_list<unsigned> linked(1 + bytes.bound(65), 0);
  auto prefix = std::ranges::subrange(std::counted_iterator(values.begin() + 1, n),
                                     std::default_sentinel);
  std::atomic<unsigned> roots{0}, joins{0}, destroyed{0};
  bool fail = mode == 5; // Only changed while the previous run is quiescent.
  unsigned expected_roots = 0, expected_joins = 0;
  dagflow::Handle survivor;
  {
    dagflow::GraphScope graph(pool);
    check(graph.empty() && graph.size() == 0);
    graph.wait(); // Empty wait must not manufacture work.
    auto root = graph.submit([&, payload = std::unique_ptr<Payload>(new Payload{destroyed})] {
      roots.fetch_add(1, std::memory_order_relaxed);
      if (fail) throw CallbackError{};
      std::fill(values.begin() + 1, values.end() - 1, 1u);
      std::fill(linked.begin(), linked.end(), 1u);
    });
    auto array = graph.parallel_for_after(root, prefix, [delta](unsigned& x) { x += delta; }, opt);
    auto list = graph.parallel_for_after(root, linked.begin(), linked.end(),
                                         [delta](unsigned& x) { x += delta; }, opt);
    // Duplicate edges, mixed overloads, empty ranges and empty fan-in.
    auto empty = graph.parallel_for(std::span<unsigned>{}, [](unsigned&) { check(false); });
    auto independent = graph.when_all({}, [] {});
    std::array deps{array, list, array, empty, independent};
    auto joined = graph.when_all(std::span<const dagflow::JobHandle>(deps), [&] {
      for (auto x : prefix) check(x == 1 + delta);
      for (auto x : linked) check(x == 1 + delta);
      check(values.front() == 0x1234 && values.back() == 0x5678);
      joins.fetch_add(1, std::memory_order_relaxed);
    });
    graph.then(joined, [] {});
    check(root.valid() && joined.valid() && graph.size() == 7);
    if (mode == 0) {
      // Unstarted dirty graph is run and joined by the destructor.
      expected_roots = expected_joins = 1;
    } else if (mode == 1) {
      graph.wait(); // Implicit start.
      graph.wait(); // Consumed run must not start twice.
      expected_roots = expected_joins = 1;
    } else if (mode == 2) {
      for (unsigned i = 0; i < repeats; ++i) {
        survivor = graph.run();
        auto duplicate = graph.run();
        pool.wait_and_rethrow(duplicate);
        graph.wait();
        check(survivor.ready());
        check(roots.load() == i + 1 && joins.load() == i + 1);
      }
      expected_roots = expected_joins = repeats;
    } else if (mode == 3) {
      survivor = graph.run();
      graph.clear(); // Must drain a run even without an explicit wait.
      check(survivor.ready() && graph.empty() && destroyed.load() == 1);
      expected_roots = expected_joins = 1;
    } else if (mode == 4) {
      graph.clear(); // Unstarted work is discarded, including captures.
      check(graph.empty() && destroyed.load() == 1);
    } else {
      bool caught = false;
      try { graph.run_and_wait(); }
      catch (const CallbackError&) { caught = true; }
      check(caught && graph.last_error() && roots.load() == 1 && joins.load() == 0);
      fail = false;
      graph.run_and_wait(); // Same compiled graph after a failed execution.
      check(!graph.last_error());
      expected_roots = 2;
      expected_joins = 1;
    }
    if (mode != 0) {
      check(roots.load() == expected_roots && joins.load() == expected_joins);
      graph.clear();
      graph.clear();
      check(graph.empty() && !graph.last_error() && destroyed.load() == 1);
      // New nodes after clear must not retain old edges, run handles or errors.
      graph.emplace([&] { joins.fetch_add(1, std::memory_order_relaxed); });
      graph.run_and_wait();
      ++expected_joins;
    }
  }
  check(roots.load() == expected_roots && joins.load() == expected_joins);
  check(destroyed.load() == 1 && survivor.ready());
  pool.shutdown();
  survivor.rethrow_if_failed();
}
} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  if (size > 4096) return 0;
  dagflow::fuzz::Bytes bytes(data, size);
  exercise(bytes);
  return 0;
}
