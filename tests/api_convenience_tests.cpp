#include <algorithm>
#include <atomic>
#include <forward_list>
#include <iterator>
#include <istream>
#include <memory>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "dagflow/dagflow.hpp"
#include "support.hpp"

namespace {
struct Visit {
  void operator()(const int&) const {}
};

template <class R>
concept PoolRange = requires(dagflow::Pool& pool, R&& range) {
  pool.for_each(std::forward<R>(range), Visit{});
};
template <class R>
concept StealingRange = requires(dagflow::Pool& pool, R&& range) {
  pool.for_each_ws(std::forward<R>(range), Visit{});
};
template <class R>
concept GraphRange = requires(dagflow::GraphScope& graph, R&& range) {
  graph.parallel_for(std::forward<R>(range), Visit{});
  graph.parallel_for_after(dagflow::JobHandle{}, std::forward<R>(range), Visit{});
};

static_assert(PoolRange<std::vector<int>&> && StealingRange<std::vector<int>&> &&
              GraphRange<std::vector<int>&>);
static_assert(PoolRange<const std::vector<int>&> &&
              StealingRange<const std::vector<int>&> &&
              GraphRange<const std::vector<int>&>);
static_assert(!PoolRange<std::vector<int>> && !StealingRange<std::vector<int>> &&
              !GraphRange<std::vector<int>>);
static_assert(PoolRange<std::span<int>> && StealingRange<std::span<int>> &&
              GraphRange<std::span<int>>);
static_assert(PoolRange<std::forward_list<int>&> &&
              !StealingRange<std::forward_list<int>&> &&
              GraphRange<std::forward_list<int>&>);
using OwningView = decltype(std::views::all(std::vector<int>{}));
static_assert(!PoolRange<OwningView> && !StealingRange<OwningView> &&
              !GraphRange<OwningView>);
using InputRange = std::ranges::istream_view<int>;
static_assert(!PoolRange<InputRange&> && !GraphRange<InputRange&>);

void wait_and_errors(dagflow::Pool& pool) {
  pool.wait_and_rethrow({});
  int result = 0;
  // With one worker the inner wait must help execute the child, not block.
  pool.wait_and_rethrow(pool.submit([&] {
    pool.wait_and_rethrow(pool.submit([&] { result = 42; }));
    CHECK(result == 42);
    pool.wait_and_rethrow(pool.submit([] { throw std::runtime_error("child"); }));
  }));
}

void range_algorithms(dagflow::Pool& pool) {
  std::vector<int> data(40000, 0);
  auto first = pool.for_each(data, [amount = std::make_unique<int>(2)](int& x) {
    x += *amount;
  });
  pool.wait_and_rethrow(first);
  pool.wait_and_rethrow(pool.for_each_ws(std::span(data), [](int& x) { ++x; },
                                       {.priority = dagflow::Priority::High}, 64));
  CHECK(std::ranges::all_of(data, [](int x) { return x == 3; }));

  std::atomic<int> sum{0};
  pool.wait_and_rethrow(pool.for_each(std::as_const(data), [&](const int& x) {
    sum.fetch_add(x, std::memory_order_relaxed);
  }));
  CHECK(sum == 120000);
  pool.wait_and_rethrow(pool.for_each(std::views::iota(0, 10), [&](int x) {
    sum.fetch_add(x, std::memory_order_relaxed);
  }));
  CHECK(sum == 120045);

  // Different iterator/sentinel types, with no requirement for common_range.
  auto prefix = std::ranges::subrange(std::counted_iterator(data.begin(), 7),
                                     std::default_sentinel);
  static_assert(!std::ranges::common_range<decltype(prefix)>);
  pool.wait_and_rethrow(pool.for_each(prefix, [](int& x) { ++x; }));
  pool.wait_and_rethrow(pool.for_each_ws(prefix, [](int& x) { ++x; }, {}, 1));
  CHECK(data[6] == 5 && data[7] == 3);

  std::forward_list<int> linked{1, 2, 3, 4};
  auto partial = linked | std::views::take(3);
  pool.wait_and_rethrow(pool.for_each(partial, [](int& x) { x *= 2; }));
  CHECK((linked == std::forward_list<int>{2, 4, 6, 4}));

  std::vector<int> empty;
  pool.wait_and_rethrow(pool.for_each(empty, [](int&) { CHECK(false); }));
  pool.wait_and_rethrow(pool.for_each_ws(empty, [](int&) { CHECK(false); }));
  auto failed = pool.for_each(data, [](int&) { throw std::runtime_error("range"); });
  pool.wait(failed);  // Existing completion-only contract still does not throw.
  bool caught = false;
  try { pool.wait_and_rethrow(failed); }
  catch (const std::runtime_error& e) { caught = std::string(e.what()) == "range"; }
  CHECK(caught);
}

void graph_ranges(dagflow::Pool& pool) {
  std::vector<int> values(40000), independent(10), empty;
  std::forward_list<int> linked{1, 2, 3};
  auto prefix = std::ranges::subrange(std::counted_iterator(independent.begin(), 5),
                                     std::default_sentinel);
  dagflow::GraphScope graph(pool);  // All borrowed storage outlives the graph.
  auto seed = graph.emplace([&] { std::ranges::fill(values, 1); });
  auto process = graph.parallel_for_after(seed, std::span(values),
                                         [](int& x) { x += 2; }, {.concurrency = 2});
  graph.then(process, [&] {
    CHECK(std::ranges::all_of(values, [](int x) { return x == 3; }));
  });
  graph.parallel_for(prefix, [](int& x) { ++x; });
  graph.parallel_for(linked, [](int& x) { ++x; });
  graph.parallel_for(empty, [](int&) { CHECK(false); });
  graph.parallel_for_after(seed, empty, [](int&) { CHECK(false); });
  graph.run_and_wait();
  graph.run_and_wait();
  CHECK(independent[4] == 2 && independent[5] == 0);
  CHECK((linked == std::forward_list<int>{3, 4, 5}));
}
}  // namespace

int main() {
  for (uint32_t workers : {1u, 4u}) {
    dagflow::Pool pool({.threads = workers, .pin_threads = false});
    bool caught = false;
    try { wait_and_errors(pool); }
    catch (const std::runtime_error& e) { caught = std::string(e.what()) == "child"; }
    CHECK(caught);
    range_algorithms(pool);
    graph_ranges(pool);
  }
}
