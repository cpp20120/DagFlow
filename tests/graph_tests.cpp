#include <atomic>
#include <deque>
#include <forward_list>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <vector>

#include <dagflow/dagflow.hpp>
#include "support.hpp"
#include <dagflow/graph_scope.hpp>

void graph_dependencies(dagflow::Pool& pool) {
  dagflow::TaskGraph graph(pool);
  CHECK(graph.empty());
  int root_value = 0;
  std::vector<int> results(8);
  auto root = graph.emplace([&] { root_value = 7; });
  auto join = graph.emplace(
      [&] { CHECK(std::accumulate(results.begin(), results.end(), 0) == 56); });
  for (std::size_t i = 0; i < results.size(); ++i) {
    auto branch = graph.emplace([&, i] { results[i] = root_value; });
    graph.add_edge(root, branch);
    graph.add_edge(branch, join);
  }
  CHECK(graph.size() == 10 && graph.seal());
  pool.wait(graph.run());
  CHECK(!graph.last_error());
}

void graph_tokens_and_errors(dagflow::Pool& pool) {
  {
    dagflow::TaskGraph graph(pool);
    CHECK(!graph.run());
    int calls = 0;
    auto node = graph.add_node([&] { ++calls; });  // Compatibility API.
    graph.set_tokens(node, 128);
    pool.wait(graph.run());
    CHECK(calls == 128);
  }
  {
    dagflow::TaskGraph graph(pool);
    auto a = graph.emplace([] {}), b = graph.emplace([] {});
    graph.add_edge(a, b);
    graph.add_edge(b, a);
    CHECK(!graph.seal());
    bool threw = false;
    try {
      graph.run();
    } catch (const std::logic_error&) {
      threw = true;
    }
    CHECK(threw);
    graph.clear();
    CHECK(graph.empty());
  }
  {
    dagflow::GraphScope scope(pool);
    scope.emplace([] { throw std::runtime_error("node error"); });
    bool threw = false;
    try {
      scope.run_and_wait();
    } catch (const std::runtime_error&) {
      threw = true;
    }
    CHECK(threw && scope.last_error());
  }
}

void scope_building(dagflow::Pool& pool) {
  int a_value = 0, b_value = 0;
  dagflow::GraphScope scope(pool);
  CHECK(scope.empty());
  auto a = scope.emplace([&] { a_value = 2; });
  auto b = scope.then(a, [&] { b_value = a_value + 3; });
  scope.when_all({a, b}, [&] { CHECK(a_value + b_value == 7); });
  CHECK(a && b && a != b && scope.size() == 3);
  scope.run_and_wait();

  std::vector<int> values(50000);
  auto captured = std::make_shared<int>(9);
  dagflow::GraphScope parallel(pool);
  parallel.parallel_for(values.begin(), values.end(),
                        [captured](int& value) { value = *captured; });
  parallel.run_and_wait();
  for (auto value : values) CHECK(value == 9);
}

void pool_algorithms(dagflow::Pool& pool) {
  std::forward_list<int> forward(100, 1);
  pool.wait(pool.for_each(forward.begin(), forward.end(), [](int& n) { ++n; }));
  for (int n : forward) CHECK(n == 2);
  std::deque<int> segmented(100000, 3);
  pool.wait(pool.for_each_ws(segmented.begin(), segmented.end(),
                             [](int& n) { n *= 2; }));
  for (int n : segmented) CHECK(n == 6);
  auto a = pool.submit([] {}), b = pool.submit([] {});
  pool.wait(pool.combine({a, b}));
  pool.wait(pool.combine({a, dagflow::Handle{}, b}));
  CHECK(!pool.combine({}));
}

void graph_capacity_and_reuse(dagflow::Pool& pool) {
  using Graph = dagflow::TaskGraph;
  for (auto policy :
       {Graph::Overflow::Block, Graph::Overflow::Drop, Graph::Overflow::Fail}) {
    Graph graph(pool);
    std::atomic<int> calls{0};
    int observed = -1;
    Graph::NodeOptions opt;
    opt.capacity = 2;
    opt.concurrency = 4;
    opt.overflow = policy;
    auto a = graph.emplace([&] { calls.fetch_add(1); }, opt);
    graph.set_tokens(a, 257);
    auto b = graph.emplace([&] { observed = calls.load(); });
    graph.add_edge(a, b);
    for (int run = 0; run < 3; ++run) {
      calls = 0;
      observed = -1;
      pool.wait(graph.run());
      if (policy == Graph::Overflow::Fail) {
        CHECK(graph.last_error());
        CHECK(observed == -1);
      } else {
        CHECK(!graph.last_error());
        CHECK(observed == (policy == Graph::Overflow::Block ? 257 : 2));
      }
      graph.reset();
    }
    graph.clear();
    CHECK(graph.empty());
  }
  for (auto policy : {Graph::Overflow::Drop, Graph::Overflow::Fail}) {
    Graph graph(pool);
    Graph::NodeOptions opt;
    opt.capacity = 0;
    opt.overflow = policy;
    int calls = 0;
    auto a = graph.emplace([&] { ++calls; }, opt);
    auto b = graph.emplace([&] { calls += 10; });
    graph.add_edge(a, b);
    pool.wait(graph.run());
    CHECK(calls == (policy == Graph::Overflow::Drop ? 10 : 0));
  }
  Graph graph(pool);
  Graph::NodeOptions invalid;
  invalid.capacity = 0;
  bool rejected = false;
  try {
    graph.emplace([] {}, invalid);
  } catch (const std::invalid_argument&) {
    rejected = true;
  }
  CHECK(rejected);
  auto a = graph.emplace([] {}), b = graph.emplace([] {});
  graph.add_edge(a, b);
  CHECK(graph.seal());
  graph.add_edge(b, a);
  CHECK(!graph.seal());
}

void graph_barrier_and_cancellation(dagflow::Pool& pool) {
  using Graph = dagflow::TaskGraph;
  for (int repeat = 0; repeat < 30; ++repeat) {
    Graph graph(pool);
    std::atomic<int> finished{0}, active{0}, peak{0};
    Graph::NodeOptions opt;
    opt.concurrency = 4;
    opt.capacity = 2;
    auto a = graph.emplace(
        [&] {
          int running = active.fetch_add(1) + 1;
          auto prev = peak.load();
          while (prev < running && !peak.compare_exchange_weak(prev, running)) {
          }
          std::this_thread::yield();
          ++finished;
          --active;
        },
        opt);
    graph.set_tokens(a, 1024);
    auto b = graph.emplace([&] {
      CHECK(finished == 1024);
      CHECK(active == 0);
    });
    graph.add_edge(a, b);
    pool.wait(graph.run());
    CHECK(peak <= 2);
  }
  Graph graph(pool);
  int calls = 0;
  auto a = graph.emplace([&] {
    ++calls;
    throw std::runtime_error("cancel");
  });
  graph.set_tokens(a, SIZE_MAX);
  auto b = graph.emplace([] { CHECK(false); });
  graph.add_edge(a, b);
  pool.wait(graph.run());
  CHECK(calls == 1 && graph.last_error());
  pool.wait(graph.run());
  CHECK(calls == 2 && graph.last_error());
}

void graph_explicit_cancel_and_retry(dagflow::Pool& pool) {
  dagflow::TaskGraph graph(pool);
  int calls = 0;
  bool fail = true;
  auto a = graph.emplace([&] {
    ++calls;
    if (fail) throw std::runtime_error("first run fails");
  });
  graph.set_tokens(a, 8);
  pool.wait(graph.run());
  CHECK(calls == 1 && graph.last_error());
  fail = false;
  pool.wait(graph.run());
  CHECK(calls == 9 && !graph.last_error());
  graph.clear();
  calls = 0;
  a = graph.emplace([&] {
    ++calls;
    graph.cancel();
  });
  graph.set_tokens(a, SIZE_MAX);
  auto b = graph.emplace([] { CHECK(false); });
  graph.add_edge(a, b);
  pool.wait(graph.run());
  CHECK(calls == 1 && !graph.last_error());
  graph.clear();

  // Mutation and reentrant run are rejected while a callable is active.
  graph.emplace([&] {
    int rejected = 0;
    try {
      graph.clear();
    } catch (const std::logic_error&) {
      ++rejected;
    }
    try {
      graph.reset();
    } catch (const std::logic_error&) {
      ++rejected;
    }
    try {
      graph.run();
    } catch (const std::logic_error&) {
      ++rejected;
    }
    CHECK(rejected == 3);
  });
  pool.wait(graph.run());
}

void graph_cooperative_destruction_and_chain() {
  dagflow::Config cfg;
  cfg.threads = 1;
  cfg.pin_threads = false;
  dagflow::Pool pool(cfg);
  int calls = 0;
  pool.wait(pool.submit([&] {
    dagflow::TaskGraph graph(pool);
    auto previous = graph.emplace([&] { ++calls; });
    for (int i = 1; i < 10000; ++i) {
      auto next = graph.emplace([&] { ++calls; });
      graph.add_edge(previous, next);
      previous = next;
    }
    graph.run();
    // Destructor must help the only worker execute the graph.
  }));
  CHECK(calls == 10000);
}

void scope_repeated_parallel_for(dagflow::Pool& pool) {
  std::vector<int> values(100000, 0);
  int joins = 0;
  {
    dagflow::GraphScope scope(pool);
    dagflow::ScheduleOptions opt;
    opt.concurrency = 4;
    auto work = scope.parallel_for(
        values.begin(), values.end(), [](int& n) { ++n; }, opt);
    scope.then(work, [&] {
      ++joins;
      for (auto n : values) CHECK(n == joins);
    });
    scope.run_and_wait();
    scope.run_and_wait();
    scope.wait();  // Waiting twice does not start an implicit extra run.
  }
  CHECK(joins == 2);  // Nor does the destructor rerun completed work.
  for (auto n : values) CHECK(n == 2);
  int implicit = 0;
  {
    dagflow::GraphScope scope(pool);
    scope.emplace([&] { ++implicit; });
  }
  CHECK(implicit == 1);

  // The helper preserves the caller's admission policy instead of replacing
  // capacity with the number of chunks.
  std::vector<int> dropped(32768, 0);
  dagflow::GraphScope bounded(pool);
  dagflow::ScheduleOptions options;
  options.concurrency = 4;
  options.capacity = 1;
  options.overflow = dagflow::TaskGraph::Overflow::Drop;
  bounded.parallel_for(
      dropped.begin(), dropped.end(), [](int& n) { ++n; }, options);
  bounded.run_and_wait();
  CHECK(std::accumulate(dropped.begin(), dropped.end(), 0) == 16384);
}

void graph_recompile_and_callable_ownership(dagflow::Pool& pool) {
  using Graph = dagflow::TaskGraph;
  Graph graph(pool);
  int observed = 0;
  auto lifetime = std::make_shared<int>(0);
  std::weak_ptr<int> weak = lifetime;
  auto root = graph.emplace(
      [state = std::move(lifetime), &observed] { observed = ++*state; });
  CHECK(graph.seal());
  auto first = graph.run();
  pool.wait(first);
  CHECK(observed == 1);
  // Re-sealing and resetting preserve mutable callable state.
  CHECK(graph.seal());
  graph.reset();
  pool.wait(graph.run());
  CHECK(observed == 2 && first.ready());
  graph.set_tokens(root, 3);
  CHECK(graph.seal());
  pool.wait(graph.run());
  CHECK(observed == 5);

  int joined = 0;
  auto join = graph.emplace([&] { joined = observed; });
  // Parallel edges each contribute to indegree, but activate the join once.
  graph.add_edge(root, join);
  graph.add_edge(root, join);
  CHECK(graph.seal());
  pool.wait(graph.run());
  CHECK(joined == 8);
  graph.set_tokens(root, 0);
  pool.wait(graph.run());
  CHECK(joined == 9);

  // Pending and compiled callables survive unsuccessful cycle validation.
  auto pending = std::make_shared<int>(42);
  std::weak_ptr<int> pending_weak = pending;
  auto extra =
      graph.emplace([state = std::move(pending)] { CHECK(*state == 42); });
  graph.add_edge(join, extra);
  graph.add_edge(extra, root);
  CHECK(!graph.seal() && !weak.expired() && !pending_weak.expired());
  graph.clear();
  CHECK(graph.empty() && weak.expired() && pending_weak.expired());
  auto again = graph.emplace([&] { ++observed; });
  CHECK(again.idx == 0);
  pool.wait(graph.run());
  CHECK(observed == 10 && first.ready());
}

void graph_compiled_options_and_indices(dagflow::Pool& pool) {
  using Graph = dagflow::TaskGraph;
  for (int concurrency : {-1, 0, 1, INT_MAX}) {
    Graph graph(pool);
    Graph::NodeOptions opt;
    opt.concurrency = concurrency;
    opt.capacity = 3;
    opt.overflow = Graph::Overflow::Drop;
    opt.priority = dagflow::Priority::High;
    opt.affinity = UINT32_MAX;  // Placement remains a scheduler hint.
    std::atomic<int> calls{0};
    auto root = graph.emplace([&] { ++calls; }, opt);
    graph.set_tokens(root, SIZE_MAX);
    int observed = 0;
    auto join = graph.emplace([&] { observed = calls.load(); });
    graph.add_edge(root, join);
    CHECK(graph.seal());
    pool.wait(graph.run());
    CHECK(observed == 3 && !graph.last_error());
    graph.set_tokens(root, 2);
    pool.wait(graph.run());
    CHECK(observed == 5);
  }
  for (bool bad_priority : {false, true}) {
    Graph graph(pool);
    Graph::NodeOptions opt;
    if (bad_priority)
      opt.priority = static_cast<dagflow::Priority>(255);
    else
      opt.overflow = static_cast<Graph::Overflow>(255);
    graph.emplace([] {}, opt);
    bool rejected = false;
    try {
      (void)graph.seal();
    } catch (const std::invalid_argument&) {
      rejected = true;
    }
    CHECK(rejected);
  }
  Graph graph(pool);
  int calls = 0;
  auto root = graph.emplace([&] { ++calls; });
  CHECK(graph.seal());
  std::vector<std::size_t> invalid_ids{std::size_t{UINT32_MAX}, SIZE_MAX};
  if constexpr (SIZE_MAX > UINT32_MAX)
    invalid_ids.push_back(std::size_t{UINT32_MAX} + 1);
  for (auto invalid : invalid_ids) {
    int rejected = 0;
    try {
      graph.add_edge(root, {invalid});
    } catch (const std::out_of_range&) {
      ++rejected;
    }
    try {
      graph.add_edge({invalid}, root);
    } catch (const std::out_of_range&) {
      ++rejected;
    }
    try {
      graph.set_tokens({invalid}, 4);
    } catch (const std::out_of_range&) {
      ++rejected;
    }
    CHECK(rejected == 3);
  }
  pool.wait(graph.run());
  CHECK(calls == 1);
}

void graph_lane_boundaries_and_visibility(dagflow::Pool& pool) {
  using Graph = dagflow::TaskGraph;
  for (int concurrency : {1, 4}) {
    for (std::size_t tokens : {1, 63, 64, 65, 127, 128, 129}) {
      Graph graph(pool);
      std::atomic<int> calls{0}, active{0};
      int serial_value = 0, joined = 0;
      Graph::NodeOptions opt;
      opt.concurrency = concurrency;
      auto root = graph.emplace([&] {
        if (concurrency == 1) {
          CHECK(active.fetch_add(1) == 0);
          // The only lane may migrate on continuation or enter helping, but
          // must never overlap another invocation of this mutable callable.
          const auto before = serial_value;
          pool.wait(pool.submit([] {}));
          serial_value = before + 1;
          CHECK(active.fetch_sub(1) == 1);
        }
        ++calls;
      }, opt);
      graph.set_tokens(root, tokens);
      std::vector<int> values(16);
      auto join = graph.emplace([&] {
        for (auto value : values) CHECK(value == calls.load());
        if (concurrency == 1) CHECK(serial_value == calls.load());
        ++joined;
      });
      for (std::size_t i = 0; i < values.size(); ++i) {
        auto branch = graph.emplace([&, i] { values[i] = calls.load(); });
        graph.add_edge(root, branch);
        graph.add_edge(branch, join);
        if (i % 2 == 0) graph.add_edge(branch, join);  // Duplicate arrivals.
      }
      for (int run = 1; run <= 3; ++run) {
        auto done = graph.run();
        pool.wait(done);
        done.rethrow_if_failed();
        CHECK(calls == int(tokens * run) && joined == run);
      }
    }
  }
}

void graph_cancel_before_activation_and_reseal(dagflow::Pool& pool) {
  dagflow::TaskGraph graph(pool);
  bool cancel = true;
  int calls = 0, joined = 0;
  auto root = graph.emplace([&] { if (cancel) graph.cancel(); });
  auto child = graph.emplace([&] { ++calls; });
  graph.set_tokens(child, 129);
  auto join = graph.emplace([&] { joined = calls; });
  graph.add_edge(root, child);
  graph.add_edge(child, join);
  for (int run = 0; run < 8; ++run) {
    cancel = run % 2 == 0;
    pool.wait(graph.run());
    CHECK(!graph.last_error());
    CHECK(calls == (run + 1) / 2 * 129 && joined == calls);
  }
  // A former root becomes a child at re-seal: the compiled root list must be
  // rebuilt, otherwise the original root and its descendants activate twice.
  int root_calls = 0;
  auto new_root = graph.emplace([&] { ++root_calls; });
  graph.add_edge(new_root, root);
  pool.wait(graph.run());
  CHECK(root_calls == 1 && calls == 5 * 129 && joined == calls);
}

void graph_budget_allows_priority_progress() {
  dagflow::Config cfg;
  cfg.threads = 1;
  cfg.pin_threads = false;
  dagflow::Pool pool(cfg);
  // A token loop and a chain of zero-token nodes must both yield. Empty nodes
  // consume transition budget even though they invoke no callable.
  for (bool empty_chain : {false, true}) {
    dagflow::TaskGraph graph(pool);
    bool urgent_ran = false;
    int calls = 0;
    auto previous = graph.emplace([&] {
      if (++calls == 1)
        pool.submit_detached([&] { urgent_ran = true; },
                             {.priority = dagflow::Priority::High});
    });
    if (empty_chain) {
      dagflow::TaskGraph::NodeOptions opt;
      opt.capacity = 0;
      opt.overflow = dagflow::TaskGraph::Overflow::Drop;
      for (int i = 0; i < 128; ++i) {
        auto next = graph.emplace([] { CHECK(false); }, opt);
        graph.add_edge(previous, next);
        previous = next;
      }
    } else {
      graph.set_tokens(previous, 129);
    }
    auto join = graph.emplace([&] { CHECK(urgent_ran); });
    graph.add_edge(previous, join);
    pool.wait(graph.run());
    CHECK(!graph.last_error() && calls == (empty_chain ? 1 : 129));
  }
}

int main() {
  dagflow::Config config;
  config.threads = 4;
  config.pin_threads = false;
  dagflow::Pool pool(config);
  graph_dependencies(pool);
  graph_tokens_and_errors(pool);
  scope_building(pool);
  pool_algorithms(pool);
  graph_capacity_and_reuse(pool);
  graph_barrier_and_cancellation(pool);
  graph_explicit_cancel_and_retry(pool);
  graph_cooperative_destruction_and_chain();
  scope_repeated_parallel_for(pool);
  graph_recompile_and_callable_ownership(pool);
  graph_compiled_options_and_indices(pool);
  graph_lane_boundaries_and_visibility(pool);
  graph_cancel_before_activation_and_reseal(pool);
  graph_budget_allows_priority_progress();
}
