#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "runtime_suite_backend.hpp"

namespace {
using Clock = std::chrono::steady_clock;
using Us = std::chrono::duration<double, std::micro>;
constexpr std::string_view scenarios[] = {
    "local_saturated", "steal_heavy",  "external_detached", "external_handles",
    "idle_burst",      "nested_spawn", "scope_recursive",   "fanout_fanin",
    "deep_dag",        "uneven",       "graph_reuse",       "dag_build_run",
    "graph_tokens_serial", "graph_tokens_parallel"};

struct Options {
  unsigned shards = 0;
  unsigned submit_batch = 0, central_batch = 32;
  unsigned workers = 4, repeats = 15, warmup = 3, sample_stride = 16;
  std::size_t tasks = 4096;
  std::uint64_t iterations = 0, work_ns = 0, idle_us = 2000;
  bool fixed_iterations = false, latency = false, calibrate = false,
       fresh_graph = false,
       list = false;
  std::string scenario = "all";
};

std::uint64_t number(std::string_view s) {
  std::uint64_t value{};
  auto [end, error] = std::from_chars(s.data(), s.data() + s.size(), value);
  if (error != std::errc{} || end != s.data() + s.size() || s.empty())
    throw std::invalid_argument("invalid unsigned integer: " + std::string(s));
  return value;
}

Options parse(int argc, char** argv) {
  Options o;
  for (int i = 1; i < argc; ++i) {
    const std::string_view key = argv[i];
    if (key == "--fresh-graph") {
      o.fresh_graph = true;
      continue;
    }
    if (key == "--latency") {
      o.latency = true;
      continue;
    }
    if (key == "--calibrate") {
      o.calibrate = true;
      continue;
    }
    if (key == "--list") {
      o.list = true;
      continue;
    }
    if (key == "--help") {
      std::cout << "dagflow-runtime-suite [--scenario NAME|all] [--workers N] "
                   "[--shards N] [--submit-batch 0..64] [--central-batch N] "
                   "[--tasks N] [--repeats N] [--warmup N] [--work-ns N] "
                   "[--iterations N] [--latency] [--sample-stride N] "
                   "[--idle-us N] [--fresh-graph] [--list] [--calibrate]\n"
                   "Outputs JSONL. --iterations fixes work across builds; "
                   "--work-ns otherwise calibrates approximately.\n";
      std::exit(0);
    }
    if (++i == argc) throw std::invalid_argument("missing option value");
    if (key == "--scenario") {
      o.scenario = argv[i];
      continue;
    }
    const auto n = number(argv[i]);
    if (key == "--tasks")
      o.tasks = n;
    else if (key == "--iterations") {
      o.iterations = n;
      o.fixed_iterations = true;
    } else if (key == "--work-ns")
      o.work_ns = n;
    else if (key == "--idle-us")
      o.idle_us = n;
    else if (key == "--submit-batch" || key == "--central-batch" ||
             key == "--shards" || key == "--workers" || key == "--repeats" ||
             key == "--warmup" || key == "--sample-stride") {
      if (n > std::numeric_limits<unsigned>::max())
        throw std::invalid_argument("option exceeds unsigned range");
      if (key == "--workers") o.workers = n;
      if (key == "--shards") o.shards = n;
      if (key == "--submit-batch") o.submit_batch = n;
      if (key == "--central-batch") o.central_batch = n;
      if (key == "--repeats") o.repeats = n;
      if (key == "--warmup") o.warmup = n;
      if (key == "--sample-stride") o.sample_stride = n;
    } else
      throw std::invalid_argument("unknown option: " + std::string(key));
  }
  if (!o.workers || !o.tasks || !o.repeats || !o.sample_stride ||
      !o.central_batch || o.submit_batch > 64 ||
      o.tasks > UINT32_MAX - 2 || o.iterations > UINT64_MAX / 32 ||
      o.idle_us > 60'000'000 || o.work_ns > 1'000'000'000 ||
      o.warmup > UINT32_MAX - o.repeats)
    throw std::invalid_argument("invalid size, count, work or idle interval");
  if (o.scenario != "all" &&
      std::find(std::begin(scenarios), std::end(scenarios), o.scenario) ==
          std::end(scenarios))
    throw std::invalid_argument("unknown scenario: " + o.scenario);
  return o;
}

// A data-dependent integer chain. Results escape into checked output slots;
// no clock reads or shared atomic increment in the ordinary payload.
std::uint64_t burn(std::uint64_t value, std::uint64_t iterations) {
  for (std::uint64_t i = 0; i < iterations; ++i) {
    value ^= value >> 12;
    value ^= value << 25;
    value ^= value >> 27;
    value *= 0x2545f4914f6cdd1dULL;
  }
  return value | 1;
}
volatile std::uint64_t calibration_sink;
double calibrate() {
  std::vector<double> samples;
  for (int i = 0; i < 7; ++i) {
    auto begin = Clock::now();
    calibration_sink = burn(0x12345678ULL + i, 200'000);
    samples.push_back(
        std::chrono::duration<double, std::nano>(Clock::now() - begin).count() /
        200'000);
  }
  std::sort(samples.begin(), samples.end());
  return samples[samples.size() / 2];
}

double percentile(const std::vector<double>& sorted, double fraction) {
  if (sorted.empty()) return 0;
  // Nearest rank, including the maximum for a small-sample p999.
  const auto rank =
      static_cast<std::size_t>(std::ceil(sorted.size() * fraction));
  return sorted[std::max<std::size_t>(1, rank) - 1];
}

struct Work {
  const Options& options;
  bool uneven;
  std::vector<std::uint64_t> results, expected;
  std::vector<double> latency;
  Clock::time_point graph_started;

  Work(const Options& o, bool skew)
      : options(o),
        uneven(skew),
        results(o.tasks),
        expected(o.tasks),
        latency(o.tasks) {
    for (std::size_t i = 0; i < o.tasks; ++i)
      expected[i] = burn(i + 1, iterations(i));
  }
  std::uint64_t iterations(std::size_t i) const {
    return options.iterations * (uneven && i % 16 == 0 ? 32 : 1);
  }
  bool sampled(std::size_t i) const {
    return options.latency && i % options.sample_stride == 0;
  }
  Clock::time_point submitted(std::size_t i) const {
    return sampled(i) ? Clock::now() : Clock::time_point{};
  }
  void execute(std::size_t i, Clock::time_point submitted_at) {
    if (sampled(i)) latency[i] = Us(Clock::now() - submitted_at).count();
    results[i] = burn(i + 1, iterations(i));
  }
  auto task(std::size_t i) {
    return [this, i, at = submitted(i)] { execute(i, at); };
  }
  void reset() {
    std::fill(results.begin(), results.end(), 0);
    std::fill(latency.begin(), latency.end(), -1);
  }
  void verify(std::vector<double>* samples) const {
    if (results != expected)
      throw std::runtime_error("missing or incorrect task result");
    if (options.latency) {
      for (std::size_t i = 0; i < options.tasks; i += options.sample_stride) {
        if (latency[i] < 0) throw std::runtime_error("missing latency sample");
        if (samples) samples->push_back(latency[i]);
      }
    }
  }
};

#ifndef DAGFLOW_SUITE_LEGACY
void require(bool accepted) {
  if (!accepted) throw std::runtime_error("unexpected scope rejection");
}
void scope_tree(dagflow::TaskScope::Context& context, Work& w, std::size_t lo,
                std::size_t hi, Clock::time_point at) {
  w.execute(lo, at);
  const auto mid = lo + 1 + (hi - lo - 1) / 2;
  if (lo + 1 < mid) {
    const auto child_at = w.submitted(lo + 1);
    require(context.spawn(
        [&w, lo, mid, child_at](dagflow::TaskScope::Context& child) {
          scope_tree(child, w, lo + 1, mid, child_at);
        }));
  }
  if (mid < hi) {
    const auto child_at = w.submitted(mid);
    require(context.spawn(
        [&w, mid, hi, child_at](dagflow::TaskScope::Context& child) {
          scope_tree(child, w, mid, hi, child_at);
        }));
  }
}
#endif
void pool_tree(suite_backend::Pool& pool, Work& w, std::size_t lo, std::size_t hi,
               Clock::time_point at) {
  w.execute(lo, at);
  const auto mid = lo + 1 + (hi - lo - 1) / 2;
  dagflow::Handle a, b;
  if (lo + 1 < mid) {
    const auto child_at = w.submitted(lo + 1);
    a = pool.submit([&pool, &w, lo, mid, child_at] {
      pool_tree(pool, w, lo + 1, mid, child_at);
    });
  }
  if (mid < hi) {
    const auto child_at = w.submitted(mid);
    b = pool.submit([&pool, &w, mid, hi, child_at] {
      pool_tree(pool, w, mid, hi, child_at);
    });
  }
  pool.wait(a);
  pool.wait(b);
  suite_backend::rethrow(a);
  suite_backend::rethrow(b);
}

void build_graph(suite_backend::TaskGraph& graph, Work& w, bool chain, bool fan) {
  suite_backend::TaskGraph::NodeId previous{}, root{}, join{};
  if (fan) {
    root = graph.emplace([] {});
    join = graph.emplace([] {});
  }
  for (std::size_t i = 0; i < w.options.tasks; ++i) {
    auto node = graph.emplace([&w, i] { w.execute(i, w.graph_started); });
    if (chain && i) graph.add_edge(previous, node);
    if (fan) {
      graph.add_edge(root, node);
      graph.add_edge(node, join);
    }
    previous = node;
  }
  if (!graph.seal()) throw std::runtime_error("invalid benchmark graph");
}

void run_scenario(suite_backend::Pool& pool, const Options& o,
                  std::string_view name) {
  if (name == "steal_heavy" && o.workers < 2) {
    std::cout
        << "{\"schema\":1,\"scenario\":\"steal_heavy\",\"status\":\"skipped\","
           "\"reason\":\"requires at least two workers\",\"workers\":1}\n";
    return;
  }
#ifdef DAGFLOW_SUITE_LEGACY
  if (name == "scope_recursive") {
    std::cout << "{\"schema\":1,\"scenario\":\"scope_recursive\","
                 "\"status\":\"skipped\",\"reason\":"
                 "\"legacy TaskScope has no recursive spawn API\"}\n";
    return;
  }
#endif
  Work w(o, name == "uneven");
  std::atomic<std::size_t> remaining{0};
  std::atomic<std::size_t> token_index{0};
  // This guard must retire every outstanding borrow before Work/counters die,
  // including partial submission failures in a detached batch.
  struct Drain {
    suite_backend::Pool& pool;
    ~Drain() { pool.wait_idle(); }
  } drain{pool};
  const bool token_graph = name == "graph_tokens_serial" ||
                           name == "graph_tokens_parallel";
  const bool graph_case = token_graph || name == "deep_dag" ||
                          name == "fanout_fanin" || name == "graph_reuse" ||
                          name == "dag_build_run";
  std::unique_ptr<suite_backend::TaskGraph> graph;
  auto prepare_graph = [&] {
    if (!graph_case || name == "dag_build_run") return;
    graph = std::make_unique<suite_backend::TaskGraph>(pool);
    if (token_graph) {
      suite_backend::TaskGraph::NodeOptions opt;
      opt.concurrency = name == "graph_tokens_serial" ? 1 : 0;
      // The public no-argument node API does not expose token indices. This
      // benchmark's relaxed ticket selects a unique verification slot; include
      // its cost in both versions, not in claims about pure graph overhead.
      auto node = graph->emplace([&] {
        const auto i = token_index.fetch_add(1, std::memory_order_relaxed);
        if (i >= o.tasks) std::abort();
        w.execute(i, w.graph_started);
      }, opt);
      graph->set_tokens(node, o.tasks);
      if (!graph->seal()) throw std::runtime_error("invalid token graph");
    } else {
      build_graph(*graph, w, name == "deep_dag", name == "fanout_fanin");
    }
  };
  if (!o.fresh_graph) prepare_graph();
  std::vector<dagflow::Handle> handles;
  handles.reserve(o.tasks);
  std::vector<decltype(w.task(0))> batch;
  batch.reserve(o.submit_batch);
  std::vector<double> times, latencies;
  times.reserve(o.repeats);
  if (o.latency)
    latencies.reserve(((o.tasks - 1) / o.sample_stride + 1) * o.repeats);
  dagflow::detail::RuntimeCounts diagnostic_totals{};
  for (unsigned run = 0; run < o.warmup + o.repeats; ++run) {
    // For runtimes without graph reuse, compare identical freshly built graphs.
    // Construction/destruction are excluded here and included in dag_build_run.
    if (o.fresh_graph) prepare_graph();
    w.reset();
    token_index.store(0, std::memory_order_relaxed);
    handles.clear();
    if (name == "idle_burst")
      std::this_thread::sleep_for(std::chrono::microseconds(o.idle_us));
    // Diagnostic snapshots bracket only the workload and drained packet
    // cleanup. Ordinary builds compile these calls away. Diagnostic timings
    // are deliberately not comparable with uninstrumented timings.
    const auto diagnostics_before = dagflow::detail::runtime_counts();
    const auto start = Clock::now();
    if (graph_case) {
      if (name == "dag_build_run") {
        suite_backend::TaskGraph temporary(pool);
        build_graph(temporary, w, true, false);
        w.graph_started = Clock::now();
        auto result = temporary.run();
        pool.wait(result);
        suite_backend::rethrow_graph(temporary, result);
      } else {
        w.graph_started = Clock::now();
        auto result = graph->run();
        pool.wait(result);
        suite_backend::rethrow_graph(*graph, result);
      }
#ifndef DAGFLOW_SUITE_LEGACY
    } else if (name == "scope_recursive") {
      dagflow::TaskScope scope(pool);
      require(scope.spawn(
          [&w, at = w.submitted(0)](dagflow::TaskScope::Context& ctx) {
            scope_tree(ctx, w, 0, w.options.tasks, at);
          }));
      scope.join();
#endif
    } else if (name == "nested_spawn") {
      auto result = pool.submit([&pool, &w, at = w.submitted(0)] {
        pool_tree(pool, w, 0, w.options.tasks, at);
      });
      pool.wait(result);
      suite_backend::rethrow(result);
    } else if (name == "local_saturated" || name == "uneven") {
      auto result = pool.submit([&] {
        for (std::size_t i = 0; i < o.tasks; ++i)
          pool.submit_detached(w.task(i));
      });
      pool.wait(result);
      suite_backend::rethrow(result);
    } else if (name == "steal_heavy") {
      // Keep the producer worker occupied while thieves drain each local batch.
      // Small batches avoid the inline overflow path while the producer waits.
      auto result = pool.submit([&] {
        for (std::size_t base = 0; base < o.tasks; base += 256) {
          const auto end = std::min(o.tasks, base + 256);
          remaining.store(end - base, std::memory_order_relaxed);
          for (auto i = base; i < end; ++i) {
            pool.submit_detached([&, i, at = w.submitted(i)] {
              w.execute(i, at);
              remaining.fetch_sub(1, std::memory_order_release);
            });
          }
          while (remaining.load(std::memory_order_acquire))
            std::this_thread::yield();
        }
      });
      pool.wait(result);
      suite_backend::rethrow(result);
    } else if (o.submit_batch &&
               (name == "external_detached" || name == "idle_burst")) {
      for (std::size_t base = 0; base < o.tasks; base += o.submit_batch) {
        batch.clear();
        const auto end = std::min(o.tasks, base + o.submit_batch);
        for (auto i = base; i < end; ++i) batch.push_back(w.task(i));
        pool.submit_batch_detached(std::span{batch});
      }
    } else {
      for (std::size_t i = 0; i < o.tasks; ++i) {
        if (name == "external_handles")
          handles.push_back(pool.submit(w.task(i)));
        else
          pool.submit_detached(w.task(i));
      }
      for (auto& handle : handles) {
        pool.wait(handle);
        suite_backend::rethrow(handle);
      }
    }
    // Include packet teardown and finish before reusing output slots. TaskScope
    // readiness may precede the final pool bookkeeping operation.
    pool.wait_idle();
    const auto elapsed = Us(Clock::now() - start).count();
    const bool measured = run >= o.warmup;
    const auto diagnostics_after = dagflow::detail::runtime_counts();
    if (measured)
      for (std::size_t i = 0; i < diagnostic_totals.size(); ++i)
        diagnostic_totals[i] += diagnostics_after[i] - diagnostics_before[i];
    w.verify(measured ? &latencies : nullptr);
    if (token_graph && token_index.load(std::memory_order_relaxed) != o.tasks)
      throw std::runtime_error("token count mismatch");
    if (measured) times.push_back(elapsed);
  }
  const double total_us = std::accumulate(times.begin(), times.end(), 0.0);
  auto sorted = times;
  std::sort(sorted.begin(), sorted.end());
  std::sort(latencies.begin(), latencies.end());
  const auto checksum =
      std::accumulate(w.results.begin(), w.results.end(), std::uint64_t{0});
  std::cout << std::setprecision(12)
            << "{\"schema\":1,\"status\":\"ok\",\"scenario\":\"" << name
            << "\",\"backend\":\"" << suite_backend::name
            << "\",\"allocator\":\"" << dagflow::detail::allocator_backend()
            << "\",\"workers\":" << o.workers
            << ",\"shards\":" << (o.shards ? o.shards : o.workers)
            << ",\"submit_batch\":"
            << ((name == "external_detached" || name == "idle_burst") ? o.submit_batch : 0)
            << ",\"central_batch\":" << o.central_batch
            << ",\"fresh_graph\":" << (o.fresh_graph ? "true" : "false")
            << ",\"tasks\":" << o.tasks << ",\"repeats\":" << o.repeats
            << ",\"warmup\":" << o.warmup << ",\"work_ns\":" << o.work_ns
            << ",\"iterations\":" << o.iterations
            << ",\"idle_us\":" << o.idle_us
            << ",\"sample_stride\":" << o.sample_stride << ",\"mode\":\""
            << (o.latency ? "latency" : "throughput")
            << "\",\"payload_tasks_per_second\":"
            << (o.tasks * double(o.repeats) * 1e6 / total_us)
            << ",\"run_p50_us\":" << percentile(sorted, .50)
            << ",\"run_p99_us\":" << percentile(sorted, .99)
            << ",\"run_p999_us\":" << percentile(sorted, .999)
            << ",\"latency_kind\":\""
            << (graph_case ? "run_to_start" : "submit_to_start")
            << "\",\"latency_samples\":" << latencies.size()
            << ",\"latency_p50_us\":";
  if (latencies.empty())
    std::cout << "null,\"latency_p99_us\":null,\"latency_p999_us\":null";
  else
    std::cout << percentile(latencies, .50)
              << ",\"latency_p99_us\":" << percentile(latencies, .99)
              << ",\"latency_p999_us\":" << percentile(latencies, .999);
  std::cout << ",\"checksum\":" << checksum << ",\"run_us\":[";
  for (std::size_t i = 0; i < times.size(); ++i) {
    if (i) std::cout << ',';
    std::cout << times[i];
  }
  std::cout << "],\"diagnostics\":";
  if constexpr (dagflow::detail::runtime_diagnostics_enabled) {
    std::cout << '{';
    for (std::size_t i = 0; i < diagnostic_totals.size(); ++i) {
      if (i) std::cout << ',';
      std::cout << '\"' << dagflow::detail::runtime_event_names[i] << "\":"
                << diagnostic_totals[i];
    }
    std::cout << '}';
  } else {
    std::cout << "null";
  }
  std::cout << "}\n";
}
}  // namespace

int main(int argc, char** argv) {
  try {
    auto options = parse(argc, argv);
    if (options.list) {
      for (auto s : scenarios) std::cout << s << '\n';
      return 0;
    }
    if (options.calibrate) {
      std::cout << std::setprecision(12)
                << "{\"ns_per_iteration\":" << calibrate() << "}\n";
      return 0;
    }
    if (!options.fixed_iterations && options.work_ns)
      options.iterations = std::max<std::uint64_t>(
          1, std::llround(options.work_ns / calibrate()));
    dagflow::Config config;
    config.threads = options.workers;
    config.shards = options.shards;
    config.central_batch = options.central_batch;
    config.pin_threads = false;
    suite_backend::Pool pool(config);
    // Unwind safely on failures: the pool itself does not provide draining
    // destruction. Scope/graph locals drain their own accepted tasks first.
    struct Drain {
      suite_backend::Pool& pool;
      ~Drain() { pool.wait_idle(); }
    } drain{pool};
    for (auto scenario : scenarios)
      if (options.scenario == "all" || options.scenario == scenario)
        run_scenario(pool, options, scenario);
  } catch (const std::exception& error) {
    std::cerr << "runtime suite: " << error.what() << '\n';
    return 1;
  }
}
