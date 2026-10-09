#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <dagflow/dagflow.hpp>
#include <dagflow/detail/runtime_diagnostics.hpp>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <dagflow/graph_scope.hpp>
#include <dagflow/task_graph.hpp>

namespace dagflow {
class Pool;
}
namespace {
using Clock = std::chrono::steady_clock;
using Seconds = std::chrono::duration<double>;
using dagflow::detail::RuntimeCounts;
using dagflow::detail::RuntimeEvent;

struct Options {
  std::string benchmark;
  std::uint32_t workers = 4;
  std::uint32_t runs = 5;
  std::uint32_t warmup = 1;
  std::uint32_t payload_rounds = 100;
  std::uint32_t seed = 0x9e3779b9u;
};

[[noreturn]] void usage(const char* argv0) {
  std::cerr
      << "usage: " << argv0
      << " --benchmark chain|independent|independent-batch|parallel-for|workflow|noop"
         " [--workers N] [--runs N] [--warmup N]\n";
  std::exit(2);
}

std::uint64_t parse_u64(std::string_view text) {
  std::size_t used = 0;
  auto value = std::stoull(std::string(text), &used);
  if (used != text.size()) throw std::invalid_argument("invalid integer");
  return value;
}

Options parse(int argc, char** argv) {
  Options result;
  for (int i = 1; i < argc; ++i) {
    const std::string_view key = argv[i];
    if (i + 1 >= argc) usage(argv[0]);
    const std::string_view value = argv[++i];
    if (key == "--benchmark") {
      result.benchmark = value;
    } else if (key == "--workers") {
      result.workers = static_cast<std::uint32_t>(parse_u64(value));
    } else if (key == "--runs") {
      result.runs = static_cast<std::uint32_t>(parse_u64(value));
    } else if (key == "--warmup") {
      result.warmup = static_cast<std::uint32_t>(parse_u64(value));
    } else if (key == "--payload-rounds") {
      result.payload_rounds = static_cast<std::uint32_t>(parse_u64(value));
    } else if (key == "--seed") {
      result.seed = static_cast<std::uint32_t>(parse_u64(value));
    } else {
      usage(argv[0]);
    }
  }
  if (result.benchmark.empty() || !result.workers || !result.runs ||
      !result.payload_rounds)
    usage(argv[0]);
  return result;
}

std::size_t event_index(RuntimeEvent event) {
  return static_cast<std::size_t>(event);
}

RuntimeCounts delta(const RuntimeCounts& before, const RuntimeCounts& after) {
  RuntimeCounts result{};
  for (std::size_t i = 0; i < result.size(); ++i) result[i] = after[i] - before[i];
  return result;
}

struct Result {
  std::string_view label;
  std::string_view unit;
  std::uint64_t logical_units{};
  std::vector<double> samples_s;
  RuntimeCounts counts{};
  std::uint64_t checksum{};
};

template <class Setup, class Work, class Check>
Result measure(std::string_view label, std::string_view unit,
               std::uint64_t logical_units, const Options& options,
               Setup&& setup, Work&& work, Check&& check) {
  for (std::uint32_t i = 0; i < options.warmup; ++i) {
    setup();
    work();
    check();
  }

  Result result{label, unit, logical_units};
  result.samples_s.reserve(options.runs);
  for (std::uint32_t i = 0; i < options.runs; ++i) {
    setup();
    const auto before_counts = dagflow::detail::runtime_counts();
    const auto begin = Clock::now();
    work();
    const auto end = Clock::now();
    const auto after_counts = dagflow::detail::runtime_counts();
    check();
    result.samples_s.push_back(Seconds(end - begin).count());
    const auto d = delta(before_counts, after_counts);
    for (std::size_t n = 0; n < result.counts.size(); ++n) result.counts[n] += d[n];
  }
  return result;
}

std::uint64_t fold_double(double value) {
  const auto scaled = std::llround(std::abs(value) * 1'000'000.0);
  return static_cast<std::uint64_t>(scaled);
}

double heavy_value(int i) {
  double x = 0.0;
  for (int j = 0; j < 1'000'000; ++j) {
    x += std::sin(static_cast<double>(i + j)) *
         std::cos(static_cast<double>(i - j));
  }
  return x;
}

Result chain(dagflow::Pool& pool, const Options& options) {
  constexpr std::size_t tasks = 1000;
  std::vector<double> values(tasks);
  return measure(
      "Dependent chain (1,000 tasks)", "task", tasks, options,
      [&] { std::fill(values.begin(), values.end(), 0.0); },
      [&] {
        dagflow::TaskGraph graph(pool);
        auto previous = graph.emplace([&] { values[0] = 1.0; });
        for (std::size_t i = 1; i < tasks; ++i) {
          auto next = graph.emplace([&, i] {
            values[i] = values[i - 1] + std::sqrt(static_cast<double>(i));
          });
          graph.add_edge(previous, next);
          previous = next;
        }
        pool.wait_and_rethrow(graph.run());
        pool.wait_idle();
      },
      [&] {
        if (!(values.back() > 1.0) || !std::isfinite(values.back()))
          throw std::runtime_error("chain validation failed");
      });
}

Result independent(dagflow::Pool& pool, const Options& options, bool batched) {
  constexpr std::size_t tasks = 1000;
  constexpr std::size_t batch_size = 10;
  std::vector<double> values(tasks);

  struct Job {
    double* values{};
    int index{};
    void operator()() const { values[index] = heavy_value(index); }
  };

  return measure(
      batched ? "Independent batched (1,000, b=10)"
              : "Independent tasks (1,000)",
      "task", tasks, options,
      [&] { std::fill(values.begin(), values.end(), 0.0); },
      [&] {
        if (!batched) {
          for (std::size_t i = 0; i < tasks; ++i) {
            pool.submit_detached(Job{values.data(), static_cast<int>(i)});
          }
        } else {
          std::array<Job, batch_size> jobs{};
          for (std::size_t base = 0; base < tasks; base += batch_size) {
            const auto count = std::min(batch_size, tasks - base);
            for (std::size_t j = 0; j < count; ++j)
              jobs[j] = Job{values.data(), static_cast<int>(base + j)};
            pool.submit_batch_detached(std::span<Job>(jobs.data(), count));
          }
        }
        pool.wait_idle();
      },
      [&] {
        double sum = std::accumulate(values.begin(), values.end(), 0.0);
        if (!std::isfinite(sum) || sum == 0.0)
          throw std::runtime_error("independent validation failed");
      });
}

Result parallel_for(dagflow::Pool& pool, const Options& options) {
  constexpr std::size_t elements = 1'000'000;
  std::vector<std::uint32_t> data(elements);
  std::uint32_t run_seed = options.seed;

  return measure(
      "Parallel_for (1,000,000 elements)", "element", elements, options,
      [&] {
        std::iota(data.begin(), data.end(), 0u);
        run_seed = run_seed * 1664525u + 1013904223u;
      },
      [&] {
        const auto seed = run_seed;
        const auto rounds = options.payload_rounds;
        auto handle = pool.for_each_ws(data, [seed, rounds](std::uint32_t& value) {
          std::uint32_t x = value ^ seed;
          for (std::uint32_t k = 0; k < rounds; ++k) {
            x ^= x >> 16;
            x *= 0x7feb352du;
            x ^= x >> 15;
            x *= 0x846ca68bu;
            x ^= x >> 16;
            x += k + 0x9e3779b9u;
          }
          value = x;
        });
        pool.wait_and_rethrow(handle);
        pool.wait_idle();
      },
      [&] {
        std::uint64_t checksum = 0xcbf29ce484222325ULL;
        for (const auto value : data) {
          checksum ^= value;
          checksum *= 0x100000001b3ULL;
        }
        if (checksum == 0)
          throw std::runtime_error("parallel_for validation failed");
      });
}

Result workflow(dagflow::Pool& pool, const Options& options) {
  constexpr std::size_t width = 10;
  constexpr std::size_t depth = 5;
  constexpr std::size_t tasks = width * depth;
  std::array<std::array<double, width>, depth> matrix{};

  return measure(
      "Workflow (width=10, depth=5)", "task", tasks, options,
      [&] {
        for (auto& row : matrix) row.fill(0.0);
      },
      [&] {
        dagflow::GraphScope graph(pool);
        std::vector<dagflow::JobHandle> previous;
        previous.reserve(width);
        for (std::size_t i = 0; i < width; ++i) {
          previous.push_back(graph.emplace(
              [&, i] { matrix[0][i] = std::sin(static_cast<double>(i)); }));
        }
        for (std::size_t d = 1; d < depth; ++d) {
          std::vector<dagflow::JobHandle> next;
          next.reserve(width);
          for (std::size_t i = 0; i < width; ++i) {
            next.push_back(graph.when_all(
                std::span<const dagflow::JobHandle>(previous), [&, d, i] {
                  double sum = 0.0;
                  for (double x : matrix[d - 1]) sum += x;
                  matrix[d][i] = sum / static_cast<double>(width) +
                                 static_cast<double>(d * i);
                }));
          }
          previous = std::move(next);
        }
        graph.run_and_wait();
        pool.wait_idle();
      },
      [&] {
        double sum = 0.0;
        for (const auto& row : matrix)
          for (double x : row) sum += x;
        if (!std::isfinite(sum) || sum == 0.0)
          throw std::runtime_error("workflow validation failed");
      });
}

Result noop(dagflow::Pool& pool, const Options& options) {
  constexpr std::size_t tasks = 1'000'000;
  return measure(
      "Noop tasks (1,000,000)", "task", tasks, options,
      [] {},
      [&] {
        for (std::size_t i = 0; i < tasks; ++i) pool.submit_detached([] {});
        pool.wait_idle();
      },
      [] {});
}

void print_json(const Result& result, const Options& options) {
  const double mean = std::accumulate(result.samples_s.begin(), result.samples_s.end(), 0.0) /
                      static_cast<double>(result.samples_s.size());
  const auto [min_it, max_it] =
      std::minmax_element(result.samples_s.begin(), result.samples_s.end());
  auto mean_count = [&](RuntimeEvent event) {
    return static_cast<double>(result.counts[event_index(event)]) /
           static_cast<double>(options.runs);
  };

  std::cout << std::setprecision(17)
            << "{\"benchmark\":" << std::quoted(std::string(result.label))
            << ",\"unit\":" << std::quoted(std::string(result.unit))
            << ",\"logical_units\":" << result.logical_units
            << ",\"workers\":" << options.workers
            << ",\"runs\":" << options.runs
            << ",\"warmup\":" << options.warmup
            << ",\"payload_rounds\":" << options.payload_rounds
            << ",\"seed\":" << options.seed
            << ",\"diagnostics\":"
            << (dagflow::detail::runtime_diagnostics_enabled ? "true" : "false")
            << ",\"mean_s\":" << mean << ",\"min_s\":" << *min_it
            << ",\"max_s\":" << *max_it
            << ",\"throughput_per_s\":"
            << static_cast<double>(result.logical_units) / mean
            << ",\"mean_ns_per_unit\":"
            << mean * 1e9 / static_cast<double>(result.logical_units)
            << ",\"runtime_allocations_per_run\":"
            << mean_count(RuntimeEvent::memory_allocations)
            << ",\"runtime_requested_bytes_per_run\":"
            << mean_count(RuntimeEvent::memory_requested_bytes)
            << ",\"packet_allocations_per_run\":"
            << mean_count(RuntimeEvent::packet_allocations)
            << ",\"packet_bytes_per_run\":"
            << mean_count(RuntimeEvent::packet_allocated_bytes)
            << ",\"cross_thread_frees_per_run\":"
            << mean_count(RuntimeEvent::cross_thread_free)
            << ",\"samples_s\":[";
  for (std::size_t i = 0; i < result.samples_s.size(); ++i) {
    if (i) std::cout << ',';
    std::cout << result.samples_s[i];
  }
  std::cout << "]}\n";
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const auto options = parse(argc, argv);
    dagflow::Pool pool({.threads = options.workers,
                        .shards = options.workers,
                        .pin_threads = false});
    // Make thread startup/parking initialization happen before scenario warmup.
    pool.submit_detached([] {});
    pool.wait_idle();

    Result result;
    if (options.benchmark == "chain")
      result = chain(pool, options);
    else if (options.benchmark == "independent")
      result = independent(pool, options, false);
    else if (options.benchmark == "independent-batch")
      result = independent(pool, options, true);
    else if (options.benchmark == "parallel-for")
      result = parallel_for(pool, options);
    else if (options.benchmark == "workflow")
      result = workflow(pool, options);
    else if (options.benchmark == "noop")
      result = noop(pool, options);
    else
      usage(argv[0]);

    print_json(result, options);
  } catch (const std::exception& error) {
    std::cerr << "public API benchmark failed: " << error.what() << '\n';
    return 1;
  }
}
