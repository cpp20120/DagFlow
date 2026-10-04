#include <tbb/blocked_range.h>
#include <tbb/flow_graph.h>
#include <tbb/global_control.h>
#include <tbb/parallel_for.h>
#include <tbb/task_arena.h>
#include <tbb/task_group.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {
using Clock = std::chrono::steady_clock;
using Seconds = std::chrono::duration<double>;
namespace flow = tbb::flow;

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
      << " --benchmark chain|independent|parallel-for|workflow|noop"
         " [--workers N] [--runs N] [--warmup N]"
         " [--payload-rounds N] [--seed N]\n";
  std::exit(2);
}

std::uint64_t parse_u64(std::string_view text) {
  std::size_t used = 0;
  const auto value = std::stoull(std::string(text), &used, 0);
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

struct Result {
  std::string_view key;
  std::string_view label;
  std::string_view unit;
  std::uint64_t logical_units{};
  std::vector<double> samples_s;
  std::uint64_t checksum{};
};

double percentile(std::vector<double> values, double q) {
  if (values.empty()) return 0.0;
  std::ranges::sort(values);
  return values[static_cast<std::size_t>((values.size() - 1) * q)];
}

template <class Setup, class Work, class Check>
Result measure(std::string_view key, std::string_view label,
               std::string_view unit, std::uint64_t logical_units,
               const Options& options, Setup&& setup, Work&& work,
               Check&& check) {
  for (std::uint32_t i = 0; i < options.warmup; ++i) {
    setup();
    work();
    check();
  }

  Result result{key, label, unit, logical_units};
  result.samples_s.reserve(options.runs);
  for (std::uint32_t i = 0; i < options.runs; ++i) {
    setup();
    const auto begin = Clock::now();
    work();
    const auto end = Clock::now();
    result.checksum = check();
    result.samples_s.push_back(Seconds(end - begin).count());
  }
  return result;
}

double heavy_value(int i) {
  double x = 0.0;
  for (int j = 0; j < 1'000'000; ++j) {
    x += std::sin(static_cast<double>(i + j)) *
         std::cos(static_cast<double>(i - j));
  }
  return x;
}

std::uint32_t mix_payload(std::uint32_t value, std::uint32_t seed,
                          std::uint32_t rounds) noexcept {
  std::uint32_t x = value ^ seed;
  for (std::uint32_t k = 0; k < rounds; ++k) {
    x ^= x >> 16;
    x *= 0x7feb352du;
    x ^= x >> 15;
    x *= 0x846ca68bu;
    x ^= x >> 16;
    x += k + 0x9e3779b9u;
  }
  return x;
}

Result chain(const Options& options) {
  constexpr std::size_t tasks = 1000;
  std::vector<double> values(tasks);

  return measure(
      "chain", "Dependent chain (1,000 tasks)", "task", tasks, options,
      [&] { std::fill(values.begin(), values.end(), 0.0); },
      [&] {
        flow::graph graph;
        flow::broadcast_node<flow::continue_msg> start(graph);
        using Node = flow::continue_node<flow::continue_msg>;
        std::vector<std::unique_ptr<Node>> nodes;
        nodes.reserve(tasks);

        nodes.push_back(std::make_unique<Node>(
            graph, [&](flow::continue_msg) { values[0] = 1.0; }));
        for (std::size_t i = 1; i < tasks; ++i) {
          nodes.push_back(std::make_unique<Node>(graph, [&, i](flow::continue_msg) {
            values[i] = values[i - 1] + std::sqrt(static_cast<double>(i));
          }));
        }
        flow::make_edge(start, *nodes[0]);
        for (std::size_t i = 1; i < tasks; ++i)
          flow::make_edge(*nodes[i - 1], *nodes[i]);

        start.try_put(flow::continue_msg{});
        graph.wait_for_all();
      },
      [&] -> std::uint64_t {
        if (!(values.back() > 1.0) || !std::isfinite(values.back()))
          throw std::runtime_error("chain validation failed");
        return static_cast<std::uint64_t>(std::llround(values.back()));
      });
}

Result independent(const Options& options) {
  constexpr std::size_t tasks = 1000;
  std::vector<double> values(tasks);
  tbb::task_group group;

  return measure(
      "independent", "Independent tasks (1,000)", "task", tasks, options,
      [&] { std::fill(values.begin(), values.end(), 0.0); },
      [&] {
        for (std::size_t i = 0; i < tasks; ++i) {
          group.run([&, i] { values[i] = heavy_value(static_cast<int>(i)); });
        }
        group.wait();
      },
      [&] -> std::uint64_t {
        const double sum = std::accumulate(values.begin(), values.end(), 0.0);
        if (!std::isfinite(sum) || sum == 0.0)
          throw std::runtime_error("independent validation failed");
        return static_cast<std::uint64_t>(std::llround(std::abs(sum) * 1'000'000.0));
      });
}

Result parallel_for(const Options& options) {
  constexpr std::size_t elements = 1'000'000;
  std::vector<std::uint32_t> data(elements);
  std::uint32_t run_seed = options.seed;

  return measure(
      "parallel-for", "Parallel_for (1,000,000 elements)", "element", elements,
      options,
      [&] {
        std::iota(data.begin(), data.end(), 0u);
        run_seed = run_seed * 1664525u + 1013904223u;
      },
      [&] {
        const auto seed = run_seed;
        const auto rounds = options.payload_rounds;
        tbb::parallel_for(
            tbb::blocked_range<std::size_t>(0, elements),
            [&](const tbb::blocked_range<std::size_t>& range) {
              for (std::size_t i = range.begin(); i != range.end(); ++i)
                data[i] = mix_payload(data[i], seed, rounds);
            });
      },
      [&] -> std::uint64_t {
        std::uint64_t checksum = 0xcbf29ce484222325ULL;
        for (const auto value : data) {
          checksum ^= value;
          checksum *= 0x100000001b3ULL;
        }
        if (checksum == 0) throw std::runtime_error("parallel_for validation failed");
        return checksum;
      });
}

Result workflow(const Options& options) {
  constexpr std::size_t width = 10;
  constexpr std::size_t depth = 5;
  constexpr std::size_t tasks = width * depth;
  std::array<std::array<double, width>, depth> matrix{};

  return measure(
      "workflow", "Workflow (width=10, depth=5)", "task", tasks, options,
      [&] {
        for (auto& row : matrix) row.fill(0.0);
      },
      [&] {
        flow::graph graph;
        flow::broadcast_node<flow::continue_msg> start(graph);
        using Node = flow::continue_node<flow::continue_msg>;
        std::vector<std::unique_ptr<Node>> nodes;
        nodes.reserve(tasks);

        auto at = [&](std::size_t d, std::size_t i) -> Node& {
          return *nodes[d * width + i];
        };

        for (std::size_t d = 0; d < depth; ++d) {
          for (std::size_t i = 0; i < width; ++i) {
            if (d == 0) {
              nodes.push_back(std::make_unique<Node>(graph, [&, i](flow::continue_msg) {
                matrix[0][i] = std::sin(static_cast<double>(i));
              }));
            } else {
              nodes.push_back(std::make_unique<Node>(graph, [&, d, i](flow::continue_msg) {
                double sum = 0.0;
                for (double x : matrix[d - 1]) sum += x;
                matrix[d][i] = sum / static_cast<double>(width) +
                               static_cast<double>(d * i);
              }));
            }
          }
        }

        for (std::size_t i = 0; i < width; ++i) flow::make_edge(start, at(0, i));
        for (std::size_t d = 1; d < depth; ++d)
          for (std::size_t p = 0; p < width; ++p)
            for (std::size_t i = 0; i < width; ++i)
              flow::make_edge(at(d - 1, p), at(d, i));

        start.try_put(flow::continue_msg{});
        graph.wait_for_all();
      },
      [&] -> std::uint64_t {
        double sum = 0.0;
        for (const auto& row : matrix)
          for (double x : row) sum += x;
        if (!std::isfinite(sum) || sum == 0.0)
          throw std::runtime_error("workflow validation failed");
        return static_cast<std::uint64_t>(std::llround(std::abs(sum) * 1'000'000.0));
      });
}

Result noop(const Options& options) {
  constexpr std::size_t tasks = 1'000'000;
  tbb::task_group group;

  return measure(
      "noop", "Noop tasks (1,000,000)", "task", tasks, options,
      [] {},
      [&] {
        for (std::size_t i = 0; i < tasks; ++i) group.run([] {});
        group.wait();
      },
      [] -> std::uint64_t { return 1; });
}

Result dispatch(const Options& options) {
  if (options.benchmark == "chain") return chain(options);
  if (options.benchmark == "independent") return independent(options);
  if (options.benchmark == "parallel-for") return parallel_for(options);
  if (options.benchmark == "workflow") return workflow(options);
  if (options.benchmark == "noop") return noop(options);
  usage("tbb-public-api-bench");
}

void print_json(const Result& result, const Options& options, int arena_concurrency) {
  const double mean =
      std::accumulate(result.samples_s.begin(), result.samples_s.end(), 0.0) /
      static_cast<double>(result.samples_s.size());
  const auto [min_it, max_it] =
      std::minmax_element(result.samples_s.begin(), result.samples_s.end());
  const double median = percentile(result.samples_s, 0.5);

  std::cout << std::setprecision(17)
            << "{\"schema\":1,\"runtime\":\"oneTBB\",\"key\":"
            << std::quoted(std::string(result.key))
            << ",\"benchmark\":" << std::quoted(std::string(result.label))
            << ",\"unit\":" << std::quoted(std::string(result.unit))
            << ",\"logical_units\":" << result.logical_units
            << ",\"workers\":" << options.workers
            << ",\"arena_concurrency\":" << arena_concurrency
            << ",\"runs\":" << options.runs
            << ",\"warmup\":" << options.warmup
            << ",\"payload_rounds\":" << options.payload_rounds
            << ",\"seed\":" << options.seed
            << ",\"mean_s\":" << mean
            << ",\"median_s\":" << median
            << ",\"min_s\":" << *min_it
            << ",\"max_s\":" << *max_it
            << ",\"throughput_per_s\":"
            << static_cast<double>(result.logical_units) / mean
            << ",\"mean_ns_per_unit\":"
            << mean * 1e9 / static_cast<double>(result.logical_units)
            << ",\"checksum\":" << result.checksum
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

    // oneTBB max_allowed_parallelism=N allows at most N-1 worker threads;
    // the application thread can occupy the remaining execution slot.
    tbb::global_control control(tbb::global_control::max_allowed_parallelism,
                                options.workers);
    tbb::task_arena arena(static_cast<int>(options.workers));
    arena.initialize();

    // Force lazy scheduler/arena initialization before benchmark warmups.
    arena.execute([] {
      tbb::task_group group;
      group.run([] {});
      group.wait();
    });

    const auto result = arena.execute([&] { return dispatch(options); });
    print_json(result, options, arena.max_concurrency());
  } catch (const std::exception& error) {
    std::cerr << "oneTBB public API benchmark failed: " << error.what() << '\n';
    return 1;
  }
}
