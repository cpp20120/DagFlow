#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <iostream>
#include <new>
#include <stdexcept>
#include <string>
#include <vector>

#include <dagflow/task_graph.hpp>
#include <dagflow/thread_pool.hpp>

// Optional instrumentation of ordinary operator new calls in this executable
// and interposed library calls. This does not measure malloc, aligned new, or
// retained memory. Keep disabled for timing comparisons.
// TSan owns the global allocation operators; its interceptors must stay intact.
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define DAGFLOW_BENCH_TSAN 1
#endif
#endif
#if defined(__SANITIZE_THREAD__) || defined(DAGFLOW_BENCH_TSAN)
#define DAGFLOW_BENCH_ALLOCATION_TRACKING 0
#else
#define DAGFLOW_BENCH_ALLOCATION_TRACKING 1
#endif

namespace {
std::atomic<bool> count_allocations{false};
std::atomic<std::size_t> allocations{0}, allocated_bytes{0};
}  // namespace
#if DAGFLOW_BENCH_ALLOCATION_TRACKING
void* operator new(std::size_t size) {
  if (void* ptr = std::malloc(size ? size : 1)) {
    if (count_allocations.load(std::memory_order_relaxed)) {
      allocations.fetch_add(1, std::memory_order_relaxed);
      allocated_bytes.fetch_add(size, std::memory_order_relaxed);
    }
    return ptr;
  }
  throw std::bad_alloc{};
}
void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void* ptr) noexcept { std::free(ptr); }
void operator delete[](void* ptr) noexcept { std::free(ptr); }
void operator delete(void* ptr, std::size_t) noexcept { std::free(ptr); }
void operator delete[](void* ptr, std::size_t) noexcept { std::free(ptr); }
#endif

namespace {
using Clock = std::chrono::steady_clock;
using Microseconds = std::chrono::duration<double, std::micro>;

double percentile(std::vector<double> values, double fraction) {
  std::sort(values.begin(), values.end());
  return values[static_cast<std::size_t>((values.size() - 1) * fraction)];
}

template <class F>
void measure(const char* name, unsigned repeats, bool count, F&& work) {
  work();  // Warm caches and worker threads before measuring.
  std::vector<double> samples;
  samples.reserve(repeats);
  allocations.store(0);
  allocated_bytes.store(0);
  count_allocations.store(count);
  for (unsigned i = 0; i < repeats; ++i) {
    const auto begin = Clock::now();
    work();
    samples.push_back(Microseconds(Clock::now() - begin).count());
  }
  count_allocations.store(false);
  std::cout << name << " median_us=" << percentile(samples, .50)
            << " p95_us=" << percentile(samples, .95)
            << " p99_us=" << percentile(samples, .99);
  if (count)
    std::cout << " ordinary_new_calls/run="
              << static_cast<double>(allocations.load()) / repeats
              << " ordinary_new_bytes/run="
              << static_cast<double>(allocated_bytes.load()) / repeats;
  std::cout << '\n';
}
}  // namespace

int main(int argc, char** argv) {
  try {
    const unsigned threads = argc > 1 ? std::stoul(argv[1]) : 4;
    const std::size_t tasks = argc > 2 ? std::stoull(argv[2]) : 10000;
    const unsigned repeats = argc > 3 ? std::stoul(argv[3]) : 7;
    const bool count = argc > 4 && std::string(argv[4]) == "--allocations";
    if (count && !DAGFLOW_BENCH_ALLOCATION_TRACKING)
      throw std::invalid_argument("--allocations is unavailable under ThreadSanitizer");
    if (!threads || !tasks || !repeats)
      throw std::invalid_argument(
          "threads, tasks, and repeats must be positive");
    dagflow::Config cfg;
    cfg.threads = threads;
    cfg.shards = threads;
    cfg.central_batch = 32;
    cfg.pin_threads = false;
    dagflow::Pool pool(cfg);
    std::cout << std::fixed << std::setprecision(2) << "threads=" << threads
              << " tasks=" << tasks << " repeats=" << repeats
              << " central_batch=32 pin_threads=false"
              << " allocations=" << count << '\n';

    // Each graph is rebuilt so this also works with single-run runtimes.
    // Construction/seal/run/destruction are included in these two timings.
    std::atomic<std::size_t> completed{0};
    measure("dag_chain_build_run", repeats, count, [&] {
      completed.store(0);
      dagflow::TaskGraph graph(pool);
      auto previous = graph.emplace([&] { completed.fetch_add(1); });
      for (std::size_t i = 1; i < tasks; ++i) {
        const auto next = graph.emplace([&] { completed.fetch_add(1); });
        graph.add_edge(previous, next);
        previous = next;
      }
      pool.wait(graph.run());
      pool.wait_idle();
      if (completed.load() != tasks)
        throw std::runtime_error("chain lost work");
    });
    measure("dag_fanout_fanin_build_run", repeats, count, [&] {
      completed.store(0);
      dagflow::TaskGraph graph(pool);
      const auto root = graph.emplace([] {});
      const auto join = graph.emplace([&] {
        if (completed.load() != tasks) std::abort();
      });
      for (std::size_t i = 0; i < tasks; ++i) {
        const auto leaf = graph.emplace([&] { completed.fetch_add(1); });
        graph.add_edge(root, leaf);
        graph.add_edge(leaf, join);
      }
      pool.wait(graph.run());
      pool.wait_idle();
    });
    measure("external_detached", repeats, count, [&] {
      for (std::size_t i = 0; i < tasks; ++i) pool.submit_detached([] {});
      pool.wait_idle();
    });
    measure("external_handles", repeats, count, [&] {
      std::vector<dagflow::Handle> handles;
      handles.reserve(tasks);
      for (std::size_t i = 0; i < tasks; ++i)
        handles.push_back(pool.submit([] {}));
      for (const auto& handle : handles) pool.wait(handle);
      pool.wait_idle();
    });
    measure("nested_spawn", repeats, count, [&] {
      const auto parent = pool.submit([&] {
        for (std::size_t i = 0; i < tasks; ++i) pool.submit_detached([] {});
      });
      pool.wait(parent);
      pool.wait_idle();
    });

    // Queueing latency with sustained local work; finite chain keeps this
    // benchmark usable on schedulers that do not service external work fairly.
    std::vector<double> latencies(tasks);
    measure("external_burst_with_local_spawn", repeats, count, [&] {
      std::atomic<bool> started{false};
      std::atomic<std::size_t> remaining{tasks * 4};
      std::function<void()> spawn;
      spawn = [&] {
        started.store(true, std::memory_order_release);
        started.notify_one();
        if (remaining.fetch_sub(1) > 1) pool.submit_detached(spawn);
      };
      pool.submit_detached(spawn);
      started.wait(false, std::memory_order_acquire);
      for (std::size_t i = 0; i < tasks; ++i) {
        const auto submitted = Clock::now();
        pool.submit_detached([&, submitted, i] {
          latencies[i] = Microseconds(Clock::now() - submitted).count();
        });
      }
      pool.wait_idle();
    });
    std::cout << "last_burst_latency p50_us=" << percentile(latencies, .50)
              << " p95_us=" << percentile(latencies, .95)
              << " p99_us=" << percentile(latencies, .99) << '\n';
  } catch (const std::exception& error) {
    std::cerr << error.what()
              << "\nusage: dagflow_runtime_bench [threads] [tasks] [repeats] "
                 "[--allocations]\n";
    return 1;
  }
}
