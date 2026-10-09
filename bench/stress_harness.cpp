// Stress/measurement harness. Runtime algorithms are intentionally independent
// of these controls. See docs/benchmarks/main-harness.md for timing boundaries.
#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <dagflow/dagflow.hpp>
#include <dagflow/detail/runtime_diagnostics.hpp>
#ifdef __linux__
#include <poll.h>
#include <sched.h>
#include <sys/resource.h>
#include <unistd.h>

#include <cerrno>
#endif

namespace {
using Clock = std::chrono::steady_clock;
using Counts = dagflow::detail::RuntimeCounts;
constexpr std::array scenarios{
    std::string_view{"external-contention"}, std::string_view{"external-batch"},
    std::string_view{"hot-shard-skew"},      std::string_view{"local-overflow"},
    std::string_view{"nested-helping"},      std::string_view{"mixed-chaos"},
    std::string_view{"idle-burst"}};
constexpr uint64_t seed_bias = 0x9e3779b97f4a7c15ULL;
constexpr uint64_t mix_multiplier_1 = 0xbf58476d1ce4e5b9ULL;
constexpr uint64_t mix_multiplier_2 = 0x94d049bb133111ebULL;
constexpr uint64_t nested_seed_mask = 0xdeadbeefULL;
constexpr uint64_t mixed_seed_mask = 0xa0761d6478bd642fULL;
thread_local volatile uint64_t sink = 0;

struct Options {
  std::string scenario = "all", verify = "checksum",
              producer_mode = "persistent";
  std::string placement = "spread", cpus, worker_cpus, producer_cpus;
  uint32_t workers = 0, producers = 2, shards = 0, central_batch = 1024;
  uint32_t repeats = 7, warmup = 2, bursts = 64, warmup_ms = 200, min_ms = 0;
  uint32_t max_samples = 100000, batch = 0, fanout = 4, high_every = 8;
  uint32_t idle_us = 10000, sample_stride = 64;
  std::size_t tasks = 0;
  std::optional<uint32_t> iterations;
  bool json = false, latency = false;
  int control_fd = -1, ack_fd = -1;
};
uint64_t number(std::string_view value) {
  uint64_t result{};
  const auto [end, ec] =
      std::from_chars(value.data(), value.data() + value.size(), result);
  if (ec != std::errc{} || end != value.data() + value.size() || value.empty())
    throw std::invalid_argument("invalid integer: " + std::string(value));
  return result;
}
Options parse(int argc, char** argv) {
  Options o;
  for (int i = 1; i < argc; ++i) {
    const std::string_view key = argv[i];
    if (key == "--json") {
      o.json = true;
      continue;
    }
    if (key == "--latency") {
      o.latency = true;
      continue;
    }
    if (key == "--help" || key == "--list") {
      std::cout
          << "dagflow-example [--scenario NAME|all] [--workers N] [--producers "
             "N]\n"
             "  --shards N --central-batch N --tasks N --iterations N "
             "--submit-batch 0..64\n"
             "  --warmup N --warmup-ms N --repeats N --min-ms N --max-samples "
             "N\n"
             "  --bursts N --idle-us N --fanout N --high-every N (0 disables "
             "high-priority roots)\n"
             "  --producer-mode persistent|fresh --placement spread|hot|none\n"
             "  --verify checksum|exact|off --latency --sample-stride N "
             "--json\n"
             "  --cpus LIST --worker-cpus LIST --producer-cpus LIST (Linux CPU "
             "IDs)\n"
             "  --perf-control-fd N --perf-ack-fd N (perf --control fd:... -D "
             "-1)\n"
             "Defaults: workers=1 then 4, producers=2 independent of workers. "
             "Scenarios:\n";
      for (auto name : scenarios) std::cout << "  " << name << '\n';
      std::exit(0);
    }
    if (++i == argc)
      throw std::invalid_argument("missing value for " + std::string(key));
    const std::string value = argv[i];
    if (key == "--scenario")
      o.scenario = value;
    else if (key == "--verify")
      o.verify = value;
    else if (key == "--producer-mode")
      o.producer_mode = value;
    else if (key == "--placement")
      o.placement = value;
    else if (key == "--cpus")
      o.cpus = value;
    else if (key == "--worker-cpus")
      o.worker_cpus = value;
    else if (key == "--producer-cpus")
      o.producer_cpus = value;
    else {
      const auto n = number(value);
      if (n > UINT32_MAX)
        throw std::invalid_argument("option exceeds uint32_t");
      if (key == "--workers") {
        if (!n) throw std::invalid_argument("workers must be positive");
        o.workers = n;
      } else if (key == "--producers")
        o.producers = n;
      else if (key == "--shards")
        o.shards = n;
      else if (key == "--central-batch")
        o.central_batch = n;
      else if (key == "--tasks") {
        if (!n) throw std::invalid_argument("tasks must be positive");
        o.tasks = n;
      } else if (key == "--iterations")
        o.iterations = n;
      else if (key == "--repeats")
        o.repeats = n;
      else if (key == "--warmup")
        o.warmup = n;
      else if (key == "--warmup-ms")
        o.warmup_ms = n;
      else if (key == "--min-ms")
        o.min_ms = n;
      else if (key == "--max-samples")
        o.max_samples = n;
      else if (key == "--bursts")
        o.bursts = n;
      else if (key == "--idle-us")
        o.idle_us = n;
      else if (key == "--submit-batch")
        o.batch = n;
      else if (key == "--fanout")
        o.fanout = n;
      else if (key == "--high-every")
        o.high_every = n;
      else if (key == "--sample-stride")
        o.sample_stride = n;
      else if (key == "--perf-control-fd" && n <= INT32_MAX)
        o.control_fd = n;
      else if (key == "--perf-ack-fd" && n <= INT32_MAX)
        o.ack_fd = n;
      else
        throw std::invalid_argument("unknown option: " + std::string(key));
    }
  }
  if ((o.scenario != "all" &&
       std::ranges::find(scenarios, o.scenario) == scenarios.end()) ||
      !o.producers || o.producers > 256 || o.workers > 256 || o.shards > 256 ||
      !o.repeats || !o.bursts || !o.central_batch ||
      o.central_batch > DAGFLOW_LOCAL_QUEUE_CAPACITY || !o.sample_stride ||
      o.batch > 64 || o.fanout > 64 || o.tasks > 10'000'000 ||
      o.idle_us > 10'000'000 || o.warmup_ms > 60000 || o.min_ms > 60000 ||
      !o.max_samples || o.max_samples > 1'000'000 ||
      o.repeats > o.max_samples || o.bursts > o.max_samples ||
      o.warmup > o.max_samples ||
      (o.verify != "checksum" && o.verify != "exact" && o.verify != "off") ||
      (o.producer_mode != "persistent" && o.producer_mode != "fresh") ||
      (o.placement != "spread" && o.placement != "hot" &&
       o.placement != "none") ||
      ((o.control_fd < 0) != (o.ack_fd < 0)))
    throw std::invalid_argument("invalid harness configuration (see --help)");
  return o;
}

std::vector<int> cpu_list(std::string_view value) {
  std::vector<int> result;
  while (!value.empty()) {
    const auto comma = value.find(',');
    const auto part = value.substr(0, comma);
    const auto dash = part.find('-');
    const auto lo = number(part.substr(0, dash));
    const auto hi = dash == part.npos ? lo : number(part.substr(dash + 1));
    if (lo > hi || hi >= 1024) throw std::invalid_argument("invalid CPU range");
    for (auto cpu = lo; cpu <= hi; ++cpu)
      result.push_back(static_cast<int>(cpu));
    if (comma == value.npos) break;
    value.remove_prefix(comma + 1);
    if (value.empty())
      throw std::invalid_argument("trailing comma in CPU list");
  }
  std::ranges::sort(result);
  if (std::adjacent_find(result.begin(), result.end()) != result.end())
    throw std::invalid_argument("duplicate CPU ID");
  return result;
}
std::vector<int> allowed_cpus() {
  std::vector<int> cpus;
#ifdef __linux__
  cpu_set_t set;
  if (sched_getaffinity(0, sizeof(set), &set))
    throw std::runtime_error("sched_getaffinity failed");
  for (int i = 0; i < CPU_SETSIZE; ++i)
    if (CPU_ISSET(i, &set)) cpus.push_back(i);
#endif
  return cpus;
}
void bind_cpus(const std::vector<int>& cpus) {
#ifdef __linux__
  if (cpus.empty()) return;
  cpu_set_t set;
  CPU_ZERO(&set);
  for (int cpu : cpus) CPU_SET(cpu, &set);
  if (sched_setaffinity(0, sizeof(set), &set))
    throw std::runtime_error("sched_setaffinity failed");
#else
  if (!cpus.empty()) throw std::invalid_argument("CPU affinity requires Linux");
#endif
}
std::string cpu_text(const std::vector<int>& cpus) {
  std::string result;
  for (int cpu : cpus) {
    if (!result.empty()) result += ',';
    result += std::to_string(cpu);
  }
  return result;
}

// Perf owns the opposite ends. Acknowledgements ensure warmup/validation cannot
// leak into the next enabled phase. Counter windows include control overhead;
// wall-clock samples start after enable ack and end before disable is sent.
class PerfControl {
 public:
  explicit PerfControl(const Options& o)
      : control_(o.control_fd), ack_(o.ack_fd) {}
  void set(bool enabled) {
    if (control_ < 0) return;
#ifdef __linux__
    const std::string_view message = enabled ? "enable\n" : "disable\n";
    std::size_t sent = 0;
    while (sent != message.size()) {
      const auto n =
          ::write(control_, message.data() + sent, message.size() - sent);
      if (n < 0 && errno == EINTR) continue;
      if (n <= 0) throw std::runtime_error("perf control write failed");
      sent += static_cast<std::size_t>(n);
    }
    std::string reply;
    const auto deadline = Clock::now() + std::chrono::seconds(10);
    while (reply.size() < 16 && Clock::now() < deadline) {
      pollfd fd{ack_, POLLIN, 0};
      const auto ready = ::poll(&fd, 1, 100);
      if (ready < 0 && errno == EINTR) continue;
      if (ready < 0) break;
      if (!ready) continue;
      char c;
      if (::read(ack_, &c, 1) != 1) break;
      // perf writes sizeof("ack\n"), including NUL. Consume that padding on
      // the next handshake as well; perf versions without NUL also work.
      if (c == '\0' && reply.empty()) continue;
      reply += c;
      if (c == '\n') break;
    }
    if (reply != "ack\n")
      throw std::runtime_error("perf acknowledgement missing/invalid");
#else
    throw std::invalid_argument("perf control requires Linux");
#endif
  }

 private:
  int control_, ack_;
};

uint64_t burn(uint64_t seed, uint32_t iterations) noexcept {
  auto value = seed + seed_bias;
  for (uint32_t i = 0; i < iterations; ++i) {
    value ^= value >> 30;
    value *= mix_multiplier_1;
    value ^= value >> 27;
    value *= mix_multiplier_2;
    value = std::rotl(value, 17);
  }
  return value;
}
int64_t now_ns() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             Clock::now().time_since_epoch())
      .count();
}
double us(int64_t ns) { return static_cast<double>(ns) / 1000.0; }
double percentile(std::vector<double> values, double q) {
  if (values.empty()) return 0;
  std::ranges::sort(values);
  return values[static_cast<std::size_t>((values.size() - 1) * q)];
}

// Persistent producer threads are created before warmup/timing. Fresh mode
// deliberately includes creation/join. A failed thread constructor releases
// already-started threads; publication failure is rethrown only after all
// producers finish, then Workload's lifetime barrier drains accepted tasks.
class Producers {
 public:
  Producers(uint32_t count, std::function<void(uint32_t)> fn,
            std::vector<int> cpus)
      : function_(std::move(fn)), cpus_(std::move(cpus)), errors_(count) {
    try {
      for (uint32_t id = 0; id < count; ++id)
        threads_.emplace_back([this, id] { worker(id); });
    } catch (...) {
      stop();
      throw;
    }
  }
  ~Producers() { stop(); }
  void run() {
    std::unique_lock lock(mutex_);
    std::fill(errors_.begin(), errors_.end(), nullptr);
    pending_ = threads_.size();
    ++generation_;
    start_.notify_all();
    done_.wait(lock, [&] { return pending_ == 0; });
    for (auto error : errors_)
      if (error) std::rethrow_exception(error);
  }

 private:
  void stop() {
    {
      std::lock_guard lock(mutex_);
      stopping_ = true;
      start_.notify_all();
    }
    for (auto& thread : threads_)
      if (thread.joinable()) thread.join();
  }
  void worker(uint32_t id) {
    std::exception_ptr startup;
    try {
      if (!cpus_.empty()) bind_cpus({cpus_[id % cpus_.size()]});
    } catch (...) {
      startup = std::current_exception();
    }
    uint64_t seen = 0;
    for (;;) {
      std::unique_lock lock(mutex_);
      start_.wait(lock, [&] { return stopping_ || generation_ != seen; });
      if (stopping_) return;
      seen = generation_;
      lock.unlock();
      std::exception_ptr error = startup;
      if (!error) try {
          function_(id);
        } catch (...) {
          error = std::current_exception();
        }
      lock.lock();
      errors_[id] = error;
      if (--pending_ == 0) done_.notify_one();
    }
  }
  std::function<void(uint32_t)> function_;
  std::vector<int> cpus_;
  std::vector<std::exception_ptr> errors_;
  std::vector<std::thread> threads_;
  std::mutex mutex_;
  std::condition_variable start_, done_;
  uint64_t generation_{};
  std::size_t pending_{};
  bool stopping_{};
};

struct Spec {
  uint64_t seed;
  uint32_t iterations;
};
struct Slot {
  std::atomic<uint64_t> value{0};
  std::atomic<uint32_t> visits{0};
  // Written once per sampled invocation; observed only after wait_idle().
  int64_t submitted{}, started{}, finished{};
};
struct Submission {
  std::size_t slot;
  dagflow::SubmitOptions options;
};
class Workload {
 public:
  Workload(dagflow::Pool& pool, const Options& options, uint32_t workers,
           std::string_view scenario)
      : pool(pool), o(options), workers(workers), name(scenario) {
    n = o.tasks                                                     ? o.tasks
        : name == "external-contention" || name == "external-batch" ? 131072
        : name == "nested-helping"                                  ? 16384
        : name == "idle-burst"                                      ? 64
                                                                    : 65536;
    std::size_t total = n;
    if (name == "local-overflow") ++total;
    if (name == "nested-helping") total *= 2;
    fanout_count = ((n + 7) / 8) * o.fanout;
    if (name == "mixed-chaos") total += fanout_count + (n + 15) / 16;
    if (total > 20'000'000)
      throw std::invalid_argument("too many logical tasks");
    specs.reserve(total);
    for (std::size_t i = 0; i < n; ++i) {
      uint32_t iterations = name == "hot-shard-skew" ? (i % 64 == 0 ? 4000 : 16)
                            : name == "nested-helping" ? 8
                            : name == "mixed-chaos" ? (i % 128 == 0 ? 2000 : 8)
                                                    : 0;
      specs.push_back({name == "nested-helping" ? i ^ nested_seed_mask : i,
                       o.iterations.value_or(iterations)});
    }
    if (name == "local-overflow")
      specs.push_back({n, o.iterations.value_or(0)});
    if (name == "nested-helping")
      for (std::size_t i = 0; i < n; ++i)
        specs.push_back({i, o.iterations.value_or(32)});
    if (name == "mixed-chaos") {
      for (std::size_t i = 0; i < n; i += 8)
        for (uint32_t c = 0; c < o.fanout; ++c)
          specs.push_back({i * o.fanout + c, o.iterations.value_or(8)});
      for (std::size_t i = 0; i < n; i += 16)
        specs.push_back({i ^ mixed_seed_mask, o.iterations.value_or(64)});
    }
    slots = std::make_unique<Slot[]>(total);
    expected.reserve(total);
    for (auto spec : specs)
      expected.push_back(burn(spec.seed, spec.iterations));
    plans.resize(multi_producer() ? o.producers : 1);
    // Same per-producer order, affinity and priority for scalar/batch
    // comparison. Consecutive tasks sharing options can be grouped by
    // submit_batch_detached.
    for (uint32_t p = 0; p < plans.size(); ++p) {
      std::vector<std::vector<Submission>> buckets(workers * 2);
      for (std::size_t i = p; i < n; i += plans.size()) {
        auto opt = options_for(i);
        auto bucket = opt.affinity.value_or(0) * 2 +
                      (opt.priority == dagflow::Priority::High);
        buckets[bucket].push_back({i, opt});
      }
      for (auto& bucket : buckets)
        plans[p].insert(plans[p].end(), bucket.begin(), bucket.end());
    }
  }
  ~Workload() { pool.wait_idle(); }
  bool multi_producer() const {
    return name == "external-contention" || name == "external-batch" ||
           name == "mixed-chaos";
  }
  uint32_t submit_batch() const {
    if (name == "local-overflow" || name == "nested-helping" ||
        name == "mixed-chaos")
      return 0;
    return name == "external-batch" && !o.batch ? 16u : o.batch;
  }
  bool sampled(std::size_t i) const {
    return (o.latency || name == "idle-burst") &&
           (name == "idle-burst" || i % o.sample_stride == 0);
  }
  dagflow::SubmitOptions options_for(std::size_t i) const {
    dagflow::SubmitOptions opt;
    if (name == "hot-shard-skew" || o.placement == "hot")
      opt.affinity = 0;
    else if (o.placement == "spread")
      opt.affinity = static_cast<uint32_t>(i % workers);
    if (multi_producer() && o.high_every && i % o.high_every == 0)
      opt.priority = dagflow::Priority::High;
    return opt;
  }
  void stamp(std::size_t i) {
    if (sampled(i)) slots[i].submitted = now_ns();
  }
  void start(std::size_t i) {
    if (sampled(i)) slots[i].started = now_ns();
  }
  void finish(std::size_t i) {
    if (sampled(i)) slots[i].finished = now_ns();
  }
  void record(std::size_t i) noexcept {
    const auto spec = specs[i];
    if (sampled(i) && !slots[i].started) slots[i].started = now_ns();
    const auto value = burn(spec.seed, spec.iterations);
    sink = value;
    if (o.verify != "off") {
      slots[i].value.store(value, std::memory_order_relaxed);
      if (o.verify == "exact")
        slots[i].visits.fetch_add(1, std::memory_order_relaxed);
      else
        slots[i].visits.store(1, std::memory_order_relaxed);
    }
    if (sampled(i)) slots[i].finished = now_ns();
  }
  void reset() {
    for (std::size_t i = 0; i < specs.size(); ++i) {
      slots[i].value.store(0, std::memory_order_relaxed);
      slots[i].visits.store(0, std::memory_order_relaxed);
      slots[i].submitted = slots[i].started = slots[i].finished = 0;
    }
    failed.store(false, std::memory_order_relaxed);
  }
  uint64_t validate() const {
    if (failed.load(std::memory_order_relaxed))
      throw std::runtime_error("nested publication/task failure");
    uint64_t checksum = 0;
    for (std::size_t i = 0; i < specs.size(); ++i) {
      if (o.verify != "off" &&
          (slots[i].visits.load(std::memory_order_relaxed) != 1 ||
           slots[i].value.load(std::memory_order_relaxed) != expected[i]))
        throw std::runtime_error("payload verification failed at slot " +
                                 std::to_string(i));
      if (sampled(i) &&
          (!slots[i].submitted || slots[i].started < slots[i].submitted ||
           slots[i].finished < slots[i].started))
        throw std::runtime_error("invalid task timestamps");
      checksum += o.verify == "off"
                      ? 0
                      : slots[i].value.load(std::memory_order_relaxed);
    }
    return checksum;
  }
  struct Payload {
    Workload* owner{};
    std::size_t slot{};
    void operator()() const noexcept { owner->record(slot); }
  };
  void produce(uint32_t producer) {
    const auto& plan = plans[producer];
    const auto batch = submit_batch();
    for (std::size_t at = 0; at < plan.size();) {
      const auto i = plan[at].slot;
      const auto options = plan[at].options;
      if (name == "mixed-chaos") {
        stamp(i);
        pool.submit_detached(
            [this, i] {
              try {
                record(i);
                if (i % 8 == 0)
                  for (uint32_t c = 0; c < o.fanout; ++c) {
                    const auto child = n + (i / 8) * o.fanout + c;
                    stamp(child);
                    pool.submit_detached(Payload{this, child});
                  }
                if (i % 16 == 0) {
                  const auto child = n + fanout_count + i / 16;
                  stamp(child);
                  auto h = pool.submit(
                      Payload{this, child},
                      {.affinity = static_cast<uint32_t>((i + 1) % workers),
                       .priority = dagflow::Priority::High});
                  pool.wait_and_rethrow(h);
                }
                finish(i);
              } catch (...) {
                failed.store(true, std::memory_order_relaxed);
              }
            },
            options);
        ++at;
      } else if (batch) {
        std::array<Payload, 64> payloads;
        std::size_t count = 0;
        while (at < plan.size() && count < batch &&
               plan[at].options.affinity == options.affinity &&
               plan[at].options.priority == options.priority) {
          stamp(plan[at].slot);
          payloads[count++] = Payload{this, plan[at++].slot};
        }
        pool.submit_batch_detached(std::span(payloads.data(), count), options);
      } else {
        stamp(i);
        pool.submit_detached(Payload{this, i}, options);
        ++at;
      }
    }
  }
  void execute(Producers* persistent, const std::vector<int>& producer_cpus) {
    if (multi_producer()) {
      if (persistent)
        persistent->run();
      else {
        Producers fresh(
            o.producers, [this](uint32_t p) { produce(p); }, producer_cpus);
        fresh.run();
      }
    } else if (name == "local-overflow") {
      stamp(n);
      auto root = pool.submit([this] {
        record(n);
        for (std::size_t i = 0; i < n; ++i) {
          stamp(i);
          pool.submit_detached(Payload{this, i});
        }
        finish(n);
      });
      pool.wait_and_rethrow(root);
    } else if (name == "nested-helping") {
      for (std::size_t i = 0; i < n; ++i) {
        stamp(i);
        pool.submit_detached([this, i] {
          try {
            start(i);
            stamp(n + i);
            auto child = pool.submit(Payload{this, n + i});
            pool.wait_and_rethrow(child);
            record(i);
          } catch (...) {
            failed.store(true, std::memory_order_relaxed);
          }
        });
      }
    } else
      produce(0);
    pool.wait_idle();
  }
  dagflow::Pool& pool;
  const Options& o;
  uint32_t workers;
  std::string_view name;
  std::size_t n{}, fanout_count{};
  std::vector<Spec> specs;
  std::vector<uint64_t> expected;
  std::unique_ptr<Slot[]> slots;
  std::vector<std::vector<Submission>> plans;
  std::atomic<bool> failed{false};
};

struct Usage {
  double user_us{}, system_us{};
  int64_t voluntary{}, involuntary{}, minor{}, major{}, max_rss_kb{};
};
Usage usage() {
  Usage u;
#ifdef __linux__
  rusage r{};
  if (getrusage(RUSAGE_SELF, &r)) throw std::runtime_error("getrusage failed");
  u = {r.ru_utime.tv_sec * 1e6 + r.ru_utime.tv_usec,
       r.ru_stime.tv_sec * 1e6 + r.ru_stime.tv_usec,
       r.ru_nvcsw,
       r.ru_nivcsw,
       r.ru_minflt,
       r.ru_majflt,
       r.ru_maxrss};
#endif
  return u;
}
struct Sample {
  double elapsed_us{}, user_us{}, system_us{}, first_start_us{},
      last_finish_us{}, drain_tail_us{};
  int64_t voluntary{}, involuntary{}, minor{}, major{}, max_rss_kb{};
  Counts counts{};
  std::vector<double> starts, finishes;
};

void run(const Options& o, uint32_t workers, std::string_view name,
         const std::vector<int>& permitted) {
  auto select = [&](const std::string& list) {
    auto result = list.empty() ? permitted : cpu_list(list);
    for (int cpu : result)
      if (std::ranges::find(permitted, cpu) == permitted.end())
        throw std::invalid_argument("CPU outside allowed affinity");
    return result;
  };
  const auto worker_cpus = select(o.worker_cpus),
             producer_cpus = select(o.producer_cpus);
  bind_cpus(worker_cpus);  // Workers inherit this mask. Pool pinning stays off.
  dagflow::Pool pool({.threads = workers,
                      .shards = o.shards,
                      .central_batch = o.central_batch,
                      .pin_threads = false});
  bind_cpus(producer_cpus);  // Controller and persistent external producers.
  Workload work(pool, o, workers, name);
  std::unique_ptr<Producers> persistent;
  if (work.multi_producer() && o.producer_mode == "persistent")
    persistent = std::make_unique<Producers>(
        o.producers, [&](uint32_t p) { work.produce(p); }, producer_cpus);
  PerfControl perf(o);
  uint32_t warmed = 0;
  const auto warm_started = Clock::now();
  while (warmed < o.warmup ||
         Clock::now() - warm_started < std::chrono::milliseconds(o.warmup_ms)) {
    if (warmed == o.max_samples)
      throw std::runtime_error("warmup exceeded max-samples");
    work.reset();
    work.execute(persistent.get(), producer_cpus);
    work.validate();
    ++warmed;
  }
  const auto warm_elapsed_ms =
      std::chrono::duration<double, std::milli>(Clock::now() - warm_started)
          .count();
  const auto minimum_samples = name == "idle-burst" ? o.bursts : o.repeats;
  std::vector<Sample> samples;
  samples.reserve(minimum_samples);
  uint64_t checksum = 0;
  double measured_us = 0;
  do {
    work.reset();
    if (name == "idle-burst")
      std::this_thread::sleep_for(std::chrono::microseconds(o.idle_us));
    const auto before_counts = dagflow::detail::runtime_counts();
    const auto before = usage();
    perf.set(true);
    const auto begin = now_ns();
    try {
      work.execute(persistent.get(), producer_cpus);
    } catch (...) {
      pool.wait_idle();
      perf.set(false);
      throw;
    }
    const auto end = now_ns();
    perf.set(false);
    const auto after = usage();
    const auto after_counts = dagflow::detail::runtime_counts();
    checksum = work.validate();  // Entirely outside the measured/perf phase.
    Sample sample;
    sample.elapsed_us = us(end - begin);
    measured_us += sample.elapsed_us;
    sample.user_us = after.user_us - before.user_us;
    sample.system_us = after.system_us - before.system_us;
    sample.voluntary = after.voluntary - before.voluntary;
    sample.involuntary = after.involuntary - before.involuntary;
    sample.minor = after.minor - before.minor;
    sample.major = after.major - before.major;
    sample.max_rss_kb = after.max_rss_kb;
    for (std::size_t i = 0; i < sample.counts.size(); ++i)
      sample.counts[i] = after_counts[i] - before_counts[i];
    int64_t first = end, last = begin;
    for (std::size_t i = 0; i < work.specs.size(); ++i)
      if (work.sampled(i)) {
        const auto& slot = work.slots[i];
        sample.starts.push_back(us(slot.started - slot.submitted));
        sample.finishes.push_back(us(slot.finished - slot.submitted));
        first = std::min(first, slot.started);
        last = std::max(last, slot.finished);
      }
    if (!sample.starts.empty()) {
      sample.first_start_us = us(first - begin);
      sample.last_finish_us = us(last - begin);
      sample.drain_tail_us = us(end - last);
    }
    samples.push_back(std::move(sample));
  } while (
      samples.size() < o.max_samples &&
      (samples.size() < minimum_samples || measured_us < o.min_ms * 1000.0));
  const bool duration_satisfied = measured_us >= o.min_ms * 1000.0;
  std::vector<double> times, starts, finishes;
  Counts counts{};
  for (const auto& s : samples) {
    times.push_back(s.elapsed_us);
    starts.insert(starts.end(), s.starts.begin(), s.starts.end());
    finishes.insert(finishes.end(), s.finishes.begin(), s.finishes.end());
    for (std::size_t i = 0; i < counts.size(); ++i) counts[i] += s.counts[i];
  }
  const auto median = percentile(times, .5);
  if (!o.json) {
    if (name == "idle-burst")
      std::cout << name << " p50=" << median
                << " us p99=" << percentile(times, .99)
                << " us max=" << percentile(times, 1) << " us\n";
    else
      std::cout << name << " median=" << median / 1000
                << " ms min=" << percentile(times, 0) / 1000
                << " max=" << percentile(times, 1) / 1000
                << " throughput=" << work.specs.size() / median << " Mtask/s\n";
    std::cout << "  workers=" << workers
              << " producers=" << (work.multi_producer() ? o.producers : 1)
              << " samples=" << samples.size() << " warmups=" << warmed
              << " verify=" << o.verify
              << " diagnostics=" << dagflow::detail::runtime_diagnostics_enabled
              << '\n';
  } else {
    std::cout << std::setprecision(12)
              << "{\"schema\":2,\"status\":\"ok\",\"scenario\":"
              << std::quoted(std::string(name)) << ",\"workers\":" << workers
              << ",\"producers\":" << (work.multi_producer() ? o.producers : 1)
              << ",\"shards\":" << (o.shards ? o.shards : workers)
              << ",\"central_batch\":" << o.central_batch
              << ",\"submit_batch\":" << work.submit_batch()
              << ",\"tasks\":" << work.n
              << ",\"logical_tasks\":" << work.specs.size()
              << ",\"iterations\":";
    if (o.iterations)
      std::cout << *o.iterations;
    else
      std::cout << "null";
    std::cout
        << ",\"verify\":" << std::quoted(o.verify)
        << ",\"checksum\":" << checksum
        << ",\"producer_mode\":" << std::quoted(o.producer_mode)
        << ",\"placement\":" << std::quoted(o.placement)
        << ",\"fanout\":" << o.fanout << ",\"high_every\":" << o.high_every
        << ",\"idle_us\":" << o.idle_us
        << ",\"sample_stride\":" << o.sample_stride
        << ",\"latency\":" << (o.latency ? "true" : "false")
        << ",\"measured_us\":" << measured_us
        << ",\"requested_min_ms\":" << o.min_ms
        << ",\"worker_cpus\":" << std::quoted(cpu_text(worker_cpus))
        << ",\"producer_cpus\":" << std::quoted(cpu_text(producer_cpus))
        << ",\"warmup_runs\":" << warmed << ",\"warmup_ms\":" << warm_elapsed_ms
        << ",\"sample_count\":" << samples.size()
        << ",\"duration_satisfied\":" << (duration_satisfied ? "true" : "false")
        << ",\"perf_gated\":" << (o.control_fd >= 0 ? "true" : "false")
        << ",\"diagnostics\":"
        << (dagflow::detail::runtime_diagnostics_enabled ? "true" : "false")
        << ",\"diagnostic_overflow_threads\":"
        << dagflow::detail::runtime_diagnostic_overflow_threads()
        << ",\"latency_sample_count\":" << starts.size()
        << ",\"run_p50_us\":" << median
        << ",\"run_p99_us\":" << percentile(times, .99)
        << ",\"start_p50_us\":" << percentile(starts, .5)
        << ",\"start_p99_us\":" << percentile(starts, .99)
        << ",\"finish_p99_us\":" << percentile(finishes, .99)
        << ",\"counts\":{ ";
    for (std::size_t i = 0; i < counts.size(); ++i) {
      if (i) std::cout << ',';
      std::cout << std::quoted(
                       std::string(dagflow::detail::runtime_event_names[i]))
                << ':' << counts[i];
    }
    std::cout << "},\"samples\":[";
    for (std::size_t i = 0; i < samples.size(); ++i) {
      if (i) std::cout << ',';
      const auto& s = samples[i];
      std::cout << "{\"elapsed_us\":" << s.elapsed_us
                << ",\"user_us\":" << s.user_us
                << ",\"system_us\":" << s.system_us
                << ",\"voluntary_switches\":" << s.voluntary
                << ",\"involuntary_switches\":" << s.involuntary
                << ",\"minor_faults\":" << s.minor
                << ",\"major_faults\":" << s.major
                << ",\"max_rss_kb\":" << s.max_rss_kb
                << ",\"first_start_us\":" << s.first_start_us
                << ",\"last_finish_us\":" << s.last_finish_us
                << ",\"drain_tail_us\":" << s.drain_tail_us
                << ",\"start_latency_us\":[";
      for (std::size_t j = 0; j < s.starts.size(); ++j) {
        if (j) std::cout << ',';
        std::cout << s.starts[j];
      }
      std::cout << "],\"finish_latency_us\":[";
      for (std::size_t j = 0; j < s.finishes.size(); ++j) {
        if (j) std::cout << ',';
        std::cout << s.finishes[j];
      }
      std::cout << "],\"counts\":{";
      for (std::size_t j = 0; j < s.counts.size(); ++j) {
        if (j) std::cout << ',';
        std::cout << std::quoted(
                         std::string(dagflow::detail::runtime_event_names[j]))
                  << ':' << s.counts[j];
      }
      std::cout << "}}";
    }
    std::cout << "]}\n";
  }
  bind_cpus(permitted);
}
}  // namespace

int main(int argc, char** argv) {
  try {
    const auto o = parse(argc, argv);
    auto permitted = allowed_cpus();
    if (!o.cpus.empty()) {
      auto selected = cpu_list(o.cpus);
      for (int cpu : selected)
        if (std::ranges::find(permitted, cpu) == permitted.end())
          throw std::invalid_argument("CPU not allowed");
      bind_cpus(selected);
      permitted = std::move(selected);
    }
    for (auto name : scenarios)
      if (o.scenario == "all" || o.scenario == name) {
        if (o.workers)
          run(o, o.workers, name, permitted);
        else {
          run(o, 1, name, permitted);
          run(o, 4, name, permitted);
        }
      }
  } catch (const std::exception& error) {
    std::cerr << "Harness failed: " << error.what() << '\n';
    return 2;
  }
}
