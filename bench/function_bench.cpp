#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <span>
#include <utility>
#include <vector>

#include "dagflow/detail/small_function.hpp"

namespace {
using Function = dagflow::small_function<void(std::uint64_t&)>;
using Clock = std::chrono::steady_clock;

template <unsigned Kind>
struct Work {
  std::uint64_t value;
  void operator()(std::uint64_t& sum) { sum += value + Kind; }
};

// Keep the target type erased at invocation; the benchmark must measure the
// indirect call, rather than an optimizer folding a known lambda into a loop.
[[gnu::noinline]] void fill(std::span<Function> functions) {
  for (std::size_t i = 0; i < functions.size(); ++i) {
    switch (i % 4) {
      case 0:
        functions[i] = Function{Work<0>{i}};
        break;
      case 1:
        functions[i] = Function{Work<1>{i}};
        break;
      case 2:
        functions[i] = Function{Work<2>{i}};
        break;
      case 3:
        functions[i] = Function{Work<3>{i}};
        break;
    }
  }
}

[[gnu::noinline]] std::uint64_t invoke(std::span<Function> functions,
                                       std::size_t passes) {
  std::uint64_t sum = 0;
  for (std::size_t pass = 0; pass < passes; ++pass)
    for (auto& function : functions) function(sum);
  return sum;
}

template <class F>
void measure(const char* name, std::size_t operations, F&& work) {
  std::vector<double> samples;
  std::uint64_t checksum = work();
  for (int trial = 0; trial < 9; ++trial) {
    const auto start = Clock::now();
    checksum += work();
    samples.push_back(
        std::chrono::duration<double, std::nano>(Clock::now() - start).count() /
        operations);
  }
  std::sort(samples.begin(), samples.end());
  std::printf("%s median_ns/op=%.3f checksum=%llu\n", name, samples[4],
              static_cast<unsigned long long>(checksum));
}
}  // namespace

int main() {
  std::printf("sizeof(function64)=%zu sizeof(function128)=%zu alignof=%zu\n",
              sizeof(Function), sizeof(dagflow::small_function<void(), 128>),
              alignof(Function));
  std::vector<Function> hot(256), streaming(65536);
  fill(hot);
  fill(streaming);
  measure("invoke_hot", hot.size() * 8192, [&] { return invoke(hot, 8192); });
  measure("invoke_streaming", streaming.size() * 32,
          [&] { return invoke(streaming, 32); });
  measure("construct_move_destroy", streaming.size(), [&] {
    std::vector<Function> source(streaming.size()),
        destination(streaming.size());
    fill(source);
    for (std::size_t i = 0; i < source.size(); ++i)
      destination[i] = std::move(source[i]);
    return invoke(destination, 1);
  });
}
