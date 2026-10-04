#include <array>
#include <iostream>
#include <numeric>
#include <span>
#include <vector>

#include <dagflow/dagflow.hpp>

int main() {
  dagflow::Pool pool({.threads = 2, .pin_threads = false});
  std::array<int, 128> results{};
  auto make_task = [&](std::size_t i) {
    return [&, i]() noexcept { results[i] = static_cast<int>(i + 1); };
  };
  std::vector<decltype(make_task(0))> tasks;
  tasks.reserve(results.size());
  for (std::size_t i = 0; i < results.size(); ++i) tasks.push_back(make_task(i));

  // Real batch publication: callables are consumed by move. No per-task handles.
  try {
    pool.submit_batch_detached(std::span(tasks));
  } catch (...) {
    // Publication can accept a prefix before throwing. Drain it while the
    // borrowed results still exist; dropping the Pool is not a drain barrier.
    pool.wait_idle();
    throw;
  }
  pool.wait_idle();  // Detached work needs an explicit lifetime barrier.
  // Detached callback errors are discarded; these callbacks are noexcept.
  const int sum = std::accumulate(results.begin(), results.end(), 0);
  std::cout << "Batch: " << results.size() << " tasks; sum: " << sum << '\n';
  return sum == 8256 ? 0 : 1;
}
