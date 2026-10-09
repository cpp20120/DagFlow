#include <atomic>
#include <iostream>
#include <memory>

#include <dagflow/dagflow.hpp>

int main() {
  dagflow::Pool pool({.threads = 2});
  std::atomic<int> sum{0};

  auto first = pool.submit([&sum, value = std::make_unique<int>(20)] {
    sum.fetch_add(*value, std::memory_order_relaxed);
  });
  auto second = pool.submit([&sum] {
    sum.fetch_add(22, std::memory_order_relaxed);
  });

  pool.wait_and_rethrow(first);
  pool.wait_and_rethrow(second);
  std::cout << "DagFlow result: " << sum.load() << '\n';
  return sum.load() == 42 ? 0 : 1;
}
