#include <iostream>

#include <dagflow/dagflow.hpp>

int main() {
  dagflow::Pool pool({.threads = 2, .pin_threads = false});
  int input = 3, left = 0, right = 0, total = 0;
  dagflow::GraphScope graph(pool);
  auto a = graph.emplace([&] { left = input * 2; });
  auto b = graph.emplace([&] { right = input + 1; });
  auto merged = graph.when_all({a, b}, [&] { total = left + right; });
  graph.then(merged, [&] { std::cout << "GraphScope result: " << total << '\n'; });
  graph.run_and_wait();
  if (total != 10) return 1;
  input = 5;  // Mutate inputs only after completion; reuse topology/callables.
  graph.run_and_wait();
  if (total != 16) return 1;

  // TaskGraph exposes explicit edges and seal() when finer control is useful.
  int value = 0;
  dagflow::TaskGraph compiled(pool);
  auto prepare = compiled.emplace([&] { value = input; });
  auto transform = compiled.emplace([&] { value *= 2; });
  compiled.add_edge(prepare, transform);
  if (!compiled.seal()) return 1;  // A cycle would prevent sealing.
  for (int next : {7, 9}) {
    input = next;
    pool.wait_and_rethrow(compiled.run());
    std::cout << "TaskGraph result: " << value << '\n';
    if (value != next * 2) return 1;
  }
}
