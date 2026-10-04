#include <algorithm>
#include <iostream>
#include <numeric>
#include <span>
#include <vector>

#include <dagflow/dagflow.hpp>

int main() {
  dagflow::Pool pool({.threads = 8, .pin_threads = false});
  std::vector data(40000, 1);
  // Pool algorithms share one callable; this callback only touches its element.
  pool.wait_and_rethrow(pool.for_each(data, [](int& x) { x += 2; }));
  // A temporary span is fine: its backing vector remains alive until completion.
  pool.wait_and_rethrow(pool.for_each_ws(std::span(data), [](int& x) { x *= 2; },
                                       {}, 512));
  // pool.for_each(std::vector<int>(100), ...); // Owning temporary is rejected.

  int total = 0;
  dagflow::GraphScope graph(pool);  // Destroy the graph before borrowed data.
  auto processed = graph.parallel_for(data, [](int& x) { ++x; },
                                      {.concurrency = 2});
  auto adjusted = graph.parallel_for_after(processed, std::span(data),
                                           [](int& x) { x *= 2; });
  graph.then(adjusted, [&] { total = std::accumulate(data.begin(), data.end(), 0); });
  // Graph algorithms keep one callable copy per chunk and retain iterators
  // across runs. concurrency limits execution; capacity is not chunk size.
  graph.run_and_wait();
  std::cout << "Processed " << data.size() << " elements; sum: " << total << '\n';
  return total == 560000 && std::ranges::all_of(data, [](int x) { return x == 14; })
             ? 0 : 1;
}
