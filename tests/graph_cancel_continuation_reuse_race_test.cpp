
#include <atomic>
#include <barrier>
#include <latch>
#include <thread>
#include <vector>

#include <dagflow/task_graph.hpp>
#include "adversarial_support.hpp"

int main() {
  dagflow::Pool pool(adversarial::config(4));
  constexpr unsigned tokens = 193;
  std::vector<dagflow::Handle> history;
  unsigned generation = 0;

  for (unsigned boundary : {63u, 64u, 65u, 127u, 128u}) {
    for (unsigned repeat = 0; repeat < 8; ++repeat, ++generation) {
      std::atomic<unsigned> calls{0}, failures{0}, successors{0};
      std::atomic<bool> failing{true};
      std::latch boundary_reached(1), published(1);
      std::barrier race(3);

      dagflow::TaskGraph graph(pool);

      auto driver = graph.emplace([&] {
        const auto count = calls.fetch_add(1) + 1;
        CHECK(count <= tokens);

        if (failing.load() && count == boundary) {
          boundary_reached.count_down();
          race.arrive_and_wait();
        }
      });

      graph.set_tokens(driver, tokens); // One lane: boundaries 64/128 yield.

      auto failure = graph.emplace([&] {
        ++failures;
        if (failing.load()) {
          boundary_reached.wait();
          race.arrive_and_wait();
          throw adversarial::Failure{generation};
        }
      });

      auto successor = graph.emplace([&] {
        CHECK(calls == tokens && failures == 1);
        ++successors;
      });

      graph.add_edge(driver, successor);
      graph.add_edge(failure, successor);

      std::thread canceller([&] {
        published.wait(); // Do not read run_state_ concurrently with run().
        boundary_reached.wait();
        race.arrive_and_wait();
        graph.cancel();
      });

      auto failed = graph.run();
      published.count_down();
      pool.wait(failed);
      canceller.join();

      CHECK(adversarial::failure_id(failed) == generation);
      CHECK(calls >= boundary &&
            calls <= tokens && failures == 1 && successors == 0);

      const auto error_id = adversarial::failure_id(failed);
      history.push_back(failed);

      // No wait_idle(): readiness alone must end all borrows of reused run
      // storage, even if an old task packet is still in the pool epilogue.
      calls = 0;
      failures = 0;
      failing = false;

      auto clean = graph.run();
      pool.wait(clean);
      clean.rethrow_if_failed();

      CHECK(calls == tokens &&
            failures == 1 && successors == 1 && !graph.last_error());
      CHECK(adversarial::failure_id(failed) == error_id);
    }
  }

  for (unsigned i = 0; i < history.size(); ++i)
    CHECK(adversarial::failure_id(history[i]) == i);
}
