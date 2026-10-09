#include <atomic>
#include <memory>
#include <vector>

#include <dagflow/graph_scope.hpp>
#include "adversarial_support.hpp"

namespace {
void graph_during_unwind(bool inner_failure) {
  dagflow::Pool pool(adversarial::config());
  std::atomic<unsigned> roots{0}, successors{0}, nested{0}, destroyed{0};
  struct Capture {
    std::atomic<unsigned>& destroyed;
    ~Capture() { CHECK(destroyed.fetch_add(1) == 0); }
  };
  auto parent = pool.submit([&] {
    dagflow::GraphScope graph(pool);
    auto root = graph.emplace([&, capture = std::unique_ptr<Capture>(new Capture{destroyed})] {
      CHECK(roots.fetch_add(1) == 0);
      auto child = pool.submit([&] { CHECK(nested.fetch_add(1) == 0); });
      pool.wait(child);
      child.rethrow_if_failed();
      if (inner_failure) throw adversarial::Failure{2};
    });
    graph.then(root, [&] { CHECK(successors.fetch_add(1) == 0); });
    // The destructor must run the unstarted graph during stack unwinding,
    // help on this sole worker, and preserve the outer exception.
    throw adversarial::Failure{1};
  });
  pool.wait(parent);
  CHECK(adversarial::failure_id(parent) == 1);
  CHECK(roots == 1 && nested == 1 && destroyed == 1);
  CHECK(successors == (inner_failure ? 0u : 1u));
}

struct CopyProbe {
  int& budget;
  std::atomic<int>& alive;
  CopyProbe(int& budget, std::atomic<int>& alive) : budget(budget), alive(alive) { ++alive; }
  CopyProbe(const CopyProbe& other) : budget(other.budget), alive(other.alive) {
    if (budget-- == 0) throw adversarial::Failure{3};
    ++alive;
  }
  CopyProbe(CopyProbe&& other) noexcept : budget(other.budget), alive(other.alive) { ++alive; }
  ~CopyProbe() { --alive; }
  void operator()(unsigned& value) { ++value; }
};

void throwing_chunk_copy() {
  dagflow::Pool pool(adversarial::config(4));
  // One local copy followed by three separately owned chunk copies. Fail at
  // each boundary, including after previous chunks acquired their captures.
  for (int budget = 0; budget <= 4; ++budget) {
    int remaining = budget;
    std::atomic<int> alive{0};
    std::atomic<unsigned> roots{0};
    std::vector<unsigned> values(2 * DAGFLOW_DEFAULT_RANGE_CHUNK + 1);
    {
      CopyProbe probe(remaining, alive);
      dagflow::GraphScope graph(pool);
      graph.emplace([&] { ++roots; });
      bool threw = false;
      try { graph.parallel_for(values, probe); }
      catch (const adversarial::Failure& failure) {
        CHECK(failure.id == 3);
        threw = true;
      }
      CHECK(threw == (budget < 4));
      CHECK(graph.size() == (threw ? 1u : 2u));
      CHECK(alive == (threw ? 1 : 4));
      graph.run_and_wait();
      CHECK(roots == 1);
      for (auto value : values) CHECK(value == (threw ? 0u : 1u));
      graph.clear();
      CHECK(alive == 1);
    }
    CHECK(alive == 0);
  }
}
}

int main() {
  graph_during_unwind(false);
  graph_during_unwind(true);
  throwing_chunk_copy();
}
