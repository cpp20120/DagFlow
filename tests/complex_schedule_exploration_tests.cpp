#include <array>
#include <atomic>
#include <dagflow/detail/fuzz_points.hpp>
#include <dagflow/task_graph.hpp>
#include <memory>

#include "schedule_exploration_support.hpp"

namespace {
namespace schedule = dagflow::test::schedule;
namespace hooks = dagflow::detail::fuzz_points;
using schedule::Controller;

dagflow::Config config(unsigned workers) {
  dagflow::Config result;
  result.threads = workers;
  result.pin_threads = false;
  return result;
}

struct Lanes {
  explicit Lanes(Controller& c) : control(c) {}
  Controller& control;
  std::array<bool, 2> entered{}, joined{};  // One writer per physical worker.
  std::atomic<unsigned> joins{0};
};

void lane_checkpoint(hooks::Point point, void* context) noexcept {
  auto& fixture = *static_cast<Lanes*>(context);
  const auto worker = dagflow::Pool::fuzz_worker_id();
  if (worker >= 2) return;
  if (point == hooks::Point::before_execute && !fixture.entered[worker]) {
    fixture.entered[worker] = true;
    fixture.control.checkpoint(worker, "root lane admitted");
  } else if (point == hooks::Point::graph_before_lane_join &&
             !fixture.joined[worker]) {
    fixture.control.checkpoint(worker, "root lane before final join");
  } else if (point == hooks::Point::graph_after_lane_join &&
             !fixture.joined[worker]) {
    fixture.joined[worker] = true;
    fixture.joins.fetch_add(1);
    // Only the root lanes are scheduled actors. Downstream packets and their
    // epilogues run as an ordinary concurrent tail, checked by atomic oracles.
    fixture.control.complete(worker);
  }
}

void graph_lanes(bool cancel) {
  schedule::explore(
      cancel ? "graph-lanes-fanin-cancel" : "graph-lanes-fanin", cancel ? 3 : 2,
      [&](Controller& control) {
        dagflow::Pool pool(config(2));
        dagflow::TaskGraph graph(pool);
        Lanes fixture(control);
        constexpr unsigned token_count = 4;
        std::atomic<unsigned> started{0};
        std::atomic<unsigned> effects{0}, left{0}, right{0}, fanin{0};
        auto capture = std::make_shared<int>(1);
        std::weak_ptr<int> lifetime = capture;
        dagflow::Handle result;
        dagflow::TaskGraph::NodeOptions options;
        options.concurrency = 2;
        auto root = graph.emplace(
            [&, capture] {
              control.require(
                  started.fetch_add(1) < token_count,
                  "invocations stay inside the admitted token budget");
              control.require(!result.ready(),
                              "executing lane retains a live credit");
              control.checkpoint(dagflow::Pool::fuzz_worker_id(),
                                 "token body in flight");
              effects.fetch_add(1);
            },
            options);
        graph.set_tokens(root, token_count);
        const auto branch = [&](std::atomic<unsigned>& calls) {
          control.require(
              effects.load() == token_count && fixture.joins == 2,
              "dependencies require every root token and both lanes");
          control.require(calls.fetch_add(1) == 0, "branch activated once");
        };
        auto a = graph.emplace([&] { branch(left); });
        auto b = graph.emplace([&] { branch(right); });
        auto join = graph.emplace([&] {
          control.require(left == 1 && right == 1,
                          "fan-in acquires effects from both predecessors");
          control.require(fanin.fetch_add(1) == 0, "fan-in activated once");
        });
        graph.add_edge(root, a);
        graph.add_edge(root, b);
        graph.add_edge(a, join);
        graph.add_edge(b, join);
        capture.reset();
        hooks::install(lane_checkpoint, &fixture);
        result = graph.run();
        std::thread canceller;
        if (cancel) canceller = control.actor(2, [&] { graph.cancel(); });
        control.drive();
        if (canceller.joinable()) canceller.join();
        pool.wait_and_rethrow(result);
        pool.wait_idle();
        hooks::clear();
        control.require(fixture.joins == 2 && result.ready(),
                        "cancelled and successful lanes both retire");
        if (!cancel)
          control.require(effects == 4 && left == 1 && right == 1 && fanin == 1,
                          "uncancelled graph executes the complete diamond");
        control.require(effects <= 4 && left <= 1 && right <= 1 && fanin <= 1,
                        "cancellation cannot duplicate effects");
        graph.clear();
        control.require(lifetime.expired() && result.ready(),
                        "definition borrows end before clear; result survives");
      });
}

void fanin_checkpoint(hooks::Point point, void* context) noexcept {
  auto& control = *static_cast<Controller*>(context);
  switch (point) {
    case hooks::Point::graph_before_dependency_check:
    case hooks::Point::graph_before_inline_continuation:
    case hooks::Point::completion_before_ready:
    case hooks::Point::completion_after_ready:
      control.checkpoint("fan-in propagation/completion boundary");
      break;
    case hooks::Point::before_retire:
      control.complete(schedule::actor_id);
      break;
    default:
      break;
  }
}

void fanin_cancel() {
  schedule::explore("fanin-cancel-complete", 3, [](Controller& control) {
    dagflow::Pool pool(config(2));
    dagflow::TaskGraph graph(pool);
    std::array<std::atomic<unsigned>, 2> effects{};
    std::atomic<unsigned> joined{0};
    std::atomic<bool> cancelled{false};
    dagflow::Handle result;
    const auto predecessor = [&](unsigned actor) {
      // The logical actor belongs to the root node, independent of which
      // physical worker stole it. Both roots enter before cancellation starts.
      schedule::actor_id = actor;
      control.checkpoint(actor, "predecessor body in flight");
      control.require(!result.ready(), "live predecessor prevents readiness");
      effects[actor].fetch_add(1);
      control.checkpoint(actor, "predecessor effects written");
    };
    auto a = graph.emplace([&] { predecessor(0); });
    auto b = graph.emplace([&] { predecessor(1); });
    auto join = graph.emplace([&] {
      control.require(!cancelled.load(),
                      "cancel before activation suppresses fan-in callable");
      control.require(effects[0] == 1 && effects[1] == 1,
                      "fan-in cannot run before both predecessor effects");
      control.require(joined.fetch_add(1) == 0, "fan-in runs once");
      control.checkpoint("fan-in body in flight");
      control.require(!result.ready(), "live fan-in prevents readiness");
    });
    graph.add_edge(a, join);
    graph.add_edge(b, join);
    hooks::install(fanin_checkpoint, &control);
    result = graph.run();
    auto canceller = control.actor(2, [&] {
      cancelled.store(true);
      graph.cancel();
    });
    control.drive();
    canceller.join();
    pool.wait_and_rethrow(result);
    pool.wait_idle();
    hooks::clear();
    control.require(
        effects[0] == 1 && effects[1] == 1 && joined <= 1,
        "in-flight bodies finish; cancelled fan-in never duplicates");
    graph.clear();
    control.require(result.ready(), "terminal result survives graph clear");
  });
}

struct Helping {
  Helping(Controller& c, dagflow::Pool& p) : control(c), pool(p) {}
  Controller& control;
  dagflow::Pool& pool;
  std::array<std::atomic<unsigned>, 3> calls{}, destroyed{};
  std::atomic<bool> root_finished{false};
};

void helping_checkpoint(hooks::Point point, void* context) noexcept {
  auto& fixture = *static_cast<Helping*>(context);
  auto& control = fixture.control;
  if (point == hooks::Point::shutdown_before_drain) {
    control.require(!fixture.pool.fuzz_stopped(),
                    "shutdown must keep workers live through descendant drain");
    control.checkpoint(1, "shutdown before physical drain");
    control.blocking_tail(1);
    return;
  }
  if (fixture.root_finished.load()) return;
  if (point == hooks::Point::before_execute)
    control.checkpoint(0, "nested helping before invocation");
  else if (point == hooks::Point::before_retire) {
    const auto depth = dagflow::Pool::worker_wait_depth();
    control.require(
        depth < 3 && fixture.calls[depth] == 1 && fixture.destroyed[depth] == 1,
        "every nested epilogue destroys its callable once");
    if (depth == 0) {
      fixture.root_finished.store(true);
      control.complete(0);
    } else {
      control.checkpoint(0, "nested completion before parent resumes");
    }
  }
}

void helping_shutdown() {
  bool covered_descendant_after_close = false;
  schedule::explore("nested-help-shutdown", 2, [&](Controller& control) {
    dagflow::Pool pool(config(1));
    Helping fixture(control, pool);
    std::atomic<bool> after_close{false};
    struct Capture {
      std::atomic<unsigned>& destroyed;
      ~Capture() { ++destroyed; }
    };
    const auto capture = [&](unsigned index) {
      return std::unique_ptr<Capture>(new Capture{fixture.destroyed[index]});
    };
    hooks::install(helping_checkpoint, &fixture);
    auto parent = pool.submit([&, owned = capture(0)] {
      ++fixture.calls[0];
      control.checkpoint(0, "parent before child publication");
      if (pool.closed()) after_close.store(true);
      auto child = pool.submit([&, owned = capture(1)] {
        ++fixture.calls[1];
        control.require(dagflow::Pool::worker_wait_depth() == 1,
                        "child executes through parent helping");
        control.checkpoint(0, "child before grandchild publication");
        if (pool.closed()) after_close.store(true);
        auto grandchild = pool.submit([&, owned = capture(2)] {
          ++fixture.calls[2];
          control.require(dagflow::Pool::worker_wait_depth() == 2,
                          "grandchild executes through nested helping");
        });
        pool.wait_and_rethrow(grandchild);
        control.require(fixture.destroyed[2] == 1 &&
                            dagflow::Pool::worker_wait_depth() == 1,
                        "inner wait restores depth after capture cleanup");
      });
      pool.wait_and_rethrow(child);
      control.require(
          fixture.destroyed[1] == 1 && dagflow::Pool::worker_wait_depth() == 0,
          "outer wait restores depth after descendant completion");
    });
    auto closer = control.actor(1, [&] {
      pool.close();
      control.checkpoint("external admission closed");
      bool rejected = false;
      try {
        (void)pool.submit([] {});
      } catch (const std::logic_error&) {
        rejected = true;
      }
      control.require(rejected, "closure rejects new external work");
      pool.shutdown();
      control.require(
          parent.ready() && fixture.root_finished && pool.fuzz_stopped(),
          "shutdown returns after parent epilogue and worker stop");
    });
    control.drive();
    closer.join();
    hooks::clear();
    parent.rethrow_if_failed();
    for (unsigned i = 0; i < 3; ++i)
      control.require(fixture.calls[i] == 1 && fixture.destroyed[i] == 1,
                      "shutdown drains all accepted descendants and captures");
    covered_descendant_after_close |= after_close.load();
  });
  if (!std::getenv("DAGFLOW_SCHEDULE_SUITE")) {
    if (!covered_descendant_after_close) std::abort();
  }
}
}  // namespace

int main() {
  fanin_cancel();
  graph_lanes(false);
  graph_lanes(true);
  helping_shutdown();
  schedule::verify_selection();
}
