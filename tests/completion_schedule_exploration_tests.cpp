#include <array>
#include <atomic>
#include <dagflow/detail/fuzz_points.hpp>
#include <dagflow/task_graph.hpp>
#include <dagflow/task_scope.hpp>
#include <memory>

#include "schedule_exploration_support.hpp"

namespace {
namespace schedule = dagflow::test::schedule;
namespace hooks = dagflow::detail::fuzz_points;
using dagflow::detail::CompletionCredit;
using schedule::Controller;
struct Error {
  unsigned id;
};

dagflow::Config config() {
  dagflow::Config result;
  result.threads = 1;
  result.pin_threads = false;
  return result;
}

void completion_checkpoint(hooks::Point point, void* context) noexcept {
  auto& control = *static_cast<Controller*>(context);
  if (point == hooks::Point::completion_before_ready ||
      point == hooks::Point::completion_after_ready ||
      point == hooks::Point::completion_before_register)
    control.checkpoint("completion registration/readiness boundary");
}

void completion_histories() {
  schedule::explore("completion-register-retire", 3, [](Controller& control) {
    dagflow::Pool pool(config());
    dagflow::Handle source;
    unsigned live = 2, destroyed = 0, first_error = 0;
    struct Payload {
      Controller& control;
      dagflow::Handle& source;
      unsigned &live, &destroyed;
      ~Payload() {
        control.require(live == 0, "payload survives all modeled live credits");
        control.require(!source.ready(), "payload destruction precedes ready");
        ++destroyed;
      }
    };
    auto root = CompletionCredit::create(std::unique_ptr<Payload>(
        new Payload{control, source, live, destroyed}));
    source = root.handle();
    auto child = root.fork();
    auto other = CompletionCredit::create();
    auto other_handle = other.handle();
    dagflow::Handle combined;
    hooks::install(completion_checkpoint, &control);
    const auto retire = [&](CompletionCredit& credit, unsigned error) {
      if (!first_error) first_error = error;
      credit.fail(std::make_exception_ptr(Error{error}));
      control.checkpoint("error published with live credit");
      --live;
      credit.finish();
      control.require(!source.ready() || (live == 0 && destroyed == 1),
                      "readiness requires zero credits and payload cleanup");
    };
    auto first = control.actor(0, [&] { retire(root, 41); });
    auto second = control.actor(1, [&] { retire(child, 73); });
    auto registration = control.actor(2, [&] {
      const std::array inputs{source, other_handle};
      combined = pool.combine(inputs);
      control.require(!combined.ready(), "unrelated source holds fan-in open");
    });
    control.drive();
    first.join();
    second.join();
    registration.join();
    hooks::clear();
    control.require(source.ready() && destroyed == 1 && !combined.ready(),
                    "final retirement closes only its own source");
    other.finish();
    control.require(combined.ready(), "every dependent credit is consumed");
    for (auto handle : {source, combined}) {
      unsigned observed = 0;
      try {
        handle.rethrow_if_failed();
      } catch (const Error& error) {
        observed = error.id;
      }
      control.require(observed == first_error,
                      "first error propagates through early and late joins");
    }
  });
}

struct ScopeFixture {
  explicit ScopeFixture(Controller& c) : control(c) {}
  Controller& control;
  bool cancel_requested = false;
  bool should_call = false;
  unsigned calls = 0, destroyed = 0;
  dagflow::Handle completion;
};

void scope_checkpoint(hooks::Point point, void* context) noexcept {
  auto& fixture = *static_cast<ScopeFixture*>(context);
  auto& control = fixture.control;
  switch (point) {
    case hooks::Point::before_execute:
    case hooks::Point::scope_before_cancel_check:
      control.checkpoint(0, "before cancellation observation");
      break;
    case hooks::Point::scope_after_cancel_check:
      fixture.should_call = !fixture.cancel_requested;
      control.checkpoint(0, "cancellation observation committed");
      break;
    case hooks::Point::task_before_destroy:
      control.require(fixture.calls == unsigned(fixture.should_call),
                      "cancellation suppresses exactly the unstarted callable");
      control.checkpoint(0, "invocation finished, capture alive");
      break;
    case hooks::Point::task_after_destroy:
      control.require(fixture.destroyed == 1 && !fixture.completion.ready(),
                      "capture cleanup precedes completion");
      control.checkpoint(0, "capture destroyed, credit live");
      break;
    case hooks::Point::before_retire:
      control.require(fixture.completion.ready() && fixture.destroyed == 1,
                      "logical completion precedes physical retirement");
      control.complete(0);
      break;
    default:
      break;
  }
}

void scope_histories() {
  schedule::explore(
      "scope-cancel-execute-complete", 2, [](Controller& control) {
        dagflow::Pool pool(config());
        ScopeFixture fixture{control};
        dagflow::TaskScope scope(pool);
        fixture.completion = scope.completion();
        struct Capture {
          ScopeFixture& fixture;
          ~Capture() {
            fixture.control.require(
                !fixture.completion.ready(),
                "ready cannot precede callable destruction");
            ++fixture.destroyed;
          }
        };
        hooks::install(scope_checkpoint, &fixture);
        control.require(
            scope.spawn([&, capture = std::unique_ptr<Capture>(
                                new Capture{fixture})] { ++fixture.calls; }),
            "scope publication accepted");
        scope.close();
        auto cancellation = control.actor(1, [&] {
          fixture.cancel_requested = true;
          scope.cancel();
          control.require(scope.cancelled(),
                          "cancel publishes cancellation state");
        });
        control.drive();
        cancellation.join();
        pool.wait_idle();
        hooks::clear();
        control.require(
            fixture.calls == unsigned(fixture.should_call) &&
                fixture.destroyed == 1 && fixture.completion.ready(),
            "cancelled and executed packets both retire exactly once");
      });
}

void graph_checkpoint(hooks::Point point, void* context) noexcept {
  auto& control = *static_cast<Controller*>(context);
  if (point == hooks::Point::before_execute ||
      point == hooks::Point::graph_before_dependency_check ||
      point == hooks::Point::graph_before_inline_continuation)
    control.checkpoint(0, "graph dependency/continuation boundary");
  else if (point == hooks::Point::before_retire)
    control.complete(0);
}

void graph_histories() {
  schedule::explore("graph-cancel-propagation", 2, [](Controller& control) {
    dagflow::Pool pool(config());
    dagflow::TaskGraph graph(pool);
    bool cancel_requested = false;
    unsigned roots = 0, successors = 0;
    auto root = graph.emplace([&] {
      control.require(!cancel_requested, "cancel suppresses unstarted root");
      control.require(++roots == 1, "root executes once");
    });
    auto successor = graph.emplace([&] {
      control.require(!cancel_requested,
                      "cancel suppresses unstarted successor");
      control.require(roots == 1 && ++successors == 1,
                      "dependency publication preserves predecessor effects");
    });
    graph.add_edge(root, successor);
    hooks::install(graph_checkpoint, &control);
    auto result = graph.run();
    auto cancellation = control.actor(1, [&] {
      cancel_requested = true;
      graph.cancel();
    });
    control.drive();
    cancellation.join();
    pool.wait_idle();
    hooks::clear();
    control.require(result.ready() && !graph.last_error(),
                    "cancelled graph drains credits without fabricating error");
    graph.clear();
    control.require(result.ready(), "result survives graph storage retirement");
  });
}
}  // namespace

int main() {
  completion_histories();
  scope_histories();
  graph_histories();
  schedule::verify_selection();
}
