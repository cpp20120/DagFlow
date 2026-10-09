#include <atomic>
#include <memory>
#include <stdexcept>

#include <dagflow/task_scope.hpp>
#include "adversarial_support.hpp"

int main() {
  for (unsigned round = 0; round < 16; ++round) {
    dagflow::Pool pool(adversarial::config());
    dagflow::TaskScope scope(pool);
    constexpr unsigned children = 8;
    std::atomic<unsigned> destroyed{0}, rejected{0}, helped{0};
    struct Capture {
      dagflow::Pool& pool;
      dagflow::TaskScope& scope;
      std::atomic<unsigned> &destroyed, &rejected, &helped;
      ~Capture() {
        CHECK(scope.cancelled() && !scope.completion().ready());
        CHECK(!scope.spawn([] { CHECK(false); }));
        try { scope.wait(); CHECK(false); }
        catch (const std::logic_error&) { ++rejected; }
        // Helping may recursively destroy another skipped task's capture.
        auto task = pool.submit([this] { ++helped; });
        pool.wait(task);
        task.rethrow_if_failed();
        CHECK(scope.cancelled());
        ++destroyed;
      }
    };
    CHECK(scope.spawn([&](dagflow::TaskScope::Context& context) {
      scope.close();
      for (unsigned i = 0; i < children; ++i) {
        CHECK(context.spawn([capture = std::unique_ptr<Capture>(
            new Capture{pool, scope, destroyed, rejected, helped})] { CHECK(false); }));
      }
      // The only worker has not run any queued child. All their captures must
      // still be destroyed under active self-join protection after cancellation.
      context.cancel();
      CHECK(!context.spawn([] { CHECK(false); }));
    }));
    scope.join();
    CHECK(scope.completion().ready() && !scope.last_error());
    CHECK(destroyed == children && rejected == children && helped == children);
    dagflow::TaskScope healthy(pool);
    CHECK(healthy.spawn([&](dagflow::TaskScope::Context& context) {
      CHECK(!context.cancelled());
      CHECK(context.spawn([&] { ++helped; }));
    }));
    healthy.join();
    CHECK(helped == children + 1);
  }
}
