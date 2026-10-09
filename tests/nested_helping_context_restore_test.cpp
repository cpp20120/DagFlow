#include <atomic>
#include <stdexcept>

#include <dagflow/task_scope.hpp>
#include "adversarial_support.hpp"

namespace {
void level(dagflow::Pool& pool, unsigned depth, std::atomic<unsigned>& descendants,
           std::atomic<unsigned>& rejected, std::atomic<unsigned>& caught) {
  dagflow::TaskScope scope(pool);
  CHECK(scope.spawn([&](dagflow::TaskScope::Context& context) {
    scope.close();
    if (depth) {
      try { level(pool, depth - 1, descendants, rejected, caught); }
      catch (const adversarial::Failure& failure) {
        CHECK((depth - 1) % 2 && failure.id == depth - 1);
        ++caught;
      }
    }
    // Child invocation, cleanup, exception collection and destruction have all
    // unwound. Our ancestor frame must still reject self-join.
    try { scope.wait(); CHECK(false); }
    catch (const std::logic_error&) { ++rejected; }
    CHECK(!context.cancelled() && !scope.last_error());
    {
      dagflow::TaskScope cancelled(pool);
      cancelled.cancel();
      cancelled.join();
    }
    CHECK(!context.cancelled());
    CHECK(context.spawn([&] { ++descendants; })); // Allowed after external close.
    if (depth % 2) throw adversarial::Failure{depth};
  }));
  scope.join();
  scope.wait(); // Finished child frames must no longer count as active here.
}
}

int main() {
  dagflow::Pool pool(adversarial::config());
  for (unsigned round = 0; round < 24; ++round) {
    std::atomic<unsigned> descendants{0}, rejected{0}, caught{0};
    auto root = pool.submit([&] { level(pool, 7, descendants, rejected, caught); });
    pool.wait(root);
    CHECK(adversarial::failure_id(root) == 7);
    CHECK(descendants == 4 && rejected == 8 && caught == 3);
    // Reuse the sole worker after its whole helping stack has disappeared.
    auto recovery = pool.submit([&] {
      dagflow::TaskScope fresh(pool);
      CHECK(fresh.spawn([&](dagflow::TaskScope::Context& context) {
        CHECK(!context.cancelled());
        CHECK(context.spawn([&] { ++descendants; }));
      }));
      fresh.join();
    });
    pool.wait(recovery);
    recovery.rethrow_if_failed();
    CHECK(descendants == 5);
  }
}
