
#include <atomic>
#include <memory>

#include "adversarial_support.hpp"

namespace {
using Credit = dagflow::detail::CompletionCredit;

struct Payload {
  dagflow::Pool& pool;
  dagflow::Handle self;
  Credit nested;
  dagflow::Handle& registered;
  std::atomic<unsigned>& destroyed;

  ~Payload() {
    CHECK(self.valid() && !self.ready());
    auto other = nested.handle();
    registered = pool.combine({self, other, self, {}});
    CHECK(!registered.ready());

    nested.finish(); // Nested retirement while this state is terminalizing.
    CHECK(other.ready() && !registered.ready());

    self = {}; // Drop the payload's observer before the terminal state is ready.
    CHECK(destroyed.fetch_add(1) == 0);
  }
};
}

int main() {
  dagflow::Pool pool(adversarial::config());
  pool.shutdown(); // Combining completions does not require worker admission.

  for (unsigned round = 0; round < 128; ++round) {
    std::atomic<unsigned> destroyed{0};
    dagflow::Handle registered;

    auto payload = std::unique_ptr<Payload>(
        new Payload{pool, {}, Credit::create(), registered, destroyed});
    auto* raw = payload.get();
    auto root = Credit::create(std::move(payload));
    raw->self = root.handle();

    const auto failure = std::make_exception_ptr(
        adversarial::Failure{round});

    if (round % 3 == 1) root.fail(failure);
    if (round % 3 == 2) raw->nested.fail(failure);

    // There is deliberately no external root observer keeping it alive.
    root.finish();

    CHECK(destroyed == 1 && registered.ready());

    if (round % 3)
      CHECK(adversarial::failure_id(registered) == round);
    else
      registered.rethrow_if_failed();
  }
}
