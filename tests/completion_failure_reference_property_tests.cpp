#include <array>
#include <cstdint>
#include <dagflow/dagflow.hpp>
#include <dagflow/handle.hpp>
#include <exception>
#include <latch>
#include <span>
#include <thread>

#include "property_support.hpp"

namespace {
using dagflow::test::property::Command;
using dagflow::test::property::Random;
using dagflow::test::property::require;
using Credit = dagflow::detail::CompletionCredit;

// Independent error oracle: the first failure of a source must survive its
// final retirement and be delivered to observers registered both before and
// after the source becomes ready. An unrelated source delays fan-in readiness.
class CompletionFailureReference {
  dagflow::Pool pool_{[] {
    dagflow::Config c;
    c.threads = 2;
    return c;
  }()};
  unsigned histories_{};

  static int observe_error(const dagflow::Handle& handle) {
    try {
      handle.rethrow_if_failed();
    } catch (int code) {
      return code;
    }
    return 0;
  }

 public:
  Command next(Random& rng) const {
    return {0, rng.bounded(16), rng.bounded(5)};
  }
  bool precondition(const Command& c) const {
    return c.op == 0 && c.a < 16 && c.b < 5;
  }
  void apply(const Command& c) {
    auto source = Credit::create();
    auto extra = source.fork();
    const auto source_handle = source.handle();
    auto unrelated = Credit::create();
    const auto unrelated_handle = unrelated.handle();
    const int first = 101 + static_cast<int>(c.a);
    const int second = first + 1000;
    const std::array<dagflow::Handle, 2> inputs{source_handle,
                                               unrelated_handle};
    dagflow::Handle join;
    const bool late = c.b == 2;
    const bool concurrent_registration = c.b == 4;
    if (!late && !concurrent_registration)
      join = pool_.combine(std::span<const dagflow::Handle>(inputs));

    // Both call sites target the *same* state. The earlier error must win,
    // independently of which live credit publishes it.
    if (c.a & 1) {
      extra.fail(std::make_exception_ptr(first));
      source.fail(std::make_exception_ptr(second));
    } else {
      source.fail(std::make_exception_ptr(first));
      extra.fail(std::make_exception_ptr(second));
    }
    if (concurrent_registration) {
      // Race registration against the last credit's terminal propagation.
      // The retained Handle keeps the source allocation alive for late joins.
      source.finish();
      require(!source_handle.ready(), "remaining credit holds source open");
      std::latch start(2);
      std::exception_ptr registration_error;
      std::thread registration([&] {
        start.arrive_and_wait();
        try {
          join = pool_.combine(std::span<const dagflow::Handle>(inputs));
        } catch (...) {
          registration_error = std::current_exception();
        }
      });
      start.arrive_and_wait();
      extra.finish();
      registration.join();
      if (registration_error) std::rethrow_exception(registration_error);
    } else if (c.b & 1) {
      source.finish();
      require(!source_handle.ready(), "fork keeps the source unfinished");
      extra.finish();
    } else {
      extra.finish();
      require(!source_handle.ready(), "root keeps the source unfinished");
      source.finish();
    }
    require(source_handle.ready(), "source becomes ready after final credit");
    require(observe_error(source_handle) == first,
            "the first source failure wins after retirement");

    if (late) join = pool_.combine(std::span<const dagflow::Handle>(inputs));
    require(join.valid(), "fan-in observation was created");
    require(!join.ready(), "unrelated outstanding credit delays the join");
    unrelated.finish();
    pool_.wait(join);
    require(join.ready(), "the join completes after both sources");
    require(observe_error(join) == first,
            "source failure propagates through early or late fan-in");
    ++histories_;
  }
  void invariant() const { require(histories_ <= 10000, "bounded failure histories"); }
  void finish() { pool_.wait_idle(); }
};
}

int main() {
  dagflow::test::property::run<CompletionFailureReference>(
      "completion reference: first failure, dependent propagation and late join", 32, 32);
}
