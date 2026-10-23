#include <atomic>
#include <dagflow/detail/fuzz_points.hpp>
#include <dagflow/detail/parking_lot.hpp>

#include "schedule_exploration_support.hpp"

namespace {
namespace schedule = dagflow::test::schedule;
namespace hooks = dagflow::detail::fuzz_points;
struct Fixture {
  schedule::Controller& control;
  std::atomic<bool> timed_out{false};
  std::atomic<bool> committed_wait{false};
  bool tail = false;  // Waiter-only, including all predicate evaluations.
};
void checkpoint(hooks::Point point, void* context) noexcept {
  auto& fixture = *static_cast<Fixture*>(context);
  switch (point) {
    case hooks::Point::idle_announce:
    case hooks::Point::before_cv:
    case hooks::Point::wake_claim:
      fixture.control.checkpoint("announce/CV/epoch boundary");
      break;
    case hooks::Point::parking_predicate_false:
      if (!fixture.tail) {
        fixture.tail = true;
        fixture.committed_wait.store(true, std::memory_order_release);
        // The next instruction can block in CV wait. Keep the runtime mutex
        // handshake intact and allow the publisher to satisfy the wait.
        fixture.control.blocking_tail(0);
      }
      break;
    case hooks::Point::parking_timed_out:
      fixture.timed_out.store(true);
      break;
    default:
      break;
  }
}
}  // namespace

void histories(bool untimed) {
  schedule::explore(
      untimed ? "untimed-notification" : "announce-park-wake", 2,
      [&](schedule::Controller& control) {
        dagflow::detail::Scheduler topology(1, 1, 1);
        dagflow::detail::ParkingLot parking(topology);
        if (untimed) parking.fuzz_wait_without_timeout(0);
        std::atomic<bool> published{false}, stop{false};
        Fixture fixture{control};
        hooks::install(checkpoint, &fixture);
        auto waiter = control.actor(0, [&] {
          const auto epoch = parking.prepare(0);
          if (published.load(std::memory_order_acquire)) {
            parking.cancel(0);
            return;
          }
          control.checkpoint("final scan observed empty");
          parking.wait(0, epoch, 500000, stop);
          control.require(published.load(std::memory_order_acquire),
                          "wake follows a committed publication");
          control.require(
              !fixture.timed_out.load(),
              "published work wakes announced waiter without timeout");
        });
        auto publisher = control.actor(1, [&] {
          published.store(true, std::memory_order_release);
          control.checkpoint("publication before wake");
          parking.wake_one(0);
          if (untimed && fixture.committed_wait.load(std::memory_order_acquire))
            control.require(
                parking.fuzz_notifications(0) == 1,
                "committed sleeper requires an actual CV notification");
        });
        control.drive();
        waiter.join();
        publisher.join();
        hooks::clear();
        control.require(!parking.fuzz_sleeping(0),
                        "wait retires sleeping state");
      });
}

int main() {
  histories(false);
  histories(true);
  schedule::verify_selection();
}
