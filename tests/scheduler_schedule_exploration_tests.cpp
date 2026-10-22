#include <array>
#include <dagflow/detail/chase_lev_deque.hpp>
#include <dagflow/detail/fuzz_points.hpp>
#include <dagflow/detail/scheduler.hpp>

#include "schedule_exploration_support.hpp"

namespace {
namespace schedule = dagflow::test::schedule;
namespace hooks = dagflow::detail::fuzz_points;
using schedule::Controller;

void queue_checkpoint(hooks::Point point, void* context) noexcept {
  auto& control = *static_cast<Controller*>(context);
  switch (point) {
    case hooks::Point::deque_before_publish:
    case hooks::Point::deque_after_owner_claim:
    case hooks::Point::deque_before_owner_cas:
    case hooks::Point::deque_after_thief_snapshot:
    case hooks::Point::deque_before_thief_cas:
      control.checkpoint("deque atomic boundary");
      break;
    default:
      break;
  }
}

void queue_histories(unsigned variant) {
  const auto name = std::array{"deque-owner-thief", "deque-two-thieves",
                               "deque-publication"}[variant];
  schedule::explore(name, 2, [&](Controller& control) {
    dagflow::detail::chase_lev_deque<int, 2> queue;
    if (variant != 2) control.require(queue.try_push(73), "initial custody");
    std::array<int, 2> output{-1, -1};
    std::array<bool, 2> acquired{};
    hooks::install(queue_checkpoint, &control);
    auto first = control.actor(0, [&] {
      if (variant == 2)
        control.require(queue.try_push(73), "publication accepted");
      else if (variant == 0)
        acquired[0] = queue.try_pop(output[0]);
      else
        acquired[0] = queue.try_steal(output[0]);
    });
    auto second =
        control.actor(1, [&] { acquired[1] = queue.try_steal(output[1]); });
    control.drive();
    first.join();
    second.join();
    hooks::clear();
    unsigned visits = 0;
    for (unsigned i = 0; i < 2; ++i) {
      control.require(output[i] == (acquired[i] ? 73 : -1),
                      "only successful acquisition may change output");
      visits += acquired[i];
    }
    int remaining = -1;
    if (queue.try_pop(remaining)) {
      control.require(remaining == 73, "publication preserves slot payload");
      ++visits;
    }
    control.require(visits == 1, "one publication grants exactly one custody");
    control.require(queue.empty(), "no duplicated packet remains queued");
  });
}

void scheduler_histories() {
  schedule::explore("scheduler-transfer", 2, [](Controller& control) {
    dagflow::detail::Scheduler scheduler(2, 1, 2);
    std::array<dagflow::detail::ScheduledTask, 2> packets;
    std::array<bool, 2> published{};
    std::array<unsigned, 2> visits{};
    const auto acquire = [&](unsigned worker) {
      bool recruit = false;
      auto* packet = scheduler.try_acquire(worker, recruit);
      if (!packet) return false;
      const auto index = packet == &packets[0] ? 0u : 1u;
      control.require(packet == &packets[index] && published[index],
                      "acquired packet has a committed publication");
      control.require(++visits[index] == 1, "scheduler transfers custody once");
      return true;
    };
    auto owner = control.actor(0, [&] {
      for (unsigned i = 0; i < 2; ++i) {
        published[i] = true;
        control.require(scheduler.try_submit_local(0, &packets[i]),
                        "bounded local publication succeeds");
        control.checkpoint("published packet");
      }
      while (acquire(0)) {
      }
    });
    auto thief = control.actor(1, [&] {
      for (unsigned i = 0; i < 2; ++i) {
        acquire(1);
        control.checkpoint("acquired or missed packet");
      }
      while (acquire(1)) {
      }
    });
    control.drive();
    owner.join();
    thief.join();
    control.require(visits[0] == 1 && visits[1] == 1,
                    "stolen batch siblings remain visible to executors");
  });
}
}  // namespace

int main() {
  for (unsigned variant = 0; variant < 3; ++variant) queue_histories(variant);
  scheduler_histories();
  schedule::verify_selection();
}
