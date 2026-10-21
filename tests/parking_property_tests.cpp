#include <array>
#include <atomic>
#include <cstdint>
#include <dagflow/detail/parking_lot.hpp>

#include "property_support.hpp"

namespace {
using namespace dagflow::test::property;

// An abstract registration/notification automaton. No fuzz-only observers or
// copied bitmap algorithm: prepare/cancel provide a reversible epoch probe
// while all virtual workers are quiescent on this one test thread.
template <unsigned Workers>
class ParkingModel {
  enum Op {
    Prepare,
    Cancel,
    WakeOne,
    WakeWorker,
    WakeAll,
    StopWait,
    TimedWait
  };
  struct State {
    bool registered{};
    std::uint64_t epoch{};
  };
  static constexpr unsigned Shards = 5;
  std::array<State, Workers> model_{};
  static std::array<std::uint32_t, Workers> placement() {
    std::array<std::uint32_t, Workers> result{};
    for (unsigned i = 0; i < Workers; ++i) result[i] = (i % 3) * 2;
    return result;
  }
  std::array<std::uint32_t, Workers> placement_ = placement();
  dagflow::detail::Scheduler scheduler_{Workers, Shards, 1, placement_};
  dagflow::detail::ParkingLot lot_{scheduler_};

 public:
  Command next(Random& random) const {
    auto op = static_cast<unsigned>(random.bounded(7));
    const auto worker = random.bounded(Workers);
    if ((op == StopWait || op == TimedWait || op == Cancel) &&
        !model_[worker].registered)
      op = Prepare;
    return {op, worker, random.bounded(Shards)};
  }
  bool precondition(const Command& c) const {
    if (c.op > TimedWait || c.a >= Workers || c.b >= Shards) return false;
    if (c.op == StopWait || c.op == TimedWait || c.op == Cancel)
      return model_[c.a].registered;
    return true;
  }
  void apply(const Command& c) {
    auto& state = model_[c.a];
    switch (c.op) {
      case Prepare:
        require(lot_.prepare(c.a) == state.epoch,
                "prepare observes notifications without inventing one");
        state.registered = true;
        break;
      case Cancel:
        lot_.cancel(c.a);
        state.registered = false;
        break;
      case WakeOne: {
        // First audit the complete set without restoring bits between wakes.
        // Otherwise the observation below could hide a lost second bit or a
        // claimed registration which was never actually cleared.
        for (unsigned i = 0; i <= Workers; ++i) lot_.wake_one(c.b);
        for (unsigned i = 0; i < Workers; ++i) {
          model_[i].epoch += model_[i].registered;
          require(lot_.prepare(i) == model_[i].epoch,
                  "repeated wake_one consumes every registration once");
          lot_.cancel(i);
        }
        for (unsigned i = 0; i < Workers; ++i)
          if (model_[i].registered) lot_.prepare(i);
        // Now check a single choice against the restored abstract state.
        lot_.wake_one(c.b);
        unsigned eligible = 0, preferred = 0, signalled = 0;
        for (unsigned i = 0; i < Workers; ++i) {
          eligible += model_[i].registered;
          preferred += model_[i].registered && placement_[i] == c.b;
        }
        for (unsigned i = 0; i < Workers; ++i) {
          const auto observed = lot_.prepare(i);
          lot_.cancel(i);
          if (observed != model_[i].epoch) {
            require(observed == model_[i].epoch + 1,
                    "one notification advances one epoch");
            require(model_[i].registered, "wake_one claims an idle worker");
            require(preferred == 0 || placement_[i] == c.b,
                    "wake_one prefers an eligible home-domain worker");
            ++signalled;
            model_[i].registered = false;
            model_[i].epoch = observed;
          }
        }
        require(signalled == unsigned(eligible != 0),
                "wake_one consumes exactly one available registration");
        // Restore only registrations which the notification did not consume.
        for (unsigned i = 0; i < Workers; ++i)
          if (model_[i].registered) lot_.prepare(i);
        break;
      }
      case WakeWorker:
        lot_.wake_worker(c.a);
        ++state.epoch;
        state.registered = false;
        break;
      case WakeAll:
        lot_.wake_all();
        for (auto& entry : model_) {
          ++entry.epoch;
          entry.registered = false;
        }
        break;
      case StopWait:
      case TimedWait: {
        const std::atomic<bool> stop{c.op == StopWait};
        lot_.wait(c.a, state.epoch, 0, stop);
        state.registered = false;
        break;
      }
    }
  }
  void invariant() {
    // Consume every actual registration before observing epochs. This detects
    // lost/extra registrations, including a cancel/timeout that forgot its bit.
    // Re-register the model's waiters afterwards; account for probe signals.
    for (unsigned i = 0; i <= Workers; ++i) lot_.wake_one(i % Shards);
    for (unsigned i = 0; i < Workers; ++i) {
      model_[i].epoch += model_[i].registered;
      require(lot_.prepare(i) == model_[i].epoch,
              "notification history and registration set match the model");
      if (!model_[i].registered) lot_.cancel(i);
    }
  }
};
}  // namespace

int main() {
  run<ParkingModel<1>>("parking_single", 24, 64);
  run<ParkingModel<63>>("parking_63", 12, 64);
  run<ParkingModel<64>>("parking_64", 12, 64);
  run<ParkingModel<65>>("parking_65", 12, 64);
  run<ParkingModel<127>>("parking_127", 12, 64);
  run<ParkingModel<128>>("parking_128", 12, 64);
  run<ParkingModel<129>>("parking_129", 12, 64);
}
