#include <string_view>

#include "model_support.hpp"
#include "parking_under_test.hpp"

// One persistent queue publication: a successful acquire scan also acquires
// payload initialization. Queue arbitration/stealing belongs to the deque
// model.
struct Announcement : rl::test_suite<Announcement, 2> {
  ParkingLot parking;
  ModelAtomic<bool> published{false};
  rl::var<int> payload;
  rl::var<bool> missed;
  rl::var<uint64_t> snapshot;
  void before() {
    payload($) = 0;
    missed($) = false;
    snapshot($) = 0;
  }
  void thread(unsigned actor) {
    if (actor == 0) {
      snapshot($) = parking.prepare(0);
      if (published.load(rl::memory_order_acquire)) {
        RL_ASSERT(payload($) == 42);
        parking.cancel(0);
      } else {
        missed($) = true;
      }
    } else {
      payload($) = 42;
      published.store(true, rl::memory_order_release);
      parking.wake_one(0);
    }
  }
  void after() {
    if (missed($))
      RL_ASSERT(parking.waiters_[0].epoch.load(rl::memory_order_relaxed) !=
                snapshot($));
  }
};

template <bool Preannounced>
struct Sleep : rl::test_suite<Sleep<Preannounced>, 2> {
  ParkingLot parking;
  ModelAtomic<bool> published{false}, stop{false};
  rl::var<int> payload;
  uint64_t initial_epoch{};
  void before() {
    payload($) = 0;
    if constexpr (Preannounced) initial_epoch = parking.prepare(0);
  }
  void thread(unsigned actor) {
    if (actor == 0) {
      const auto epoch = Preannounced ? initial_epoch : parking.prepare(0);
      if (!Preannounced && published.load(rl::memory_order_acquire))
        parking.cancel(0);
      else
        parking.wait(0, epoch, 1, stop);
      RL_ASSERT(published.load(rl::memory_order_acquire));
      RL_ASSERT(payload($) == 42);
      RL_ASSERT(!parking.waiters_[0].sleeping.load(rl::memory_order_relaxed));
    } else {
      payload($) = 42;
      published.store(true, rl::memory_order_release);
      if constexpr (Preannounced)
        parking.wake_worker(0);
      else
        parking.wake_one(0);
    }
  }
};

int main(int argc, char** argv) {
  if (argc != 2) return 2;
  const std::string_view suite(argv[1]);
  if (suite == "announce") return check_model<Announcement>(argv[1]);
  if (suite == "sleep") return check_model<Sleep<true>>(argv[1]);
  if (suite == "wake") return check_model<Sleep<false>>(argv[1]);
  return 2;
}
