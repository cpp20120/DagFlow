#include <array>
#include <atomic>
#include <barrier>
#include <cstdint>
#include <limits>
#include <thread>
#include <vector>

#include <dagflow/detail/parking_lot.hpp>
#include "adversarial_support.hpp"

namespace {
void await_round(const std::atomic<unsigned>& value, unsigned round) {
  auto seen = value.load(std::memory_order_acquire);
  while (seen < round) {
    value.wait(seen, std::memory_order_acquire);
    seen = value.load(std::memory_order_acquire);
  }
}

void exercise(bool competing_waker) {
  std::array<uint32_t, 129> homes{};
  for (unsigned i = 0; i < homes.size(); ++i) homes[i] = (i % 3) * 2;
  dagflow::detail::Scheduler topology(129, 7, 1, homes);
  dagflow::detail::ParkingLot parking(topology);
  std::atomic<bool> stop{false};
  // Offsets are in packed membership order, not raw worker IDs: straddle both
  // 64-bit word boundaries despite the noncontiguous worker-to-shard mapping.
  const auto order = topology.worker_order();
  const std::array<uint32_t, 6> owners{order[0], order[63], order[64],
                                       order[65], order[127], order[128]};
  for (auto owner : owners) {
    for (unsigned repeat = 0; repeat < 64; ++repeat) {
      auto epoch = parking.prepare(owner);
      parking.cancel(owner);
      parking.wake_one(6); // No announcement remains anywhere.
      CHECK(parking.prepare(owner) == epoch);
      parking.wake_one(6); // Empty home must find the remote announcement.
      parking.wait(owner, epoch, std::numeric_limits<uint32_t>::max(), stop);
      CHECK(parking.prepare(owner) == epoch + 1);
      parking.cancel(owner);
    }
  }

  constexpr unsigned rounds = 256;
  std::array<std::atomic<unsigned>, owners.size()> announced{}, completed{};
  std::array<unsigned, owners.size()> payload{};
  std::atomic<unsigned> published{0};
  std::barrier next_round(owners.size() + 1);
  std::vector<std::thread> waiters;
  for (unsigned i = 0; i < owners.size(); ++i) {
    waiters.emplace_back([&, i] {
      uint64_t previous = 0;
      for (unsigned round = 1; round <= rounds; ++round) {
        do {
          const auto epoch = parking.prepare(owners[i]);
          CHECK(epoch >= previous);
          previous = epoch;
          announced[i].store(round, std::memory_order_release);
          announced[i].notify_one();
          if (published.load(std::memory_order_acquire) >= round)
            parking.cancel(owners[i]);
          else
            parking.wait(owners[i], epoch, std::numeric_limits<uint32_t>::max(), stop);
        } while (published.load(std::memory_order_acquire) < round);
        CHECK(payload[i] == round * 17 + i);
        completed[i].store(round, std::memory_order_release);
        completed[i].notify_one();
        next_round.arrive_and_wait();
      }
    });
  }
  std::thread noise;
  if (competing_waker) noise = std::thread([&] {
    while (!stop.load(std::memory_order_acquire)) {
      parking.wake_all();
      std::this_thread::yield();
    }
  });
  for (unsigned round = 1; round <= rounds; ++round) {
    for (const auto& value : announced) await_round(value, round);
    for (unsigned i = 0; i < owners.size(); ++i) payload[i] = round * 17 + i;
    published.store(round, std::memory_order_release);
    for (unsigned i = 0; i < owners.size(); ++i) parking.wake_one(6);
    for (const auto& value : completed) await_round(value, round);
    // A later generation must not consume one of this generation's six wakes.
    next_round.arrive_and_wait();
  }
  for (auto& waiter : waiters) waiter.join();
  stop.store(true, std::memory_order_release);
  if (noise.joinable()) noise.join();
}
}

int main() {
  exercise(false); // Extra wakeups must not conceal a lost publication wake.
  exercise(true);  // Delayed notifiers race with later prepare/cancel epochs.
}
