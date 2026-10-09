#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

#include <dagflow/detail/fuzz_points.hpp>

namespace dagflow::fuzz {
// Fuzz-only scheduling perturbation. With bit 7 set, pause the FIRST visit
// to a chosen checkpoint until a DIFFERENT thread hits another chosen point.
// A bounded timeout ensures missing partners cannot hang the fuzz process.
// Only lock-free/unlocked checkpoints are valid arm/release locations.
// This samples selected interleavings; it is not exhaustive model checking.
#if defined(DAGFLOW_FUZZ_HOOKS)
struct Perturb {
  std::uint32_t mask;
  std::atomic<unsigned> hits{0};
  std::atomic<unsigned> gate{0}; // 0 idle, 1 armed, 2 released, 3 expired
  std::uint8_t arm_point;
  std::uint8_t release_point;
  bool gated;

  explicit Perturb(std::uint32_t m)
      : mask(m),
        arm_point((m >> 3u) % 4u),
        release_point((m >> 5u) % 4u),
        gated((m & 0x80u) != 0) {
    // Distinct events; a parked worker never needs to release itself.
    if (release_point == arm_point) release_point = (release_point + 1) % 4;
    dagflow::detail::fuzz_points::install(&Perturb::on_point, this);
  }
  Perturb(const Perturb&) = delete;
  Perturb& operator=(const Perturb&) = delete;
  ~Perturb() { dagflow::detail::fuzz_points::clear(); }

  static void on_point(dagflow::detail::fuzz_points::Point point, void* ptr) noexcept {
    auto& self = *static_cast<Perturb*>(ptr);
    const auto index = self.hits.fetch_add(1, std::memory_order_relaxed);
    const auto id = static_cast<unsigned>(point);
    if (self.gated && id == self.arm_point) {
      unsigned expected = 0;
      if (self.gate.compare_exchange_strong(expected, 1, std::memory_order_acq_rel)) {
        const auto until = std::chrono::steady_clock::now() + std::chrono::microseconds(200);
        while (self.gate.load(std::memory_order_acquire) == 1 &&
               std::chrono::steady_clock::now() < until)
          std::this_thread::yield();
        unsigned armed = 1;
        self.gate.compare_exchange_strong(armed, 3, std::memory_order_acq_rel);
      }
    } else if (self.gated && id == self.release_point) {
      unsigned armed = 1;
      self.gate.compare_exchange_strong(armed, 2, std::memory_order_acq_rel);
    }
    if ((self.mask & (1u << id)) && index % 3 == 0)
      std::this_thread::yield();
  }
};
#else
struct Perturb { explicit Perturb(std::uint32_t) {} };
#endif
}  // namespace dagflow::fuzz
