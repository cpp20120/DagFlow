#pragma once
#include <array>
#include <bit>
#include <chrono>
#include <cstdint>
#include <dagflow/detail/fuzz_points.hpp>
#include <dagflow/detail/runtime_diagnostics.hpp>

#include "model_support.hpp"

using dagflow::detail::runtime_count;
using dagflow::detail::RuntimeEvent;

template <class T>
struct OneEntry {
  T value{};
  T& operator[](std::size_t i) {
    RL_ASSERT(i == 0);
    return value;
  }
  const T& operator[](std::size_t i) const {
    RL_ASSERT(i == 0);
    return value;
  }
  struct Size {
    std::size_t count = 1;
  };
  Size get_deleter() const { return {}; }
};

struct UntimedCV {
  rl::condition_variable condition;
  template <class Duration, class Predicate>
  bool wait_for(rl::unique_lock<rl::mutex>& lock, Duration,
                Predicate predicate) {
    condition.wait(lock, predicate);
    return true;
  }
  void notify_one() { condition.notify_one(); }
};

// Fixed one-worker topology. The runner inserts the current Waiter/Domain
// declarations and method definitions; only storage construction is modeled.
constexpr unsigned epoch_spin_count = 0;
constexpr unsigned idle_word_bits = 64;
constexpr uint64_t all_idle_bits = ~uint64_t{0};
void spin_pause() {}
