#pragma once

#include <atomic>
#include <cstddef>

namespace adversarial::allocation {
// Only the dedicated test runtime links this backend. Other threads keep
// allocating normally, including exception reporting and task retirement.
struct Fault {
  int remaining = -1;
  void (*on_failure)(void*) noexcept = nullptr;
  void* context = nullptr;
};
inline thread_local Fault fault;
inline std::atomic<std::size_t> live{0};
}
