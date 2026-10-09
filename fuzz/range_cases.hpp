#pragma once

#include <array>
#include <cstddef>
#include <dagflow/config.hpp>
#include <dagflow/detail/fuzz_points.hpp>
#include "byte_reader.hpp"

namespace dagflow::fuzz {
inline std::size_t range_size(Bytes& bytes) {
  constexpr auto chunk = DAGFLOW_DEFAULT_RANGE_CHUNK;
  constexpr std::array<std::size_t, 12> boundaries{
      0, 1, 2, 31, 32, 33, chunk - 1, chunk, chunk + 1,
      2 * chunk - 1, 2 * chunk, 2 * chunk + 1};
  return boundaries[bytes.bound(boundaries.size())];
}

// Only the producer thread is affected. Always remove the injection before
// inspecting results or attempting recovery; worker cleanup stays available.
class AllocationBudget {
 public:
  explicit AllocationBudget(int value)
      : previous_(detail::fuzz_points::allocation_budget) {
    detail::fuzz_points::allocation_budget = value;
  }
  ~AllocationBudget() { detail::fuzz_points::allocation_budget = previous_; }
  AllocationBudget(const AllocationBudget&) = delete;
  AllocationBudget& operator=(const AllocationBudget&) = delete;
 private:
  int previous_;
};
} // namespace dagflow::fuzz
