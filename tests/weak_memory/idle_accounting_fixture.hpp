#pragma once
#include <array>
#include <cstdint>
#include <dagflow/config.hpp>
#include <dagflow/detail/fuzz_points.hpp>
#include <limits>
#include <memory>
#include <stdexcept>

#include "model_support.hpp"

// Stable storage replaces allocation/registry management only. The actual
// snapshot still traverses worker lanes and the producer linked list.
template <class T>
struct OwnedArray {
  std::array<T, 2> lanes{};
  struct Size {
    std::size_t count;
  } size;
  explicit OwnedArray(std::size_t count) : size{count} {
    RL_ASSERT(count > 0 && count <= lanes.size());
  }
  T& operator[](std::size_t i) {
    RL_ASSERT(i < size.count);
    return lanes[i];
  }
  const T& operator[](std::size_t i) const {
    RL_ASSERT(i < size.count);
    return lanes[i];
  }
  Size get_deleter() const { return size; }
};

template <class T>
using OwnedObject = std::unique_ptr<T>;
