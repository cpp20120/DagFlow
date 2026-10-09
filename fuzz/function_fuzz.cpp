#include <array>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <dagflow/inplace_function.hpp>
#include "byte_reader.hpp"
#include "check.hpp"

namespace {
int live = 0;
template<std::size_t Padding, std::size_t Alignment = alignof(int)>
struct alignas(Alignment) Callable {
  int value;
  std::array<unsigned char, Padding> padding{};
  explicit Callable(int v) : value(v) { ++live; }
  Callable(const Callable&) = delete;
  Callable(Callable&& other) noexcept : value(other.value) { ++live; }
  ~Callable() { --live; }
  int operator()(int input) {
    dagflow::fuzz::check(reinterpret_cast<std::uintptr_t>(this) % Alignment == 0);
    return input + value;
  }
};

template<class Function, bool Heap>
void exercise(dagflow::fuzz::Bytes& bytes) {
  std::array<Function, 4> functions;
  std::array<int, 4> values{};
  std::array<bool, 4> present{};
  const unsigned steps = 1 + bytes.bound(128);
  for (unsigned i = 0; i < steps; ++i) {
    const auto index = bytes.bound(4);
    const auto other = (index + 1 + bytes.bound(3)) % 4;
    switch (bytes.bound(5)) {
      case 0: {
        const int v = bytes.next();
        if constexpr (Heap) {
          switch (bytes.bound(3)) {
            case 0: functions[index] = Callable<1>(v); break;
            case 1: functions[index] = Callable<128>(v); break;
            default: functions[index] = Callable<1, 128>(v); break;
          }
        } else { functions[index] = Callable<1>(v); }
        values[index] = v; present[index] = true;
        break;
      }
      case 1: functions[index].reset(); present[index] = false; break;
      case 2:
        functions[index] = std::move(functions[other]);
        values[index] = values[other]; present[index] = present[other];
        present[other] = false;
        break;
      case 3: {
        auto temporary = std::move(functions[index]);
        dagflow::fuzz::check(!functions[index]);
        functions[index] = std::move(temporary);
        dagflow::fuzz::check(!temporary);
        break;
      }
      default:
        if (present[index]) {
          int input = bytes.next();
          dagflow::fuzz::check(functions[index](input) == input + values[index]);
        }
        break;
    }
    int expected_live = 0;
    for (unsigned j = 0; j < 4; ++j) {
      dagflow::fuzz::check(bool(functions[j]) == present[j]);
      expected_live += present[j];
      if (present[j]) dagflow::fuzz::check(functions[j](0) == values[j]);
    }
    dagflow::fuzz::check(live == expected_live);
  }
}
}

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  if (size > 4096) return 0;
  dagflow::fuzz::Bytes bytes(data, size);
  if (bytes.bit()) exercise<dagflow::small_function<int(int)>, true>(bytes);
  else exercise<dagflow::inplace_function<int(int)>, false>(bytes);
  dagflow::fuzz::check(live == 0);
  return 0;
}
