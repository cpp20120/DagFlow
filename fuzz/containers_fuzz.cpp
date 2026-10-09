#include <array>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>
#include <dagflow/detail/small_vector.hpp>
#include "byte_reader.hpp"
#include "check.hpp"

namespace {
struct Value {
  static inline int live = 0;
  int value;
  explicit Value(int v) : value(v) { ++live; }
  Value(const Value& v) : value(v.value) { ++live; }
  Value(Value&& v) noexcept : value(v.value) { ++live; }
  Value& operator=(const Value&) = default;
  Value& operator=(Value&&) = default;
  ~Value() { --live; }
};
}

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  if (size > 4096) return 0;
  using dagflow::fuzz::check;
  dagflow::fuzz::Bytes bytes(data, size);
  {
    std::array<dagflow::small_vector<Value, 4>, 2> actual;
    std::array<std::vector<int>, 2> expected;
    const unsigned steps = 1 + bytes.bound(128);
    for (unsigned i = 0; i < steps; ++i) {
      const auto index = bytes.bound(2), other = 1 - index;
      auto& a = actual[index];
      auto& e = expected[index];
      switch (bytes.bound(8)) {
        case 0:
          if (e.size() < 128) {
            int value = bytes.next();
            a.emplace_back(value);
            e.push_back(value);
          }
          break;
        case 1:
          if (!e.empty()) { a.pop_back(); e.pop_back(); }
          break;
        case 2: a.reserve(bytes.bound(192)); break;
        case 3: a.clear(); e.clear(); break;
        case 4: {
          auto temporary = std::move(a);
          a = std::move(temporary);
          break;
        }
        case 5:
          a = std::move(actual[other]);
          e = expected[other];
          // Explicitly clear moved-from objects: their values are unspecified.
          actual[other].clear(); expected[other].clear();
          break;
        case 6:
          if (!e.empty() && e.size() < 128) {
            const auto at = bytes.bound(static_cast<unsigned>(e.size()));
            const auto value = e[at];
            a.push_back(a[at]);  // May alias inline storage during spill.
            e.push_back(value);
          }
          break;
        default:
          try { (void)a.at(a.size()); check(false); }
          catch (const std::out_of_range&) {}
          break;
      }
      for (unsigned j = 0; j < 2; ++j) {
        check(actual[j].size() == expected[j].size());
        check(actual[j].capacity() >= actual[j].size());
        for (std::size_t k = 0; k < expected[j].size(); ++k)
          check(actual[j][k].value == expected[j][k]);
      }
      check(Value::live == static_cast<int>(expected[0].size() + expected[1].size()));
    }
  }
  check(Value::live == 0);
  return 0;
}
