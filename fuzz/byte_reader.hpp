#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace dagflow::fuzz {
// Bounded structured input: short inputs remain valid test cases. This is
// deliberately mutation-friendly (no checksum, magic, or rejection loop).
class Bytes {
 public:
  Bytes(const std::uint8_t* data, std::size_t size) : input_(data, size) {}

  std::uint8_t next() noexcept {
    return pos_ < input_.size() ? input_[pos_++] : 0;
  }
  unsigned bound(unsigned limit) noexcept {
    return limit ? static_cast<unsigned>(next()) % limit : 0;
  }
  bool bit() noexcept { return (next() & 1u) != 0; }

 private:
  std::span<const std::uint8_t> input_;
  std::size_t pos_{0};
};
}  // namespace dagflow::fuzz
