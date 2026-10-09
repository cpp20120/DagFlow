#pragma once
#include <cstdlib>

namespace dagflow::fuzz {
inline void check(bool condition) {
  if (!condition) std::abort();
}
}  // namespace dagflow::fuzz
