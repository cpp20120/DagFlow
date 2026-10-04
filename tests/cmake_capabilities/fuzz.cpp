#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <dagflow/dagflow.hpp>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data,
                                      std::size_t size) {
  dagflow::Pool pool({.threads = 2, .pin_threads = false});
  dagflow::TaskScope scope(pool);
  unsigned actual = 0;
  const unsigned expected = size ? data[0] : 42;
  scope.spawn([&] { actual = expected; });
  scope.join();
  if (actual != expected) {
    std::abort();
  }
  return 0;
}
