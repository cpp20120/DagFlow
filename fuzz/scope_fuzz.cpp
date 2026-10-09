#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <dagflow/task_scope.hpp>
#include "byte_reader.hpp"
#include "check.hpp"

namespace {
struct Payload {
  std::atomic<unsigned>& destroyed;
  ~Payload() { destroyed.fetch_add(1); }
};
}

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  if (size > 4096) return 0;
  using dagflow::fuzz::check;
  dagflow::fuzz::Bytes bytes(data, size);
  dagflow::Config config;
  config.threads = 1 + bytes.bound(4);
  config.shards = bytes.bound(5);
  config.pin_threads = false;
  dagflow::Pool pool(config);
  const auto count = 1 + bytes.bound(32);
  const auto mode = bytes.bound(4); // normal, external cancel, callback failure, destructor join
  std::array<std::atomic<unsigned>, 64> calls{};
  std::array<std::atomic<bool>, 64> accepted{};
  std::atomic<unsigned> destroyed{0};
  dagflow::Handle completion;
  {
    dagflow::TaskScope scope(pool);
    completion = scope.completion();
    check(completion.valid() && !completion.ready());
    for (unsigned i = 0; i < count; ++i) {
      const bool child = bytes.bit();
      const bool fail = mode == 2 && i == 0;
      dagflow::SubmitOptions options;
      options.priority = bytes.bit() ? dagflow::Priority::High : dagflow::Priority::Normal;
      accepted[i].store(scope.spawn(
          [&, i, child, fail, payload = std::unique_ptr<Payload>(new Payload{destroyed})]
          (dagflow::TaskScope::Context& ctx) {
            check(calls[i].fetch_add(1) == 0);
            if (fail) throw std::runtime_error("fuzz callback failure");
            if (child) {
              accepted[i + 32].store(ctx.spawn([&, i] { check(calls[i + 32].fetch_add(1) == 0); }));
            }
          }, options));
      if (mode == 1 && i == count / 2) scope.cancel();
    }
    if (mode != 3) {
      scope.close();
      check(!scope.spawn([] {}));
      bool failed = false;
      try { scope.join(); }
      catch (const std::runtime_error&) { failed = true; }
      check(failed == (mode == 2));
      check(bool(scope.last_error()) == failed);
      scope.wait();
    }
  }
  check(completion.ready());
  check(destroyed.load() == count); // Includes rejected and cancelled captures.
  bool failed = false;
  try { completion.rethrow_if_failed(); }
  catch (const std::runtime_error&) { failed = true; }
  check(failed == (mode == 2));
  for (unsigned i = 0; i < 64; ++i) {
    check(calls[i].load() <= unsigned(accepted[i].load()));
    if (mode == 0 || mode == 3) check(calls[i].load() == unsigned(accepted[i].load()));
  }
  pool.wait_idle();
  return 0;
}
