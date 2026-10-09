#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <stdexcept>
#include <utility>
#include <vector>
#include <dagflow/handle.hpp>
#include <dagflow/thread_pool.hpp>
#include <span>
#include <dagflow/detail/runtime_memory.hpp>
#include "byte_reader.hpp"
#include "check.hpp"
using dagflow::fuzz::check;
namespace {
struct Payload {
  std::atomic<unsigned>* destroyed;
  explicit Payload(std::atomic<unsigned>& d) : destroyed(&d) {}
  ~Payload() { destroyed->fetch_add(1, std::memory_order_release); }
};
void exercise(dagflow::fuzz::Bytes& b) {
  std::atomic<unsigned> destroyed{0};
  auto owner = dagflow::detail::make_owned<Payload>(destroyed);
  auto sentinel = dagflow::detail::CompletionCredit::create(std::move(owner));
  auto handle = sentinel.handle();
  auto copy = handle;
  auto moved = std::move(copy);
  check(!copy.valid() && moved.valid() && !handle.ready());
  std::vector<dagflow::detail::CompletionCredit> live;
  const unsigned n = 1 + b.bound(64);
  for (unsigned i=0;i<n;++i) live.emplace_back(sentinel.fork());
  // A credit can fork more credits; a move must transfer without retiring.
  for (unsigned i=0;i<n;++i) {
    if (b.bit()) live.emplace_back(live[i].fork());
    if (b.bit()) { auto temp = std::move(live[i]); live[i] = std::move(temp); }
  }
  if (b.bit()) sentinel.fail(std::make_exception_ptr(std::runtime_error("credit fuzz")));
  const bool errored = [&] { try { handle.rethrow_if_failed(); } catch (...) { return true; } return false; }();
  // Error publication can precede completion but never implies readiness.
  sentinel.finish();
  check(!handle.ready() && destroyed.load() == 0);
  for (std::size_t i=live.size();i>0;--i) {
    const std::size_t j = b.bound(static_cast<unsigned>(i));
    live[j].finish();
    live[j] = std::move(live[i-1]);
    live.pop_back();
    check(destroyed.load(std::memory_order_acquire)==unsigned(live.empty()));
    check(handle.ready()==live.empty());
  }
  check(destroyed.load()==1 && handle.ready() && moved.ready());
  bool observed=false;
  try { handle.rethrow_if_failed(); } catch (const std::runtime_error&) { observed=true; }
  check(observed==errored);
  // The dependent counter graph is distinct from scheduler packets. Register
  // dependents concurrently with physical task completion, then compose more
  // already-completed or still-live handles into a fresh downstream counter.
  if (b.bit()) {
    dagflow::Config cfg; cfg.threads=1+b.bound(3); cfg.pin_threads=false;
    cfg.idle_us_min=0; cfg.idle_us_max=100;
    dagflow::Pool pool(cfg);
    std::atomic<unsigned> calls{0};
    std::vector<dagflow::Handle> inputs;
    const unsigned tasks=1+b.bound(20);
    for(unsigned i=0;i<tasks;++i)
      inputs.emplace_back(pool.submit([&calls]{ calls.fetch_add(1); }));
    auto combined=pool.combine(std::span<const dagflow::Handle>{inputs});
    const unsigned depth=b.bound(16);
    for(unsigned i=0;i<depth;++i)
      combined=pool.combine({combined});
    pool.wait_and_rethrow(combined);
    check(calls.load(std::memory_order_acquire)==tasks);
    check(combined.ready());
    pool.wait_idle();
  }
}
}
extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data,std::size_t size) {
  if(size>4096)return 0;
  dagflow::fuzz::Bytes b(data,size); exercise(b); return 0;
}
