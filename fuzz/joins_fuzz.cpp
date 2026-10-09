#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <latch>
#include <memory>
#include <new>
#include <span>
#include <thread>
#include <utility>
#include <vector>

#include <dagflow/thread_pool.hpp>
#include "byte_reader.hpp"
#include "check.hpp"
#include "range_cases.hpp"

namespace {
using dagflow::fuzz::check;
struct RootError { unsigned id; };
struct Payload {
  std::atomic<unsigned>& destroyed;
  ~Payload() { destroyed.fetch_add(1, std::memory_order_release); }
};
struct Observed {
  dagflow::Handle handle;
  std::uint32_t roots;
};

void verify(const Observed& node, std::uint32_t failures,
            const std::array<std::atomic<unsigned>, 16>& destroyed) {
  check(node.handle.ready());
  bool failed = false;
  try { node.handle.rethrow_if_failed(); }
  catch (const RootError& error) {
    check(error.id < 16 && ((node.roots & failures) & (1u << error.id)) != 0);
    failed = true;
  }
  check(failed == ((node.roots & failures) != 0));
  for (unsigned i = 0; i < 16; ++i)
    if (node.roots & (1u << i)) check(destroyed[i].load(std::memory_order_acquire) == 1);
}

void exercise(dagflow::fuzz::Bytes& bytes) {
  const unsigned count = 1 + bytes.bound(16);
  const unsigned nodes = 1 + bytes.bound(64);
  const unsigned depth = 1 + bytes.bound(8) * 256 + bytes.next();
  const bool concurrent = bytes.bit();
  const int budget = bytes.bound(24);
  std::array<std::atomic<unsigned>, 16> destroyed{};
  std::vector<dagflow::detail::CompletionCredit> roots;
  std::vector<Observed> observations;
  roots.reserve(count);
  observations.reserve(count + nodes + 2);
  std::uint32_t failures = 0;
  for (unsigned i = 0; i < count; ++i) {
    roots.push_back(dagflow::detail::CompletionCredit::create(
        std::unique_ptr<Payload>(new Payload{destroyed[i]})));
    observations.push_back({roots.back().handle(), 1u << i});
    if (bytes.bit()) {
      failures |= 1u << i;
      roots.back().fail(std::make_exception_ptr(RootError{i}));
      // First failure wins even before readiness is published.
      roots.back().fail(std::make_exception_ptr(RootError{16}));
    }
    if (i != 0 && bytes.bit()) roots.back().finish();
  }
  {
    dagflow::Config cfg;
    cfg.threads = 1;
    cfg.pin_threads = false;
    dagflow::Pool pool(cfg);
    // Failure partway through dependent registration leaves attached credits
    // behind. Finishing the roots must reclaim those unobserved joins safely.
    std::vector<dagflow::Handle> inputs;
    for (const auto& node : observations) inputs.push_back(node.handle);
    try {
      dagflow::fuzz::AllocationBudget injection(budget);
      auto abandoned = pool.combine(std::span<const dagflow::Handle>(inputs));
    } catch (const std::bad_alloc&) {}
    inputs.clear();

    std::latch start(1);
    std::thread retire([&] {
      start.wait();
      if (concurrent) {
        for (unsigned i = count; i > 1; --i) {
          std::this_thread::yield();
          roots[i - 1].finish();
        }
      }
    });
    start.count_down();
    for (unsigned i = 0; i < nodes; ++i) {
      inputs.clear();
      std::uint32_t mask = 0;
      const unsigned fan_in = bytes.bound(17);
      for (unsigned j = 0; j < fan_in; ++j) {
        const auto index = bytes.bound(observations.size() + 1);
        if (index == observations.size()) inputs.emplace_back();
        else {
          inputs.push_back(observations[index].handle);
          mask |= observations[index].roots;
          if (bytes.bit()) inputs.push_back(inputs.back()); // Duplicate source credits.
        }
      }
      auto combined = pool.combine(std::span<const dagflow::Handle>(inputs));
      observations.push_back({std::move(combined), mask});
    }
    // Root zero stays live, guaranteeing a genuinely pending deep chain.
    // Intermediate observer handles are discarded to stress terminal storage
    // ownership and the iterative (not recursive) destruction worklist.
    Observed tail{pool.combine({observations.front().handle, observations.back().handle}),
                  observations.front().roots | observations.back().roots};
    const bool duplicate_chain = bytes.bit();
    for (unsigned i = 0; i < depth; ++i)
      tail.handle = duplicate_chain ? pool.combine({tail.handle, {}, tail.handle})
                                    : pool.combine({tail.handle});
    observations.push_back(std::move(tail));
    retire.join();
    for (unsigned i = 1; i < count; ++i) roots[i].finish();
    for (const auto& node : observations) {
      check(node.handle.ready() == ((node.roots & 1u) == 0));
      if (node.handle.ready()) verify(node, failures, destroyed);
    }
    check(destroyed[0].load() == 0);
    roots[0].finish();
    for (const auto& node : observations) {
      pool.wait(node.handle);
      verify(node, failures, destroyed);
    }
    // Registration against already closed dependent gates, including errors.
    auto late = pool.combine({observations.front().handle, observations.back().handle, {}});
    observations.push_back({std::move(late), observations.front().roots | observations.back().roots});
    pool.shutdown();
  }
  // Observer storage and errors must survive destruction of the pool and all
  // completion credits. Exercise copy assignment, overwrite, swap and moves.
  roots.clear();
  for (const auto& node : observations) {
    Observed copy{node.handle, node.roots};
    dagflow::Handle moved(std::move(copy.handle));
    check(!copy.handle.valid() && copy.handle.ready());
    copy.handle.swap(moved);
    moved = copy.handle;
    copy.handle = {};
    copy.handle = std::move(moved);
    verify(copy, failures, destroyed);
  }
  observations.clear();
  for (unsigned i = 0; i < count; ++i) check(destroyed[i].load() == 1);
}
} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  if (size > 4096) return 0;
  dagflow::fuzz::Bytes bytes(data, size);
  exercise(bytes);
  return 0;
}
