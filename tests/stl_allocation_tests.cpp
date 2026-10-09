#include <cstdint>
#include <limits>
#include <list>
#include <new>
#include <type_traits>
#include <vector>

#include <dagflow/thread_pool.hpp>
#include "support.hpp"

using dagflow::detail::RuntimeAllocator;

namespace {
std::size_t allocations = 0;
std::size_t live = 0;
std::size_t last_bytes = 0;
std::size_t last_alignment = 0;
bool fail_allocation = false;

struct alignas(256) AlignedValue {
  int value = 42;
};

struct ThrowsOnCopy {
  static inline int alive = 0;
  ThrowsOnCopy() { ++alive; }
  ThrowsOnCopy(const ThrowsOnCopy&) { throw 42; }
  ~ThrowsOnCopy() { --alive; }
};
}  // namespace

// Substitute the runtime boundary, so accidental use of std::allocator would
// bypass these counters even though both backends ultimately use new here.
namespace dagflow::detail {
void* allocate_bytes(std::size_t bytes, std::size_t alignment) {
  if (fail_allocation) throw std::bad_alloc{};
  void* storage = alignment > __STDCPP_DEFAULT_NEW_ALIGNMENT__
                      ? ::operator new(bytes, std::align_val_t{alignment})
                      : ::operator new(bytes);
  ++allocations;
  ++live;
  last_bytes = bytes;
  last_alignment = alignment;
  return storage;
}
void deallocate_bytes(void* storage, std::size_t alignment) noexcept {
  CHECK(live > 0);
  --live;
  if (alignment > __STDCPP_DEFAULT_NEW_ALIGNMENT__)
    ::operator delete(storage, std::align_val_t{alignment});
  else
    ::operator delete(storage);
}
}  // namespace dagflow::detail

int main() {
  using Vector = std::vector<AlignedValue, RuntimeAllocator<AlignedValue>>;
  static_assert(std::is_nothrow_move_assignable_v<Vector>);
  static_assert(std::allocator_traits<RuntimeAllocator<int>>::is_always_equal{});
  static_assert(std::is_same_v<
                decltype(dagflow::Config::worker_shards)::allocator_type,
                RuntimeAllocator<uint32_t>>);
  static_assert(std::is_same_v<
                decltype(dagflow::detail::CompletionState::dependents)::allocator_type,
                RuntimeAllocator<dagflow::detail::CompletionCredit>>);
  {
    Vector source(3);
    CHECK(live == 1 && allocations == 1);
    CHECK(last_bytes == 3 * sizeof(AlignedValue));
    CHECK(last_alignment == alignof(AlignedValue));
    auto* storage = source.data();
    CHECK(reinterpret_cast<std::uintptr_t>(storage) % alignof(AlignedValue) == 0);
    Vector destination(1);
    const auto before_move = allocations;
    destination = std::move(source);
    CHECK(destination.data() == storage && destination[2].value == 42);
    CHECK(allocations == before_move && live == 1);
    Vector swapped;
    swapped.swap(destination);
    CHECK(swapped.data() == storage && allocations == before_move);

    fail_allocation = true;
    bool caught = false;
    try { swapped.reserve(swapped.capacity() + 1); }
    catch (const std::bad_alloc&) { caught = true; }
    fail_allocation = false;
    CHECK(caught && swapped.data() == storage && swapped.size() == 3);
  }
  CHECK(live == 0);
  {
    // A node container exercises allocator_traits rebind and converting ctor.
    std::list<AlignedValue, RuntimeAllocator<AlignedValue>> nodes;
    nodes.emplace_back();
    CHECK(live == 1 && last_alignment >= alignof(AlignedValue));
    CHECK(reinterpret_cast<std::uintptr_t>(&nodes.front()) %
              alignof(AlignedValue) == 0);
  }
  CHECK(live == 0);
  {
    std::vector<ThrowsOnCopy, RuntimeAllocator<ThrowsOnCopy>> values(1);
    auto* storage = values.data();
    bool caught = false;
    try { values.reserve(values.capacity() + 1); }
    catch (int) { caught = true; }
    CHECK(caught && values.data() == storage && values.size() == 1);
    CHECK(live == 1 && ThrowsOnCopy::alive == 1);
  }
  CHECK(live == 0 && ThrowsOnCopy::alive == 0);
  {
    dagflow::Config config;
    const auto before = allocations;
    config.worker_shards = {1, 0, 1, 0};
    auto copy = config;
    CHECK(allocations == before + 2 && live == 2);
    CHECK(copy.worker_shards == config.worker_shards);
  }
  CHECK(live == 0);
  RuntimeAllocator<AlignedValue> allocator;
  auto* empty = allocator.allocate(0);
  allocator.deallocate(empty, 0);
  CHECK(live == 0);
  const auto before_overflow = allocations;
  bool caught = false;
  try {
    auto* storage = allocator.allocate(std::numeric_limits<std::size_t>::max());
    allocator.deallocate(storage, std::numeric_limits<std::size_t>::max());
  } catch (const std::bad_array_new_length&) { caught = true; }
  CHECK(caught && allocations == before_overflow && live == 0);
}
