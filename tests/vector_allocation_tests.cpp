#include <cstdint>
#include <new>
#include <stdexcept>
#include <utility>

#include "dagflow/detail/small_vector.hpp"
#include "support.hpp"

namespace {
std::size_t allocations = 0, deallocations = 0;
std::size_t last_bytes = 0, last_alignment = 0;
bool fail_allocation = false;

struct Value {
  static inline int alive = 0;
  static inline int copy_budget = 100;
  int value;
  explicit Value(int n) : value(n) {
    if (n < 0) throw std::runtime_error("construction");
    ++alive;
  }
  Value(const Value& other) : value(other.value) {
    if (copy_budget-- == 0) throw std::runtime_error("copy");
    ++alive;
  }
  Value(Value&& other) noexcept(false) : value(other.value) {
    other.value = -1;
    ++alive;
  }
  ~Value() { --alive; }
};

void allocation_policy() {
  const auto initial = allocations;
  {
    dagflow::small_vector<int, 2> values;
    CHECK(reinterpret_cast<std::uintptr_t>(values.data()) % alignof(int) == 0);
    values.push_back(1);
    values.push_back(2);
    values.reserve(2);
    CHECK(allocations == initial);
    values.push_back(values.front());
    CHECK(allocations == initial + 1 && values.capacity() == 4);
    CHECK(last_bytes == 4 * sizeof(int) &&
          last_alignment == DAGFLOW_CACHE_LINE_SIZE);
    CHECK(reinterpret_cast<std::uintptr_t>(values.data()) %
              DAGFLOW_CACHE_LINE_SIZE ==
          0);
    values.push_back(4);
    values.push_back(values[1]);
    CHECK(values.back() == 2 && values.capacity() == 8);
    CHECK(allocations == initial + 2 && last_bytes == 8 * sizeof(int));
    values.reserve(13);
    CHECK(values.capacity() == 13 && allocations == initial + 3);
    CHECK(last_bytes == 13 * sizeof(int));
    auto* storage = values.data();
    values.clear();
    for (int i = 0; i < 13; ++i) values.push_back(i);
    CHECK(values.data() == storage && allocations == initial + 3);
    values.pop_back();
    auto moved = std::move(values);
    CHECK(moved.data() == storage && allocations == initial + 3);
    CHECK(values.empty() && values.capacity() == 2);
    values.push_back(42);
    CHECK(values.front() == 42);
    dagflow::small_vector<int, 2> destination;
    destination.reserve(7);
    const auto before_move = allocations;
    const auto before_free = deallocations;
    destination = std::move(moved);
    CHECK(destination.data() == storage && destination.size() == 12);
    CHECK(allocations == before_move && deallocations == before_free + 1);
    CHECK(moved.empty() && moved.capacity() == 2);
    destination = std::move(values);
    CHECK(destination.size() == 1 && destination.front() == 42);
    CHECK(destination.capacity() == 2 && deallocations == before_free + 2);
  }
  CHECK(allocations == deallocations);
}

void allocation_failure() {
  dagflow::small_vector<int, 2> values;
  values.push_back(1);
  values.push_back(2);
  for (int spilled = 0; spilled < 2; ++spilled) {
    auto* storage = values.data();
    const auto capacity = values.capacity();
    const auto before = allocations;
    fail_allocation = true;
    bool caught = false;
    try {
      values.push_back(values.front());
    } catch (const std::bad_alloc&) {
      caught = true;
    }
    fail_allocation = false;
    CHECK(caught && values.size() == capacity && values.data() == storage);
    CHECK(values[0] == 1 && values[1] == 2 && allocations == before);
    caught = false;
    try {
      values.reserve(decltype(values)::max_size() + 1);
    } catch (const std::length_error&) {
      caught = true;
    }
    CHECK(caught && allocations == before && values.data() == storage);
    values.push_back(3);
    while (values.size() < values.capacity()) values.push_back(4);
  }
}

void relocation_failure() {
  {
    dagflow::small_vector<Value, 2> values;
    values.emplace_back(1);
    values.emplace_back(2);
    for (int spilled = 0; spilled < 2; ++spilled) {
      auto* storage = values.data();
      const auto size = values.size();
      const auto capacity = values.capacity();
      for (int operation = 0; operation < 3; ++operation) {
        const auto outstanding = allocations - deallocations;
        Value::copy_budget = 1;
        bool caught = false;
        try {
          if (operation == 0) values.reserve(capacity + 3);
          if (operation == 1) values.emplace_back(9);
          if (operation == 2) values.emplace_back(-1);
        } catch (const std::runtime_error&) {
          caught = true;
        }
        CHECK(caught && values.size() == size && values.capacity() == capacity);
        CHECK(values.data() == storage && values[0].value == 1);
        CHECK(values[1].value == 2 && Value::alive == static_cast<int>(size));
        CHECK(allocations - deallocations == outstanding);
      }
      Value::copy_budget = 100;
      values.emplace_back(3);
      while (values.size() < values.capacity()) values.emplace_back(4);
    }
  }
  CHECK(Value::alive == 0 && allocations == deallocations);
}

void extended_alignment() {
  struct alignas(256) Aligned {
    int value;
  };
  dagflow::small_vector<Aligned, 1> values;
  CHECK(reinterpret_cast<std::uintptr_t>(values.data()) % alignof(Aligned) ==
        0);
  values.emplace_back(Aligned{7});
  values.emplace_back(values.front());
  CHECK(last_alignment == alignof(Aligned));
  CHECK(last_bytes == 2 * sizeof(Aligned));
  CHECK(reinterpret_cast<std::uintptr_t>(values.data()) % alignof(Aligned) ==
        0);
  CHECK(values.front().value == 7 && values.back().value == 7);
}

void throwing_move_only() {
  struct MoveOnly : Value {
    explicit MoveOnly(int n) : Value(n) {}
    MoveOnly(const MoveOnly&) = delete;
    MoveOnly(MoveOnly&& other) : Value(std::move(other)) {
      if (copy_budget-- == 0) throw std::runtime_error("move");
    }
  };
  {
    dagflow::small_vector<MoveOnly, 2> values;
    values.emplace_back(1);
    values.emplace_back(2);
    Value::copy_budget = 1;
    bool caught = false;
    try {
      values.emplace_back(3);
    } catch (const std::runtime_error&) {
      caught = true;
    }
    CHECK(caught && values.size() == 2 && values.capacity() == 2);
    CHECK(Value::alive == 2);
    CHECK(allocations == deallocations);
    values.clear();
    Value::copy_budget = 100;
    values.emplace_back(4);
    values.reserve(7);
    CHECK(values.front().value == 4 && Value::alive == 1);
  }
  CHECK(Value::alive == 0 && allocations == deallocations);
}
}  // namespace

// Substitute only the dispatch boundary, so counts do not depend on the
// selected library allocator or allocations made by element constructors.
namespace dagflow::detail {
void* allocate_bytes(std::size_t bytes, std::size_t alignment) {
  if (fail_allocation) throw std::bad_alloc{};
  void* result = ::operator new(bytes, std::align_val_t{alignment});
  ++allocations;
  last_bytes = bytes;
  last_alignment = alignment;
  return result;
}
void deallocate_bytes(void* pointer, std::size_t alignment) noexcept {
  CHECK(pointer != nullptr);
  ++deallocations;
  ::operator delete(pointer, std::align_val_t{alignment});
}
}  // namespace dagflow::detail

int main() {
  allocation_policy();
  allocation_failure();
  CHECK(allocations == deallocations);
  relocation_failure();
  extended_alignment();
  throwing_move_only();
  CHECK(allocations == deallocations);
}
