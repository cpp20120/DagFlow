#include <cstdint>
#include <memory>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "dagflow/inplace_function.hpp"
#include "dagflow/detail/small_function.hpp"
#include "support.hpp"

namespace {
std::size_t allocations = 0, deallocations = 0, last_bytes = 0,
            last_alignment = 0;
bool fail_allocation = false;
#ifdef DAGFLOW_FUNCTION_TEST_BACKEND
constexpr bool recording = true;
#else
constexpr bool recording = false;
#endif

template <std::size_t N>
struct Sized {
  unsigned char bytes[N]{};
  void operator()(int& result) { result += bytes[0]; }
};
static_assert(sizeof(Sized<64>) == 64 && sizeof(Sized<65>) == 65);

struct alignas(128) Aligned {
  void operator()(int& result) {
    CHECK(reinterpret_cast<std::uintptr_t>(this) % alignof(Aligned) == 0);
    ++result;
  }
};

struct ThrowingMove {
  static inline int alive = 0, moves = 0;
  static inline bool fail_move = false;
  ThrowingMove() { ++alive; }
  ThrowingMove(const ThrowingMove&) { ++alive; }
  ThrowingMove(ThrowingMove&&) noexcept(false) {
    ++moves;
    if (fail_move) throw std::runtime_error("move");
    ++alive;
  }
  ~ThrowingMove() { --alive; }
  const void* operator()() { return this; }
};

template <std::size_t N>
struct ThrowingCopy {
  unsigned char bytes[N]{};
  static inline bool fail_copy = false;
  ThrowingCopy() = default;
  ThrowingCopy(const ThrowingCopy&) {
    if (fail_copy) throw std::runtime_error("copy");
  }
  ThrowingCopy(ThrowingCopy&&) noexcept = default;
  void operator()(int& result) { ++result; }
};

struct Tracked {
  static inline int alive = 0, destroyed = 0;
  int value;
  explicit Tracked(int n) : value(n) { ++alive; }
  Tracked(const Tracked&) = delete;
  Tracked(Tracked&& other) noexcept : value(other.value) { ++alive; }
  ~Tracked() {
    --alive;
    ++destroyed;
  }
  void operator()(int& result) { result += value; }
};

using Small = dagflow::small_function<void(int&)>;
using Inplace = dagflow::inplace_function<void(int&)>;
static_assert(!std::is_copy_constructible_v<Small>);
static_assert(!std::is_copy_assignable_v<Small>);
static_assert(!std::is_copy_constructible_v<Inplace>);
static_assert(!std::is_copy_assignable_v<Inplace>);
static_assert(std::is_nothrow_move_constructible_v<Small>);
static_assert(std::is_nothrow_move_assignable_v<Small>);
static_assert(std::is_nothrow_move_constructible_v<Inplace>);
static_assert(std::is_nothrow_move_assignable_v<Inplace>);
static_assert(std::is_constructible_v<Inplace, Sized<64>>);
static_assert(!std::is_constructible_v<Inplace, Sized<65>>);
static_assert(!std::is_constructible_v<Inplace, Aligned>);
static_assert(!std::is_constructible_v<dagflow::inplace_function<const void*()>,
                                       ThrowingMove>);
static_assert(std::is_constructible_v<Small, Sized<65>>);
static_assert(std::is_constructible_v<Small, Aligned>);
static_assert(std::is_constructible_v<dagflow::small_function<const void*()>,
                                      ThrowingMove>);
static_assert(!std::is_constructible_v<Small, int>);

template <class Function>
void inline_lifetime() {
  const auto before = allocations;
  {
    Function empty{}, null(nullptr);
    CHECK(!empty && !null);
    empty.reset();
    auto moved_empty = std::move(empty);
    CHECK(!empty && !moved_empty);
    Function function(Tracked{3});
    CHECK(Tracked::alive == 1);
    int result = 0;
    function(result);
    auto moved = std::move(function);
    CHECK(!function && moved && Tracked::alive == 1);
    moved(result);
    auto& self = moved;
    const auto destroyed = Tracked::destroyed;
    moved = std::move(self);
    CHECK(moved && Tracked::destroyed == destroyed);
    function.emplace(Tracked{9});
    CHECK(Tracked::alive == 2);
    function = std::move(moved);
    CHECK(!moved && Tracked::alive == 1);
    function(result);
    CHECK(result == 9);
    function.reset();
    function.reset();
    CHECK(!function && Tracked::alive == 0);
    function.emplace(
        [value = std::make_unique<int>(7)](int& n) { n += *value; });
    function(result);
    CHECK(result == 16);
    function = std::move(empty);
    CHECK(!function);
  }
  CHECK(Tracked::alive == 0 && allocations == before);
}

void storage_boundaries() {
  int result = 0;
  const auto before = allocations;
  {
    auto exact_lambda = [payload = Sized<64>{{2}}](int& n) mutable {
      payload(n);
    };
    static_assert(sizeof(exact_lambda) == 64);
    Small exact(std::move(exact_lambda));
    exact(result);
    CHECK(allocations == before);
    Small oversized(Sized<65>{{3}});
    oversized(result);
    if (recording) {
      CHECK(allocations == before + 1);
      CHECK(last_bytes == sizeof(Sized<65>) &&
            last_alignment == alignof(Sized<65>));
    }
    const auto after_spill = allocations;
    auto moved = std::move(oversized);
    CHECK(!oversized && allocations == after_spill);
    auto& self = moved;
    moved = std::move(self);
    moved(result);
    CHECK(result == 8);
    exact = std::move(moved);  // inline -> heap
    exact(result);
    CHECK(result == 11 && !moved);
    exact.emplace(Sized<64>{{4}});  // heap -> inline
    exact(result);
    CHECK(result == 15 && allocations == after_spill);
    exact.emplace(Sized<65>{{5}});  // inline -> heap
    Small other(Sized<65>{{6}});
    exact = std::move(other);  // heap -> heap, retire previous target
    exact(result);
    CHECK(result == 21 && !other);
  }
  CHECK(allocations == deallocations);

  {
    const auto before_aligned = allocations;
    dagflow::small_function<void(int&), 256> spilled(Aligned{});
    spilled(result);
    if (recording) {
      CHECK(allocations == before_aligned + 1);
      CHECK(last_bytes == sizeof(Aligned) &&
            last_alignment == alignof(Aligned));
    }
    const auto after_aligned = allocations;
    dagflow::small_function<void(int&), 256, 128> inline_aligned(Aligned{});
    dagflow::inplace_function<void(int&), 256, 128> strict_aligned(Aligned{});
    inline_aligned(result);
    strict_aligned(result);
    CHECK(allocations == after_aligned);
  }
  CHECK(allocations == deallocations);
}

void throwing_move_pointer_steal() {
  {
    ThrowingMove original;
    ThrowingMove::fail_move = true;
    dagflow::small_function<const void*()> function(
        original);  // Copy into heap.
    const void* address = function();
    const auto before = allocations;
    auto moved = std::move(function);
    CHECK(!function && moved() == address);
    function = std::move(moved);
    CHECK(!moved && function() == address);
    CHECK(ThrowingMove::moves == 0 && ThrowingMove::alive == 2);
    CHECK(allocations == before);
    bool caught = false;
    try {
      function.emplace(std::move(original));
    } catch (const std::runtime_error&) {
      caught = true;
    }
    CHECK(caught && function() == address && ThrowingMove::alive == 2);
    ThrowingMove::fail_move = false;
  }
  CHECK(ThrowingMove::alive == 0 && allocations == deallocations);
}

template <std::size_t N, class Function>
void constructor_failure() {
  ThrowingCopy<N> callable;
  ThrowingCopy<N>::fail_copy = true;
  const auto outstanding = allocations - deallocations;
  bool caught = false;
  try {
    Function failed(callable);
  } catch (const std::runtime_error&) {
    caught = true;
  }
  CHECK(caught && allocations - deallocations == outstanding);
  Function function([](int& n) { n += 42; });
  caught = false;
  try {
    function.emplace(callable);
  } catch (const std::runtime_error&) {
    caught = true;
  }
  int value = 0;
  function(value);
  CHECK(caught && value == 42 && allocations - deallocations == outstanding);
  ThrowingCopy<N>::fail_copy = false;
  function.emplace(callable);
  function(value);
  CHECK(value == 43);
}

void signatures() {
  struct Object {
    int value = 3;
    int add(int n) { return value += n; }
  } object;
  dagflow::small_function<int(Object&, int)> method(&Object::add);
  CHECK(method(object, 4) == 7);
  dagflow::inplace_function<int&(Object&)> member(&Object::value);
  member(object) = 9;
  CHECK(object.value == 9);
  dagflow::small_function<int(std::unique_ptr<int>)> consume(
      [](std::unique_ptr<int> n) { return *n; });
  CHECK(consume(std::make_unique<int>(12)) == 12);
  Small discard([](int& n) { return ++n; });
  discard(object.value);
  CHECK(object.value == 10);
  dagflow::inplace_function<void(), 1, 1> tiny([] {});
  tiny();
  struct CopyOnly {
    CopyOnly() = default;
    CopyOnly(const CopyOnly&) = default;
    CopyOnly(CopyOnly&&) = delete;
    const void* operator()() { return this; }
  } original;
  dagflow::small_function<const void*()> copied(original);
  const void* address = copied();
  auto moved = std::move(copied);
  CHECK(!copied && moved() == address);
}

void allocation_failure() {
  if (!recording) return;
  Small function(Sized<65>{{7}});
  const auto outstanding = allocations - deallocations;
  fail_allocation = true;
  bool caught = false;
  try {
    function.emplace(Sized<65>{});
  } catch (const std::bad_alloc&) {
    caught = true;
  }
  fail_allocation = false;
  int result = 0;
  function(result);
  CHECK(caught && result == 7 && allocations - deallocations == outstanding);
}
}  // namespace

#ifdef DAGFLOW_FUNCTION_TEST_BACKEND
namespace dagflow::detail {
void* allocate_bytes(std::size_t bytes, std::size_t alignment) {
  if (fail_allocation) throw std::bad_alloc{};
  void* memory = alignment > __STDCPP_DEFAULT_NEW_ALIGNMENT__
                     ? ::operator new(bytes, std::align_val_t{alignment})
                     : ::operator new(bytes);
  ++allocations;
  last_bytes = bytes;
  last_alignment = alignment;
  return memory;
}
void deallocate_bytes(void* memory, std::size_t alignment) noexcept {
  CHECK(memory != nullptr);
  ++deallocations;
  if (alignment > __STDCPP_DEFAULT_NEW_ALIGNMENT__)
    ::operator delete(memory, std::align_val_t{alignment});
  else
    ::operator delete(memory);
}
}  // namespace dagflow::detail
#endif

int main() {
  inline_lifetime<Small>();
  inline_lifetime<Inplace>();
  storage_boundaries();
  throwing_move_pointer_steal();
  constructor_failure<8, Small>();
  constructor_failure<65, Small>();
  constructor_failure<8, Inplace>();
  signatures();
  allocation_failure();
  CHECK(allocations == deallocations);
}
