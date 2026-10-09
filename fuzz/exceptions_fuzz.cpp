#include <cstddef>
#include <cstdint>
#include <new>
#include <utility>

#include <dagflow/detail/fuzz_points.hpp>
#include <dagflow/detail/small_vector.hpp>
#include <dagflow/inplace_function.hpp>

#include "byte_reader.hpp"
#include "check.hpp"

namespace {
using dagflow::fuzz::check;
struct Injected {};

struct Fault {
  static inline int remaining = -1;
  static void hit() {
    if (remaining == 0) throw Injected{};
    if (remaining > 0) --remaining;
  }
};

template<bool Copyable>
struct alignas(128) Value {
  static inline int live = 0;
  int value;
  explicit Value(int v) : value(v) { ++live; }
  Value(const Value& other) requires Copyable : value(other.value) {
    Fault::hit();
    ++live;
  }
  Value(const Value&) requires (!Copyable) = delete;
  Value(Value&& other) : value(other.value) {
    Fault::hit();
    other.value = -1;
    ++live;
  }
  ~Value() { --live; }
};

template<bool Copyable>
void vector_failure(dagflow::fuzz::Bytes& bytes) {
  using V = Value<Copyable>;
  using Vector = dagflow::small_vector<V, 4>;
  const unsigned mode = bytes.bound(4); // reserve, append, move ctor, move assign
  const unsigned count = mode < 2 ? (bytes.bit() ? 4 : 8) : 1 + bytes.bound(4);
  const int budget = static_cast<int>(bytes.bound(count + 2));
  Fault::remaining = -1;
  {
    Vector source;
    for (unsigned i = 0; i < count; ++i) source.emplace_back(10 + i);
    const auto capacity = source.capacity();
    bool failed = false;
    Fault::remaining = budget;
    try {
      if (mode == 0) source.reserve(capacity + 1);
      else if (mode == 1) {
        if constexpr (Copyable) source.push_back(source.front());
        else source.emplace_back(99);
      } else if (mode == 2) {
        Vector destination(std::move(source));
        check(destination.size() == count && source.empty());
        for (unsigned i = 0; i < count; ++i) check(destination[i].value == int(10 + i));
      } else {
        Vector destination;
        destination.emplace_back(200);
        destination = std::move(source);
        check(destination.size() == count && source.empty());
        for (unsigned i = 0; i < count; ++i) check(destination[i].value == int(10 + i));
      }
    } catch (const Injected&) { failed = true; }
    const unsigned constructions = count + unsigned(mode == 1 && Copyable);
    check(failed == (budget < static_cast<int>(constructions)));
    Fault::remaining = -1;
    check(V::live == static_cast<int>(source.size()));
    check(reinterpret_cast<std::uintptr_t>(source.data()) % alignof(V) == 0);
    if (failed) {
      check(source.size() == count && source.capacity() == capacity);
      for (unsigned i = 0; i < count; ++i) {
        // Copy relocation has the strong guarantee. Throwing move-only
        // relocation and inline moves retain a valid, partially moved source.
        const bool moved = (mode >= 2 || !Copyable) && i < unsigned(budget);
        check(source[i].value == (moved ? -1 : int(10 + i)));
      }
    } else if (mode < 2) {
      check(source.size() == count + unsigned(mode == 1));
      for (unsigned i = 0; i < count; ++i) check(source[i].value == int(10 + i));
      if (mode == 1) check(source.back().value == (Copyable ? 10 : 99));
    }
    // Recovery after both successful and failed operations, including heap
    // steal, moved-from reuse and an impossible capacity without allocating it.
    source.clear();
    source.emplace_back(42);
    source.reserve(16);
    Vector recovered(std::move(source));
    check(source.empty() && recovered.front().value == 42);
    source.emplace_back(7);
    bool too_large = false;
    try { source.reserve(Vector::max_size() + 1); }
    catch (const std::length_error&) { too_large = true; }
    check(too_large && source.size() == 1 && source.front().value == 7);
  }
  check(V::live == 0);
}

template<bool NothrowMove>
struct Callable {
  static inline int live = 0;
  int value;
  explicit Callable(int v) : value(v) { ++live; }
  Callable(const Callable& other) : value(other.value) { Fault::hit(); ++live; }
  Callable(Callable&& other) noexcept(NothrowMove) : value(other.value) {
    if constexpr (!NothrowMove) Fault::hit();
    ++live;
  }
  ~Callable() { --live; }
  int operator()() { return value; }
};

template<class Function, bool NothrowMove>
void callable_failure(dagflow::fuzz::Bytes& bytes) {
  using C = Callable<NothrowMove>;
  const bool fail = bytes.bit();
  const bool allocation = !NothrowMove && bytes.bit();
  const bool move_target = !NothrowMove && bytes.bit();
  const int value = bytes.next();
  Fault::remaining = -1;
  {
    Function function([] { return -7; });
    C replacement(value);
    Fault::remaining = fail && !allocation ? 0 : -1;
    dagflow::detail::fuzz_points::allocation_budget = fail && allocation ? 0 : -1;
    bool failed = false;
    try {
      if (move_target) function.emplace(std::move(replacement));
      else function.emplace(replacement);
    } catch (const Injected&) { check(!allocation); failed = true; }
    catch (const std::bad_alloc&) { check(allocation); failed = true; }
    Fault::remaining = -1;
    dagflow::detail::fuzz_points::allocation_budget = -1;
    check(failed == fail);
    check(function() == (fail ? -7 : value));
    check(C::live == (fail ? 1 : 2));
    function.emplace(replacement);
    // Wrapper moves must not invoke a potentially throwing target move.
    Fault::remaining = 0;
    Function moved(std::move(function));
    function = std::move(moved);
    check(!moved && function() == value);
    function.reset();
    function.reset();
    check(C::live == 1);
    Fault::remaining = -1;
  }
  check(C::live == 0);
}
} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  if (size > 4096) return 0;
  dagflow::fuzz::Bytes bytes(data, size);
  switch (bytes.bound(5)) {
    case 0: vector_failure<true>(bytes); break;
    case 1: vector_failure<false>(bytes); break;
    case 2: callable_failure<dagflow::small_function<int()>, false>(bytes); break;
    case 3: callable_failure<dagflow::small_function<int()>, true>(bytes); break;
    default: callable_failure<dagflow::inplace_function<int()>, true>(bytes); break;
  }
  return 0;
}
