#include <algorithm>
#include <array>
#include <cstdint>
#include <dagflow/detail/runtime_memory.hpp>
#include <dagflow/detail/small_vector.hpp>
#include <dagflow/inplace_function.hpp>
#include <limits>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include "property_support.hpp"

namespace {
using dagflow::test::property::Command;
using dagflow::test::property::Random;
using dagflow::test::property::require;
using dagflow::test::property::run;

struct Ledger {
  int alive = 0;
  int copy_budget = -1;
};

struct Element {
  Ledger* ledger;
  int value;
  Element(Ledger* state, int input, bool fail = false)
      : ledger(state), value(input) {
    if (fail) throw std::runtime_error("element construction");
    ++ledger->alive;
  }
  Element(const Element& other) : ledger(other.ledger), value(other.value) {
    if (ledger->copy_budget == 0) throw std::runtime_error("element copy");
    if (ledger->copy_budget > 0) --ledger->copy_budget;
    ++ledger->alive;
  }
  Element(Element&& other) noexcept(false)
      : ledger(other.ledger), value(other.value) {
    ++ledger->alive;
    other.value = -1;
  }
  ~Element() { --ledger->alive; }
};

// The model records values and the documented capacity policy, never pointers
// or the implementation's storage representation.
struct VectorMachine {
  enum Op {
    Append,
    AliasAppend,
    Pop,
    Clear,
    Reserve,
    Move,
    FailAppend,
    FailReserve,
    Bounds,
    InlineBoundary,
    RoundTrip,
    Count
  };
  struct Model {
    std::vector<int> values;
    std::size_t capacity = 4;
  };
  Ledger ledger;
  std::array<dagflow::small_vector<Element, 4>, 2> vectors;
  std::array<Model, 2> model;

  Command next(Random& random) const {
    Command c{static_cast<unsigned>(random.bounded(Count)), random.bounded(2),
              random.bounded(32)};
    if (!precondition(c)) c.op = model[c.a].values.size() < 64 ? Append : Clear;
    return c;
  }
  bool precondition(const Command& c) const {
    if (c.op >= Count || c.a >= 2 || c.b >= 32) return false;
    const auto size = model[c.a].values.size();
    if (c.op == Pop || c.op == AliasAppend || c.op == FailReserve)
      return size != 0 && (c.op != AliasAppend || size < 64);
    return c.op != Append || size < 64;
  }
  void apply(const Command& c) {
    auto& actual = vectors[c.a];
    auto& expected = model[c.a];
    const auto grow = [&] {
      if (expected.values.size() == expected.capacity) expected.capacity *= 2;
    };
    switch (c.op) {
      case Append:
        actual.emplace_back(&ledger, static_cast<int>(c.b));
        grow();
        expected.values.push_back(static_cast<int>(c.b));
        break;
      case AliasAppend: {
        const auto index = c.b % expected.values.size();
        const auto value = expected.values[index];
        actual.push_back(actual[index]);
        grow();
        expected.values.push_back(value);
        break;
      }
      case Pop:
        actual.pop_back();
        expected.values.pop_back();
        break;
      case Clear:
        actual.clear();
        expected.values.clear();
        break;
      case Reserve:
        actual.reserve(c.b);
        if (c.b > expected.capacity) expected.capacity = c.b;
        break;
      case Move: {
        const auto destination = c.b % 2;
        vectors[destination] = std::move(actual);
        if (destination != c.a) {
          model[destination] = expected;
          expected = Model{};
        }
        break;
      }
      case InlineBoundary:
        actual = decltype(vectors)::value_type{};
        expected = Model{};
        for (int i = 0; i != 4; ++i) {
          actual.emplace_back(&ledger, static_cast<int>(c.b) + i);
          expected.values.push_back(static_cast<int>(c.b) + i);
        }
        // The argument aliases the inline storage being relocated.
        actual.push_back(actual.front());
        expected.values.push_back(static_cast<int>(c.b));
        expected.capacity = 8;
        invariant();
        // Grow again using constructor arguments rather than an aliased
        // Element. This exercises emplace_back<Ledger*, int> on a full heap
        // buffer, preserving the preceding alias copy and every live object.
        for (int offset = 5; offset != 9; ++offset) {
          actual.emplace_back(&ledger, static_cast<int>(c.b) + offset);
          grow();
          expected.values.push_back(static_cast<int>(c.b) + offset);
          invariant();
        }
        break;
      case RoundTrip: {
        auto temporary(std::move(actual));
        require(actual.empty() && actual.capacity() == 4,
                "vector move construction resets source storage");
        actual = std::move(temporary);
        require(temporary.empty(), "vector move assignment empties source");
        break;
      }
      case FailAppend: {
        bool threw = false;
        try {
          actual.emplace_back(&ledger, 0, true);
        } catch (const std::runtime_error&) {
          threw = true;
        }
        require(threw, "throwing element construction must propagate");
        break;
      }
      case FailReserve: {
        ledger.copy_budget = static_cast<int>(c.b % expected.values.size());
        bool threw = false;
        try {
          actual.reserve(expected.capacity + 1);
        } catch (const std::runtime_error&) {
          threw = true;
        }
        ledger.copy_budget = -1;
        require(threw, "relocation must exercise copy rollback");
        break;
      }
      case Bounds: {
        bool threw = false;
        try {
          (void)actual.at(expected.values.size() + c.b);
        } catch (const std::out_of_range&) {
          threw = true;
        }
        require(threw, "at rejects indices outside the model");
        break;
      }
    }
  }
  void invariant() const {
    std::size_t live = 0;
    for (std::size_t slot = 0; slot < vectors.size(); ++slot) {
      const auto& actual = vectors[slot];
      const auto& expected = model[slot];
      live += expected.values.size();
      require(actual.size() == expected.values.size(), "vector size");
      require(actual.capacity() == expected.capacity, "vector capacity policy");
      require(actual.empty() == expected.values.empty(), "vector emptiness");
      for (std::size_t i = 0; i < expected.values.size(); ++i) {
        require(actual[i].value == expected.values[i], "vector sequence");
        require(actual.at(i).value == expected.values[i], "checked access");
        require(actual.data() + i == &actual[i], "contiguous live elements");
      }
    }
    require(ledger.alive == static_cast<int>(live), "vector lifetime balance");
  }
};

template <std::size_t Padding, std::size_t Alignment = alignof(void*)>
struct alignas(Alignment) Target {
  Ledger* ledger;
  int value;
  bool fail_copy = false;
  std::array<unsigned char, Padding> padding{};
  Target(Ledger* state, int initial) : ledger(state), value(initial) {
    ++ledger->alive;
  }
  Target(Target&& other) noexcept
      : ledger(other.ledger), value(other.value), fail_copy(other.fail_copy) {
    ++ledger->alive;
  }
  Target(const Target& other)
      : ledger(other.ledger), value(other.value), fail_copy(other.fail_copy) {
    if (fail_copy) throw std::runtime_error("target copy");
    ++ledger->alive;
  }
  ~Target() { --ledger->alive; }
  int operator()(int delta) { return value += delta; }
};

template <bool Heap>
struct FunctionMachine {
  using Function =
      std::conditional_t<Heap, dagflow::small_function<int(int), 32>,
                         dagflow::inplace_function<int(int), 32>>;
  enum Op { Assign, Reset, Move, Invoke, FailAssign, RoundTrip, Count };
  Ledger ledger;
  std::array<Function, 3> functions;
  std::array<std::optional<int>, 3> model;
  Command next(Random& random) const {
    Command c{static_cast<unsigned>(random.bounded(Count)), random.bounded(3),
              random.bounded(30)};
    if (!precondition(c)) c.op = Assign;
    return c;
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 3 && c.b < 30 &&
           (c.op != Invoke || model[c.a].has_value());
  }
  void apply(const Command& c) {
    auto& function = functions[c.a];
    switch (c.op) {
      case Assign:
        if constexpr (Heap) {
          if (c.b % 3 == 0)
            function.emplace(Target<128>(&ledger, static_cast<int>(c.b)));
          else if (c.b % 3 == 1)
            function.emplace(Target<1, 64>(&ledger, static_cast<int>(c.b)));
          else
            function.emplace(Target<1>(&ledger, static_cast<int>(c.b)));
        } else {
          function.emplace(Target<1>(&ledger, static_cast<int>(c.b)));
        }
        model[c.a] = static_cast<int>(c.b);
        break;
      case Reset:
        function.reset();
        model[c.a].reset();
        break;
      case Move: {
        const auto destination = c.b % functions.size();
        functions[destination] = std::move(function);
        if (destination != c.a) {
          model[destination] = model[c.a];
          model[c.a].reset();
        }
        break;
      }
      case Invoke:
        *model[c.a] += static_cast<int>(c.b);
        require(function(static_cast<int>(c.b)) == *model[c.a],
                "callable state follows invocation and moves");
        break;
      case FailAssign: {
        bool threw = false;
        // Heap targets also cover allocate-before-constructor rollback.
        using Throwing = Target<Heap ? 128 : 1>;
        Throwing replacement(&ledger, 999);
        replacement.fail_copy = true;
        try {
          function.emplace(replacement);
        } catch (const std::runtime_error&) {
          threw = true;
        }
        require(threw, "failed emplace propagates construction error");
        if (model[c.a])
          require(function(0) == *model[c.a], "failed emplace retains target");
        break;
      }
      case RoundTrip: {
        Function temporary(std::move(function));
        require(!function, "move construction empties source");
        function = std::move(temporary);
        require(!temporary, "move assignment empties source");
        break;
      }
    }
  }
  void invariant() const {
    int live = 0;
    for (std::size_t i = 0; i < functions.size(); ++i) {
      require(static_cast<bool>(functions[i]) == model[i].has_value(),
              "callable occupancy");
      live += model[i].has_value();
    }
    require(ledger.alive == live, "exactly one target per occupied wrapper");
  }
};

struct alignas(128) OwnedElement {
  static inline Ledger* constructing = nullptr;
  Ledger* ledger;
  int value;
  OwnedElement() : OwnedElement(constructing, 0) {}
  OwnedElement(Ledger* state, int input) : ledger(state), value(input) {
    if (ledger->copy_budget == 0)
      throw std::runtime_error("owned construction");
    if (ledger->copy_budget > 0) --ledger->copy_budget;
    ++ledger->alive;
  }
  ~OwnedElement() { --ledger->alive; }
};

struct MemoryMachine {
  enum Op {
    Create,
    Move,
    Reset,
    Release,
    Adopt,
    RemoteDestroy,
    ArrayCreate,
    ArrayMove,
    ArrayReset,
    FailObject,
    FailArray,
    Count
  };
  Ledger ledger;
  std::array<dagflow::detail::OwnedObject<OwnedElement>, 3> objects;
  std::array<OwnedElement*, 3> released{};
  std::array<dagflow::detail::OwnedArray<OwnedElement>, 3> arrays;
  struct Model {
    std::optional<int> owned;
    std::optional<int> released;
    std::size_t array_size = 0;
  };
  std::array<Model, 3> model;
  ~MemoryMachine() {
    for (auto* pointer : released)
      dagflow::detail::ObjectDeleter<OwnedElement>{}(pointer);
  }
  Command next(Random& random) const {
    Command c{static_cast<unsigned>(random.bounded(Count)), random.bounded(3),
              random.bounded(16)};
    if (!precondition(c)) c.op = Create;
    return c;
  }
  bool precondition(const Command& c) const {
    if (c.op >= Count || c.a >= 3 || c.b >= 16) return false;
    if (c.op == Release) return !model[c.a].released.has_value();
    return true;
  }
  void apply(const Command& c) {
    auto& expected = model[c.a];
    auto& object = objects[c.a];
    const auto destination = (c.a + 1 + c.b % 2) % 3;
    switch (c.op) {
      case Create:
        object = dagflow::detail::make_owned<OwnedElement>(
            &ledger, static_cast<int>(c.b));
        expected.owned = static_cast<int>(c.b);
        break;
      case Move:
        objects[destination] = std::move(object);
        model[destination].owned = std::exchange(expected.owned, std::nullopt);
        break;
      case Reset:
        object.reset();
        expected.owned.reset();
        break;
      case Release:
        released[c.a] = object.release();
        expected.released = std::exchange(expected.owned, std::nullopt);
        break;
      case Adopt:
        object.reset(std::exchange(released[c.a], nullptr));
        expected.owned = std::exchange(expected.released, std::nullopt);
        break;
      case RemoteDestroy: {
        std::thread destroyer([owner = std::move(object)] {});
        destroyer.join();
        expected.owned.reset();
        break;
      }
      case ArrayCreate:
        OwnedElement::constructing = &ledger;
        arrays[c.a] = dagflow::detail::make_owned_array<OwnedElement>(c.b);
        expected.array_size = c.b;
        break;
      case ArrayMove:
        arrays[destination] = std::move(arrays[c.a]);
        model[destination].array_size = std::exchange(expected.array_size, 0);
        break;
      case ArrayReset:
        arrays[c.a].reset();
        expected.array_size = 0;
        break;
      case FailObject:
      case FailArray: {
        ledger.copy_budget = c.op == FailObject ? 0 : static_cast<int>(c.b);
        OwnedElement::constructing = &ledger;
        bool threw = false;
        try {
          if (c.op == FailObject)
            object = dagflow::detail::make_owned<OwnedElement>(&ledger, 0);
          else
            arrays[c.a] =
                dagflow::detail::make_owned_array<OwnedElement>(c.b + 1);
        } catch (const std::runtime_error&) {
          threw = true;
        }
        ledger.copy_budget = -1;
        require(threw, "owned construction failure propagates");
        break;
      }
    }
  }
  void invariant() const {
    std::size_t live = 0;
    for (std::size_t i = 0; i < model.size(); ++i) {
      const auto& expected = model[i];
      live += expected.owned.has_value() + expected.released.has_value() +
              expected.array_size;
      require(bool(objects[i]) == expected.owned.has_value(), "object owner");
      require(bool(released[i]) == expected.released.has_value(),
              "raw custody");
      require(bool(arrays[i]) == (expected.array_size != 0), "array owner");
      if (objects[i])
        require(objects[i]->value == *expected.owned, "owned value");
      if (released[i])
        require(released[i]->value == *expected.released, "released value");
      for (std::size_t j = 0; j < expected.array_size; ++j)
        require(arrays[i][j].value == 0, "array initialization");
      for (const auto* pointer :
           {objects[i].get(), released[i], arrays[i].get()})
        require(
            reinterpret_cast<std::uintptr_t>(pointer) % alignof(OwnedElement) ==
                0,
            "over-aligned runtime storage");
    }
    require(ledger.alive == static_cast<int>(live), "owned lifetime balance");
  }
};

struct AllocatorMachine {
  struct alignas(128) Value {
    int value = 0;
  };
  struct BlockDeleter {
    std::size_t alignment;
    explicit BlockDeleter(std::size_t value = 1) : alignment(value) {}
    void operator()(std::byte* pointer) const noexcept {
      dagflow::detail::deallocate_bytes(pointer, alignment);
    }
  };
  using Block = std::unique_ptr<std::byte, BlockDeleter>;
  using Vector = std::vector<Value, dagflow::detail::RuntimeAllocator<Value>>;
  enum Op {
    Append,
    Clear,
    Move,
    Swap,
    Reserve,
    Allocate,
    Write,
    Free,
    RemoteFree,
    Oversize,
    Count
  };
  std::array<Vector, 3> vectors;
  std::array<std::vector<int>, 3> values;
  std::array<Block, 3> blocks{Block{nullptr, BlockDeleter{}},
                              Block{nullptr, BlockDeleter{}},
                              Block{nullptr, BlockDeleter{}}};
  struct BlockModel {
    bool allocated = false;
    std::size_t alignment = 1;
    std::vector<std::byte> bytes;
  };
  std::array<BlockModel, 3> memory;

  Command next(Random& random) const {
    Command c{static_cast<unsigned>(random.bounded(Count)), random.bounded(3),
              random.bounded(32)};
    if (!precondition(c)) c.op = Allocate;
    return c;
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 3 && c.b < 32 &&
           (c.op != Write || memory[c.a].allocated) &&
           (c.op != Append || values[c.a].size() < 64);
  }
  void apply(const Command& c) {
    const auto destination = (c.a + 1 + c.b % 2) % 3;
    switch (c.op) {
      case Append:
        vectors[c.a].push_back(Value{static_cast<int>(c.b)});
        values[c.a].push_back(static_cast<int>(c.b));
        break;
      case Clear:
        vectors[c.a].clear();
        values[c.a].clear();
        break;
      case Move:
        vectors[destination] = std::move(vectors[c.a]);
        values[destination] = values[c.a];
        // Explicit clear uses only the standard valid-but-unspecified source
        // guarantee; the model does not assume an unspecified moved-from size.
        vectors[c.a].clear();
        values[c.a].clear();
        break;
      case Swap:
        vectors[destination].swap(vectors[c.a]);
        values[destination].swap(values[c.a]);
        break;
      case Reserve:
        vectors[c.a].reserve(c.b);
        require(vectors[c.a].capacity() >= c.b, "allocator vector reserve");
        break;
      case Allocate: {
        const std::size_t alignment = std::size_t{1} << (c.b % 9);
        Block replacement(static_cast<std::byte*>(
                              dagflow::detail::allocate_bytes(c.b, alignment)),
                          BlockDeleter{alignment});
        require(bool(replacement), "zero-sized allocation still owns storage");
        blocks[c.a] = std::move(replacement);
        memory[c.a] = {true, alignment, std::vector<std::byte>(c.b)};
        std::fill_n(blocks[c.a].get(), c.b, std::byte{0});
        break;
      }
      case Write:
        std::fill_n(blocks[c.a].get(), memory[c.a].bytes.size(),
                    std::byte(c.b));
        std::fill(memory[c.a].bytes.begin(), memory[c.a].bytes.end(),
                  std::byte(c.b));
        break;
      case Free:
        blocks[c.a].reset();
        memory[c.a] = {};
        break;
      case RemoteFree: {
        std::thread destroyer([block = std::move(blocks[c.a])] {});
        destroyer.join();
        memory[c.a] = {};
        break;
      }
      case Oversize: {
        dagflow::detail::RuntimeAllocator<Value> allocator;
        bool threw = false;
        try {
          auto* pointer = allocator.allocate(
              std::numeric_limits<std::size_t>::max() / sizeof(Value) + 1);
          allocator.deallocate(pointer, 0);
        } catch (const std::bad_array_new_length&) {
          threw = true;
        }
        require(threw, "allocator rejects overflowing byte count");
        break;
      }
    }
  }
  void invariant() const {
    for (std::size_t i = 0; i < values.size(); ++i) {
      require(vectors[i].size() == values[i].size(), "allocator vector size");
      require(reinterpret_cast<std::uintptr_t>(vectors[i].data()) %
                      alignof(Value) ==
                  0,
              "allocator preserves element alignment");
      for (std::size_t j = 0; j < values[i].size(); ++j)
        require(vectors[i][j].value == values[i][j], "allocator vector values");
      require(bool(blocks[i]) == memory[i].allocated, "raw allocation custody");
      if (!memory[i].allocated) continue;
      require(reinterpret_cast<std::uintptr_t>(blocks[i].get()) %
                      memory[i].alignment ==
                  0,
              "raw allocation alignment");
      for (std::size_t j = 0; j < memory[i].bytes.size(); ++j)
        require(blocks[i].get()[j] == memory[i].bytes[j], "raw bytes retained");
    }
    require(dagflow::detail::RuntimeAllocator<Value>{} ==
                dagflow::detail::RuntimeAllocator<int>{},
            "runtime allocator instances share a deallocation domain");
  }
};
}  // namespace

int main() {
  run<VectorMachine>("small_vector values, storage transitions and rollback");
  run<FunctionMachine<true>>("small_function ownership and strong emplace");
  run<FunctionMachine<false>>("inplace_function ownership and strong emplace");
  run<MemoryMachine>("runtime object and array custody transitions");
  run<AllocatorMachine>("runtime allocator containers and raw storage");
}
