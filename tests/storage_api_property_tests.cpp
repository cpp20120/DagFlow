#include <array>
#include <cstddef>
#include <cstdint>
#include <dagflow/detail/runtime_memory.hpp>
#include <dagflow/detail/small_vector.hpp>
#include <dagflow/inplace_function.hpp>
#include <memory>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

#include "adversarial_allocation.hpp"
#include "property_support.hpp"

namespace {
using namespace dagflow::test::property;
namespace allocation = adversarial::allocation;

struct ResetAllocationFault {
  ~ResetAllocationFault() { allocation::fault = {}; }
};
struct MoveError {};
struct ConstructorError {};
struct InvocationError {};

template <class Error, class Function>
bool throws(Function&& function) {
  try {
    std::forward<Function>(function)();
  } catch (const Error&) {
    return true;
  }
  return false;
}

struct MoveLedger {
  int live = 0;
  int remaining = -1;
  unsigned moves = 0;
};

// A move-only user type with a documented moved-from value. Throwing occurs
// before changing the source; earlier successful moves may have changed it.
struct MoveOnly {
  MoveLedger* ledger;
  int value;
  MoveOnly(MoveLedger* state, int input, bool fail = false)
      : ledger(state), value(input) {
    if (fail) throw ConstructorError{};
    ++ledger->live;
  }
  MoveOnly(const MoveOnly&) = delete;
  MoveOnly(MoveOnly&& source) noexcept(false)
      : ledger(source.ledger), value(source.value) {
    ++ledger->moves;
    if (ledger->remaining == 0) throw MoveError{};
    if (ledger->remaining > 0) --ledger->remaining;
    source.value = -1;
    ++ledger->live;
  }
  ~MoveOnly() { --ledger->live; }
};

class MoveOnlyVectorModel {
  using Vector = dagflow::small_vector<MoveOnly, 4>;
  enum Op : unsigned {
    Append,
    Pop,
    Clear,
    Reserve,
    MoveAssign,
    SelfMove,
    RoundTrip,
    FailReserve,
    FailGrowth,
    FailMoveConstructor,
    FailMoveAssignment,
    FillInline,
    FillHeap,
    AllocationReserve,
    AllocationGrowth,
    Oversize,
    FailElementConstructor,
    Count
  };
  struct ElementModel {
    int value;
    bool may_be_moved = false;
  };
  struct State {
    std::vector<ElementModel> values;
    std::size_t capacity = 4;
  };
  MoveLedger ledger_;
  std::array<Vector, 2> actual_;
  std::array<State, 2> model_;

  static void permit_moved_from(State& state) {
    // Basic guarantee allows user-visible moved-from elements. Do not copy
    // the implementation's relocation order into the reference model.
    for (auto& value : state.values) value.may_be_moved = true;
  }

 public:
  Command next(Random& random) const {
    Command c{static_cast<unsigned>(random.bounded(Count)), random.bounded(2),
              random.bounded(16)};
    if (!precondition(c)) c.op = c.b % 2 ? FillInline : FillHeap;
    return c;
  }
  bool precondition(const Command& c) const {
    if (c.op >= Count || c.a >= 2 || c.b >= 16) return false;
    const auto& state = model_[c.a];
    if (c.op == Append) return state.values.size() < 32;
    if (c.op == Pop || c.op == FailReserve) return !state.values.empty();
    if (c.op == FailGrowth || c.op == AllocationGrowth)
      return state.values.size() == state.capacity;
    if (c.op == FailMoveConstructor || c.op == FailMoveAssignment)
      return !state.values.empty() && state.capacity == 4;
    return true;
  }
  void apply(const Command& c) {
    ResetAllocationFault reset;
    auto& actual = actual_[c.a];
    auto& model = model_[c.a];
    const auto destination = 1 - c.a;
    switch (c.op) {
      case Append:
        actual.emplace_back(&ledger_, static_cast<int>(c.b));
        if (model.values.size() == model.capacity) model.capacity *= 2;
        model.values.push_back({static_cast<int>(c.b)});
        break;
      case Pop:
        actual.pop_back();
        model.values.pop_back();
        break;
      case Clear:
        actual.clear();
        model.values.clear();
        break;
      case Reserve:
        actual.reserve(c.b);
        if (c.b > model.capacity) model.capacity = c.b;
        break;
      case MoveAssign:
        actual_[destination] = std::move(actual);
        model_[destination] = model;
        model = State{};
        break;
      case SelfMove: {
        const auto moves = ledger_.moves;
        auto& alias = actual;
        actual = std::move(alias);
        require(ledger_.moves == moves, "self move does not move elements");
        break;
      }
      case RoundTrip: {
        Vector temporary(std::move(actual));
        require(actual.empty() && actual.capacity() == 4,
                "successful move construction empties and resets source");
        actual = std::move(temporary);
        require(temporary.empty(), "round-trip transfers sole storage owner");
        break;
      }
      case FailReserve:
      case FailGrowth:
        ledger_.remaining = static_cast<int>(c.b % model.values.size());
        require(throws<MoveError>([&] {
                  if (c.op == FailReserve)
                    actual.reserve(model.capacity + 1);
                  else
                    actual.emplace_back(&ledger_, static_cast<int>(c.b));
                }),
                "relocation propagates a move-only element failure");
        ledger_.remaining = -1;
        permit_moved_from(model);
        break;
      case FailMoveConstructor:
        ledger_.remaining = static_cast<int>(c.b % model.values.size());
        require(throws<MoveError>([&] { Vector rejected(std::move(actual)); }),
                "inline vector move constructor can fail partway");
        ledger_.remaining = -1;
        permit_moved_from(model);
        break;
      case FailMoveAssignment:
        ledger_.remaining = static_cast<int>(c.b % model.values.size());
        require(throws<MoveError>(
                    [&] { actual_[destination] = std::move(actual); }),
                "inline move assignment propagates element failure");
        ledger_.remaining = -1;
        // The previous destination is released before transfer. Its partial
        // replacement is destroyed; the source remains valid and reusable.
        model_[destination] = State{};
        permit_moved_from(model);
        break;
      case FillInline:
      case FillHeap: {
        actual = Vector{};
        model = State{};
        const auto count = c.op == FillHeap ? 8 : c.b % 4 + 1;
        if (c.op == FillHeap) {
          actual.reserve(8);
          model.capacity = 8;
        }
        for (std::size_t i = 0; i < count; ++i) {
          actual.emplace_back(&ledger_, static_cast<int>(i + c.b));
          model.values.push_back({static_cast<int>(i + c.b)});
        }
        break;
      }
      case AllocationReserve:
      case AllocationGrowth:
        allocation::fault.remaining = 0;
        require(throws<std::bad_alloc>([&] {
                  if (c.op == AllocationReserve)
                    actual.reserve(model.capacity + 1);
                  else
                    actual.emplace_back(&ledger_, static_cast<int>(c.b));
                }),
                "failed storage allocation preserves the vector");
        require(allocation::fault.remaining == -1,
                "storage request consumes the one-shot allocation fault");
        break;
      case Oversize:
        allocation::fault.remaining = 0;
        require(throws<std::length_error>(
                    [&] { actual.reserve(Vector::max_size() + 1); }),
                "oversized reserve rejects before allocation");
        require(allocation::fault.remaining == 0,
                "size guard does not call the allocator");
        break;
      case FailElementConstructor:
        require(throws<ConstructorError>(
                    [&] { actual.emplace_back(&ledger_, 0, true); }),
                "element construction failure never changes live sequence");
        break;
    }
  }
  void invariant() const {
    std::size_t live = 0, allocations = 0;
    for (std::size_t slot = 0; slot < actual_.size(); ++slot) {
      const auto& actual = actual_[slot];
      const auto& model = model_[slot];
      live += model.values.size();
      allocations += model.capacity > 4;
      require(actual.size() == model.values.size(), "basic guarantee size");
      require(actual.capacity() == model.capacity, "basic guarantee capacity");
      require(actual.empty() == model.values.empty(), "basic guarantee empty");
      require(actual.begin() == actual.data(), "iterator starts at data");
      require(actual.cbegin() == actual.begin(),
              "const iterator starts at data");
      require(actual.cend() == actual.end(), "const end matches end");
      for (std::size_t i = 0; i < model.values.size(); ++i) {
        const auto& value = model.values[i];
        require(actual[i].value == value.value ||
                    (value.may_be_moved && actual[i].value == -1),
                "element remains original or validly moved from after failure");
      }
      if (!actual.empty()) {
        require(&actual.front() == &actual[0], "front refers to first element");
        require(&actual.back() == &actual[actual.size() - 1],
                "back refers to final element");
      }
    }
    require(ledger_.live == static_cast<int>(live),
            "failed relocation destroys all partially constructed elements");
    require(allocation::live.load() == allocations,
            "failed relocation releases temporary storage");
  }
  void finish() {
    for (std::size_t i = 0; i < actual_.size(); ++i) {
      actual_[i] = Vector{};
      model_[i] = State{};
    }
    invariant();
  }
};

struct CallableLedger {
  int live = 0;
  unsigned moves = 0;
  bool fail_copy = false;
  bool fail_move = false;
  bool fail_invoke = false;
};
struct Observation {
  const void* address;
  int value;
};
struct ThrowingMoveTarget {
  CallableLedger* ledger;
  int value;
  ThrowingMoveTarget(CallableLedger* state, int input)
      : ledger(state), value(input) {
    ++ledger->live;
  }
  ThrowingMoveTarget(const ThrowingMoveTarget& source)
      : ledger(source.ledger), value(source.value) {
    if (ledger->fail_copy) throw ConstructorError{};
    ++ledger->live;
  }
  ThrowingMoveTarget(ThrowingMoveTarget&& source) noexcept(false)
      : ledger(source.ledger), value(source.value) {
    ++ledger->moves;
    if (ledger->fail_move) throw MoveError{};
    ++ledger->live;
    source.value = -1;
  }
  ~ThrowingMoveTarget() { --ledger->live; }
  Observation operator()() {
    if (ledger->fail_invoke) throw InvocationError{};
    return {this, value};
  }
};

class SpilledCallableModel {
  using Function = dagflow::small_function<Observation()>;
  enum Op : unsigned {
    LvalueConstruct,
    LvalueEmplace,
    RvalueEmplace,
    FailedCopy,
    FailedMove,
    FailedAllocation,
    MoveAssign,
    SelfMove,
    RoundTrip,
    Null,
    ThrowInvoke,
    Count
  };
  CallableLedger ledger_;
  std::array<Function, 2> actual_;
  std::array<std::optional<int>, 2> model_;

 public:
  Command next(Random& random) const {
    Command c{static_cast<unsigned>(random.bounded(Count)), random.bounded(2),
              random.bounded(32)};
    if (!precondition(c)) c.op = LvalueConstruct;
    return c;
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 2 && c.b < 32 &&
           (c.op != ThrowInvoke || model_[c.a].has_value());
  }
  void apply(const Command& c) {
    ResetAllocationFault reset;
    auto& actual = actual_[c.a];
    const auto destination = 1 - c.a;
    switch (c.op) {
      case LvalueConstruct:
      case LvalueEmplace:
      case RvalueEmplace: {
        ThrowingMoveTarget source(&ledger_, static_cast<int>(c.b));
        const auto moves = ledger_.moves;
        if (c.op == LvalueConstruct) {
          Function replacement(source);
          actual = std::move(replacement);
        } else if (c.op == LvalueEmplace) {
          actual.emplace(source);
        } else {
          actual.emplace(std::move(source));
        }
        require(source.value == (c.op == RvalueEmplace ? -1 : int(c.b)),
                "lvalue capture copies while rvalue capture moves");
        require(ledger_.moves == moves + (c.op == RvalueEmplace),
                "spilled construction calls target move only for rvalues");
        model_[c.a] = static_cast<int>(c.b);
        break;
      }
      case FailedCopy:
      case FailedMove:
      case FailedAllocation: {
        ThrowingMoveTarget source(&ledger_, static_cast<int>(c.b));
        if (c.op == FailedAllocation) {
          allocation::fault.remaining = 0;
          require(throws<std::bad_alloc>([&] { actual.emplace(source); }),
                  "spilled callable allocation failure is transactional");
          require(allocation::fault.remaining == -1,
                  "callable consumes requested allocation fault");
        } else if (c.op == FailedCopy) {
          ledger_.fail_copy = true;
          require(throws<ConstructorError>([&] { actual.emplace(source); }),
                  "spilled lvalue construction failure propagates");
          ledger_.fail_copy = false;
        } else {
          ledger_.fail_move = true;
          require(throws<MoveError>([&] { actual.emplace(std::move(source)); }),
                  "spilled rvalue construction failure propagates");
          ledger_.fail_move = false;
        }
        require(source.value == int(c.b), "rejected target remains unchanged");
        break;
      }
      case MoveAssign:
      case SelfMove:
      case RoundTrip: {
        const auto moves = ledger_.moves;
        const auto* address = actual ? actual().address : nullptr;
        allocation::fault.remaining = 0;
        ledger_.fail_move = true;
        if (c.op == MoveAssign) {
          actual_[destination] = std::move(actual);
          model_[destination] = std::exchange(model_[c.a], std::nullopt);
          require(!actual, "spilled wrapper move empties source");
          if (actual_[destination])
            require(actual_[destination]().address == address,
                    "spilled move assignment preserves target identity");
        } else if (c.op == SelfMove) {
          auto& alias = actual;
          actual = std::move(alias);
          if (actual)
            require(actual().address == address, "self move preserves target");
        } else {
          Function temporary(std::move(actual));
          require(!actual, "spilled move constructor empties source");
          if (temporary)
            require(temporary().address == address,
                    "spilled move constructor preserves target identity");
          actual = std::move(temporary);
        }
        ledger_.fail_move = false;
        require(ledger_.moves == moves,
                "wrapper move never invokes a throwing target move");
        require(allocation::fault.remaining == 0,
                "wrapper transfer never allocates replacement storage");
        break;
      }
      case Null:
        actual = Function(nullptr);
        model_[c.a].reset();
        actual.reset();
        break;
      case ThrowInvoke:
        ledger_.fail_invoke = true;
        require(throws<InvocationError>([&] { (void)actual(); }),
                "target exception propagates from invocation");
        ledger_.fail_invoke = false;
        break;
    }
  }
  void invariant() {
    unsigned live = 0;
    for (unsigned i = 0; i < actual_.size(); ++i) {
      require(bool(actual_[i]) == model_[i].has_value(), "callable occupancy");
      if (!model_[i]) continue;
      ++live;
      require(actual_[i]().value == *model_[i],
              "replacement errors retain the previously committed target");
    }
    require(ledger_.live == int(live), "exactly one live target per wrapper");
    require(allocation::live.load() == live,
            "failed target construction returns allocated storage");
  }
  void finish() {
    for (auto& actual : actual_) actual.reset();
    for (auto& model : model_) model.reset();
    invariant();
  }
};

// Both wrapper policies must implement ordinary invoke/reference forwarding,
// independent of storage selection. The model observes caller-owned data.
template <bool Heap>
class ForwardingCallableModel {
  using Function = std::conditional_t<
      Heap, dagflow::small_function<void(std::unique_ptr<int>, int&)>,
      dagflow::inplace_function<void(std::unique_ptr<int>, int&)>>;
  struct Action {
    int bias;
    void operator()(std::unique_ptr<int> input, int& output) {
      output += *input + bias;
    }
  };
  struct Record {
    int value = 0;
    int add(int delta) { return value += delta; }
  } actual_record_;
  enum Op : unsigned {
    Bind,
    Invoke,
    Move,
    Null,
    MemberCall,
    MemberReference,
    Count
  };
  std::array<Function, 2> actual_;
  std::array<std::optional<int>, 2> model_;
  int actual_value_ = 0;
  int expected_value_ = 0;
  int expected_record_ = 0;

 public:
  Command next(Random& random) const {
    Command c{static_cast<unsigned>(random.bounded(Count)), random.bounded(2),
              random.bounded(16)};
    if (!precondition(c)) c.op = Bind;
    return c;
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 2 && c.b < 16 &&
           (c.op != Invoke || model_[c.a].has_value());
  }
  void apply(const Command& c) {
    switch (c.op) {
      case Bind: {
        Action source{static_cast<int>(c.b)};
        actual_[c.a].emplace(source);
        model_[c.a] = static_cast<int>(c.b);
        break;
      }
      case Invoke: {
        auto input = std::make_unique<int>(static_cast<int>(c.b));
        actual_[c.a](std::move(input), actual_value_);
        require(!input, "invocation consumes move-only argument");
        expected_value_ += static_cast<int>(c.b) + *model_[c.a];
        break;
      }
      case Move: {
        const auto destination = c.b % 2;
        actual_[destination] = std::move(actual_[c.a]);
        if (destination != c.a)
          model_[destination] = std::exchange(model_[c.a], std::nullopt);
        break;
      }
      case Null:
        actual_[c.a] = Function(nullptr);
        model_[c.a].reset();
        break;
      case MemberCall: {
        using Method =
            std::conditional_t<Heap, dagflow::small_function<int(Record&, int)>,
                               dagflow::inplace_function<int(Record&, int)>>;
        Method method(&Record::add);
        expected_record_ += static_cast<int>(c.b);
        require(
            method(actual_record_, static_cast<int>(c.b)) == expected_record_,
            "member-function invocation preserves reference receiver");
        break;
      }
      case MemberReference: {
        using Member =
            std::conditional_t<Heap, dagflow::small_function<int&(Record&)>,
                               dagflow::inplace_function<int&(Record&)>>;
        Member member(&Record::value);
        int& alias = member(actual_record_);
        require(&alias == &actual_record_.value,
                "member-data invocation returns an actual reference");
        alias = static_cast<int>(c.b);
        expected_record_ = static_cast<int>(c.b);
        break;
      }
    }
  }
  void invariant() const {
    require(actual_value_ == expected_value_, "void invocation mutates caller");
    require(actual_record_.value == expected_record_, "member receiver state");
    for (unsigned i = 0; i < actual_.size(); ++i)
      require(bool(actual_[i]) == model_[i].has_value(), "forwarder occupancy");
    require(allocation::live.load() == 0, "small forwarders stay inline");
  }
};
}  // namespace

int main() {
  run<MoveOnlyVectorModel>("storage API: throwing move-only vector", 48, 64);
  run<SpilledCallableModel>("storage API: spilled target transactional moves",
                            48, 64);
  run<ForwardingCallableModel<true>>("storage API: small function invocation",
                                     32, 48);
  run<ForwardingCallableModel<false>>(
      "storage API: inplace function invocation", 32, 48);
}
