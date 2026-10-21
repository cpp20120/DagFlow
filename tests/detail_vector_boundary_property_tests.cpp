#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <dagflow/detail/small_vector.hpp>
#include <dagflow/detail/runtime_memory.hpp>
#include <limits>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include "property_support.hpp"

namespace {
using namespace dagflow::test::property;

// This is an arithmetic state, never a fake vector or an allocation request.
// Near-SIZE_MAX capacities exercise the production growth policy safely.
class GrowthArithmeticModel {
  enum Op : unsigned { SetLimit, Append, Fill, Remove, Clear, Boundary, Count };
  std::size_t size_ = 0;
  std::size_t capacity_ = 1;
  std::size_t maximum_ = 7;

  static std::size_t limit(std::uint64_t index) {
    constexpr auto maximum = std::numeric_limits<std::size_t>::max();
    constexpr std::array<std::size_t, 12> limits{
        1,           2,      3, 4, 5, 7, 8, 9, maximum / 2, maximum / 2 + 1,
        maximum - 1, maximum};
    return limits[index];
  }
  void grow() {
    const auto before = capacity_;
    bool threw = false;
    std::size_t result = capacity_;
    try {
      result = dagflow::detail::small_vector_next_capacity(size_, capacity_,
                                                           maximum_);
    } catch (const std::length_error&) {
      threw = true;
    }
    require(threw == (size_ == maximum_),
            "full maximum rejects exactly one-past-limit growth");
    if (threw) {
      require(capacity_ == before, "rejection leaves arithmetic state intact");
      return;
    }
    // Addition of available room is an independent oracle for saturating
    // doubling; no multiplication or overflowing intermediate is needed.
    const auto room = maximum_ - before;
    const auto expected = before + std::min(room, before);
    require(result == expected, "growth doubles or consumes remaining room");
    require(result > size_ && result <= maximum_,
            "growth always provides one slot without exceeding maximum");
    capacity_ = result;
    ++size_;
  }

 public:
  Command next(Random& random) const {
    return {static_cast<unsigned>(random.bounded(Count)), random.bounded(12),
            random.bounded(5)};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 12 && c.b < 5;
  }
  void apply(const Command& c) {
    switch (c.op) {
      case SetLimit:
        maximum_ = limit(c.a);
        size_ = 0;
        capacity_ = 1;
        break;
      case Append:
        if (size_ == capacity_)
          grow();
        else
          ++size_;
        break;
      case Fill:
        size_ = capacity_;
        grow();
        break;
      case Remove:
        if (size_ != 0) --size_;
        break;
      case Clear:
        size_ = 0;
        break;
      case Boundary:
        maximum_ = limit(c.a);
        switch (c.b) {
          case 0:
            capacity_ = maximum_;
            break;
          case 1:
            capacity_ = std::max<std::size_t>(1, maximum_ - 1);
            break;
          case 2:
            capacity_ = std::max<std::size_t>(1, maximum_ / 2);
            break;
          case 3:
            capacity_ = maximum_ / 2 + 1;
            break;
          case 4:
            capacity_ = 1;
            break;
        }
        size_ = capacity_;
        grow();
        break;
    }
  }
  void invariant() const {
    require(size_ <= capacity_ && capacity_ <= maximum_ && capacity_ > 0,
            "valid arithmetic states stay within the logical capacity limit");
  }
};

struct CopyState {
  int live = 0;
  int copies_before_failure = -1;
  bool reject_next_construction = false;
};
struct CopyElement {
  CopyState* state;
  int value;
  CopyElement(CopyState* ledger, int input, bool reject = false)
      : state(ledger), value(input) {
    if (reject || state->reject_next_construction)
      throw std::runtime_error("construction rejected");
    ++state->live;
  }
  CopyElement(const CopyElement& other)
      : state(other.state), value(other.value) {
    if (state->copies_before_failure == 0)
      throw std::runtime_error("copy rejected");
    if (state->copies_before_failure > 0) --state->copies_before_failure;
    ++state->live;
  }
  CopyElement(CopyElement&& other) noexcept(false)
      : state(other.state), value(other.value) {
    ++state->live;
    other.value = -1;
  }
  ~CopyElement() { --state->live; }
};

// Complement the pure arithmetic check with real constructor categories:
// ordinary emplacement, aliased copy, and rollback while a full buffer grows.
class CopyGrowthModel {
  using Vector = dagflow::small_vector<CopyElement, 4>;
  enum Op : unsigned {
    Append,
    Grow,
    AliasGrow,
    FailedGrow,
    FailedConstructAtGrowth,
    Clear,
    Pop,
    Reset,
    Count
  };
  CopyState state_;
  Vector actual_;
  std::vector<int> values_;
  std::size_t capacity_ = 4;

  void append(int value) {
    actual_.emplace_back(&state_, value);
    if (values_.size() == capacity_) capacity_ += capacity_;
    values_.push_back(value);
  }
  void fill() {
    while (values_.size() < capacity_) append(static_cast<int>(values_.size()));
  }

 public:
  Command next(Random& random) const {
    Command c{static_cast<unsigned>(random.bounded(Count)), random.bounded(32),
              0};
    if (!precondition(c)) c.op = Reset;
    return c;
  }
  bool precondition(const Command& c) const {
    if (c.op >= Count || c.a >= 32 || c.b != 0) return false;
    if (c.op == Pop) return !values_.empty();
    if (c.op == Append) return values_.size() < 64;
    if (c.op == Grow || c.op == AliasGrow || c.op == FailedGrow ||
        c.op == FailedConstructAtGrowth)
      return capacity_ < 64;
    return true;
  }
  void apply(const Command& c) {
    switch (c.op) {
      case Append:
        append(static_cast<int>(c.a));
        break;
      case Grow:
        fill();
        append(static_cast<int>(c.a));
        break;
      case AliasGrow: {
        fill();
        const auto index = c.a % values_.size();
        const auto value = values_[index];
        actual_.push_back(actual_[index]);
        capacity_ += capacity_;
        values_.push_back(value);
        break;
      }
      case FailedGrow: {
        fill();
        state_.copies_before_failure = static_cast<int>(c.a % values_.size());
        bool threw = false;
        try {
          actual_.emplace_back(&state_, static_cast<int>(c.a));
        } catch (const std::runtime_error&) {
          threw = true;
        }
        state_.copies_before_failure = -1;
        require(threw, "copy failure during full-buffer growth propagates");
        break;
      }
      case FailedConstructAtGrowth: {
        fill();
        // Unlike FailedGrow, reject construction of the *new* element before
        // relocation begins. Reuse the existing emplace_back<CopyState*, int>
        // instantiation rather than introducing an extra compiler branch set.
        const auto* before_data = actual_.data();
        const auto before_size = actual_.size();
        const auto before_capacity = actual_.capacity();
        const auto before_live = state_.live;
        state_.reject_next_construction = true;
        bool threw = false;
        try {
          actual_.emplace_back(&state_, static_cast<int>(c.a));
        } catch (const std::runtime_error&) {
          threw = true;
        }
        state_.reject_next_construction = false;
        require(threw, "new-element constructor failure at growth propagates");
        require(actual_.data() == before_data &&
                    actual_.size() == before_size &&
                    actual_.capacity() == before_capacity,
                "failed construction preserves the old storage and capacity");
        require(state_.live == before_live,
                "failed construction does not change live element count");
        break;
      }
      case Clear:
        actual_.clear();
        values_.clear();
        break;
      case Pop:
        actual_.pop_back();
        values_.pop_back();
        break;
      case Reset:
        actual_ = Vector{};
        values_.clear();
        capacity_ = 4;
        break;
    }
  }
  void invariant() const {
    require(actual_.size() == values_.size(), "copy growth logical size");
    require(actual_.capacity() == capacity_,
            "real vector follows growth policy");
    require(state_.live == static_cast<int>(values_.size()),
            "growth rollback retires newly constructed and relocated elements");
    for (std::size_t i = 0; i < values_.size(); ++i)
      require(actual_[i].value == values_[i],
              "copy relocation keeps original sequence on failure");
  }
  void finish() {
    actual_ = Vector{};
    values_.clear();
    capacity_ = 4;
    invariant();
  }
};


// The vector arithmetic and accounting boundary models above are already
// present in the current repository. Exercise the remaining allocator and
// explicit custody-transfer contracts here; do not mutate runtime headers or
// introduce another CMake target just to test those branches.
struct alignas(128) OwnedCell {
  static inline int alive = 0;
  static inline int constructed = 0;
  static inline int destroyed = 0;
  OwnedCell() noexcept {
    ++alive;
    ++constructed;
  }
  ~OwnedCell() {
    --alive;
    ++destroyed;
  }
};

class RuntimeOwnershipModel {
  using Array = dagflow::detail::OwnedArray<OwnedCell>;
  using Object = dagflow::detail::OwnedObject<OwnedCell>;
  enum Op : unsigned {
    MakeArray, MakeObject, ClearArray, ClearObject, MoveArray,
    MoveObject, ReleaseArray, ReleaseObject, NullDeleters,
    EmptyAndZeroStorage, Oversize, Count
  };
  std::array<Array, 2> arrays_{};
  std::array<Object, 2> objects_{};
  std::array<std::size_t, 2> array_sizes_{};
  std::array<bool, 2> object_live_{};

  static void check_alignment(const void* pointer) {
    require(reinterpret_cast<std::uintptr_t>(pointer) % alignof(OwnedCell) == 0,
            "owned runtime memory respects over-alignment");
  }

 public:
  RuntimeOwnershipModel() {
    require(OwnedCell::alive == 0, "previous ownership trace released all cells");
  }
  Command next(Random& random) const {
    return {static_cast<unsigned>(random.bounded(Count)), random.bounded(2),
            random.bounded(5)};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < 2 && c.b < 5;
  }
  void apply(const Command& c) {
    const auto slot = static_cast<std::size_t>(c.a);
    const auto other = 1 - slot;
    switch (c.op) {
      case MakeArray: {
        const auto n = static_cast<std::size_t>(c.b + 1);
        auto owned = dagflow::detail::make_owned_array<OwnedCell>(n);
        require(owned != nullptr, "nonempty array has an owner");
        check_alignment(owned.get());
        arrays_[slot] = std::move(owned);
        array_sizes_[slot] = n;
        break;
      }
      case MakeObject: {
        auto owned = dagflow::detail::make_owned<OwnedCell>();
        require(owned != nullptr, "constructed object has an owner");
        check_alignment(owned.get());
        objects_[slot] = std::move(owned);
        object_live_[slot] = true;
        break;
      }
      case ClearArray:
        arrays_[slot].reset();
        array_sizes_[slot] = 0;
        break;
      case ClearObject:
        objects_[slot].reset();
        object_live_[slot] = false;
        break;
      case MoveArray:
        arrays_[other] = std::move(arrays_[slot]);
        array_sizes_[other] = array_sizes_[slot];
        array_sizes_[slot] = 0;
        break;
      case MoveObject:
        objects_[other] = std::move(objects_[slot]);
        object_live_[other] = object_live_[slot];
        object_live_[slot] = false;
        break;
      case ReleaseArray: {
        auto* raw = arrays_[slot].release();
        arrays_[slot].get_deleter()(raw);
        array_sizes_[slot] = 0;
        break;
      }
      case ReleaseObject: {
        auto* raw = objects_[slot].release();
        objects_[slot].get_deleter()(raw);
        object_live_[slot] = false;
        break;
      }
      case NullDeleters: {
        const auto before = OwnedCell::alive;
        dagflow::detail::ObjectDeleter<OwnedCell>{}(nullptr);
        dagflow::detail::ArrayDeleter<OwnedCell>{4}(nullptr);
        require(before == OwnedCell::alive,
                "explicit null deletion must not destroy a live owner");
        break;
      }
      case EmptyAndZeroStorage: {
        auto empty = dagflow::detail::make_owned_array<OwnedCell>(0);
        require(!empty, "zero-size owned array has no allocation or elements");
        dagflow::detail::RuntimeAllocator<OwnedCell> allocator;
        auto* zero = allocator.allocate(0);
        require(zero != nullptr, "zero-count STL allocation is reclaimable");
        check_alignment(zero);
        allocator.deallocate(zero, 0);
        break;
      }
      case Oversize: {
        const auto max_objects =
            static_cast<std::size_t>(std::numeric_limits<std::ptrdiff_t>::max()) /
            sizeof(OwnedCell);
        bool length_rejected = false;
        try {
          auto impossible = dagflow::detail::make_owned_array<OwnedCell>(
              max_objects + 1);
          (void)impossible;
        } catch (const std::length_error&) {
          length_rejected = true;
        }
        require(length_rejected, "owned array rejects ptrdiff_t overflow");
        dagflow::detail::RuntimeAllocator<OwnedCell> allocator;
        bool allocation_rejected = false;
        try {
          auto* impossible = allocator.allocate(
              std::numeric_limits<std::size_t>::max() / sizeof(OwnedCell) + 1);
          allocator.deallocate(impossible, 0);
        } catch (const std::bad_array_new_length&) {
          allocation_rejected = true;
        }
        require(allocation_rejected, "STL allocator rejects size_t overflow");
        break;
      }
    }
  }
  void invariant() const {
    std::size_t live = 0;
    for (std::size_t i = 0; i < 2; ++i) {
      require(static_cast<bool>(arrays_[i]) == (array_sizes_[i] != 0),
              "array custody matches abstract ownership");
      require(static_cast<bool>(objects_[i]) == object_live_[i],
              "object custody matches abstract ownership");
      if (arrays_[i]) check_alignment(arrays_[i].get());
      if (objects_[i]) check_alignment(objects_[i].get());
      live += array_sizes_[i] + (object_live_[i] ? 1 : 0);
    }
    require(OwnedCell::alive == static_cast<int>(live),
            "every accepted object has exactly one live owner");
    require(OwnedCell::constructed - OwnedCell::destroyed == OwnedCell::alive,
            "construction and destruction remain balanced");
  }
  void finish() {
    for (auto& array : arrays_) array.reset();
    for (auto& object : objects_) object.reset();
    array_sizes_.fill(0);
    object_live_.fill(false);
    invariant();
  }
};
}  // namespace

// Guarantee both inline->heap and heap->heap growth reject an element before
// relocation, regardless of random seeds. Follow each rejection with a valid
// insertion to demonstrate the vector is still usable.
void check_new_element_failure_at_growth() {
  CopyState state;
  using Vector = dagflow::small_vector<CopyElement, 4>;
  {
    Vector actual;
    for (int boundary : {4, 8}) {
      while (actual.size() < static_cast<std::size_t>(boundary))
        actual.emplace_back(&state, static_cast<int>(actual.size()));
      const auto* const before_data = actual.data();
      const auto before_live = state.live;
      const auto before_capacity = actual.capacity();
      state.reject_next_construction = true;
      bool rejected = false;
      try {
        actual.emplace_back(&state, 99);
      } catch (const std::runtime_error&) {
        rejected = true;
      }
      state.reject_next_construction = false;
      require(rejected, "full-vector emplacement propagates constructor failure");
      require(actual.data() == before_data &&
                  actual.capacity() == before_capacity &&
                  actual.size() == static_cast<std::size_t>(boundary),
              "failed first-stage growth keeps original vector storage");
      require(state.live == before_live,
              "failed first-stage growth creates no surviving elements");
      for (int i = 0; i < boundary; ++i)
        require(actual[static_cast<std::size_t>(i)].value == i,
                "failed first-stage growth preserves the element sequence");
      actual.emplace_back(&state, boundary);
      require(actual.back().value == boundary,
              "vector remains usable after failed first-stage growth");
    }
  }
  require(state.live == 0, "all successfully constructed elements are retired");
}

int main() {
  check_new_element_failure_at_growth();
  run<GrowthArithmeticModel>("detail vector: saturating capacity arithmetic",
                             48, 64);
  run<CopyGrowthModel>("detail vector: copy construction at growth boundaries",
                       48, 64);
  run<RuntimeOwnershipModel>("detail storage: owned allocator lifetime and bounds",
                             48, 64);
}
