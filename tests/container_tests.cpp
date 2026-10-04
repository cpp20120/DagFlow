#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

#include "dagflow/detail/ring_mpmc.hpp"
#include "dagflow/detail/small_function.hpp"
#include "dagflow/detail/small_vector.hpp"
#include "support.hpp"

void vector_storage() {
  dagflow::small_vector<std::string, 2> values;
  static_assert(std::is_same_v<decltype(values)::value_type, std::string>);
  CHECK(values.empty() && values.capacity() == 2);
  values.emplace_back("first");
  values.emplace_back("second");
  values.push_back(
      values.front());  // Aliasing across the inline-to-heap spill.
  CHECK(values.size() == 3 && values.back() == "first");
  values.emplace_back("fourth");
  values.emplace_back(values.front().data(), values.front().size());
  CHECK(values.size() == 5 && values.back() == "first");
  CHECK(values.capacity() == 8);
  values.pop_back();
  values.pop_back();
  CHECK(values[0] == "first" && values[1] == "second");
  values.pop_back();
  const auto& const_values = values;
  CHECK(const_values.cend() - const_values.cbegin() == 2);
  CHECK(const_values.at(1) == "second");
  bool threw = false;
  try {
    (void)values.at(2);
  } catch (const std::out_of_range&) {
    threw = true;
  }
  CHECK(threw);
  auto moved = std::move(values);
  CHECK(values.empty() && moved.size() == 2);
  const auto capacity = moved.capacity();
  moved.clear();
  CHECK(moved.empty() && moved.capacity() == capacity);

  dagflow::small_vector<std::unique_ptr<int>, 2> pointers;
  pointers.push_back(std::make_unique<int>(7));
  auto inline_move = std::move(pointers);
  CHECK(pointers.empty() && *inline_move.front() == 7);
  inline_move.reserve(8);
  CHECK(*inline_move.front() == 7 && inline_move.capacity() >= 8);
  pointers = std::move(inline_move);
  CHECK(inline_move.empty() && *pointers.front() == 7);
}

void vector_alignment() {
  struct alignas(256) Aligned {
    int value;
  };
  dagflow::small_vector<Aligned, 1> values;
  CHECK(reinterpret_cast<std::uintptr_t>(values.data()) % alignof(Aligned) ==
        0);
  values.emplace_back(Aligned{42});
  values.emplace_back(values.front());
  CHECK(reinterpret_cast<std::uintptr_t>(values.data()) % alignof(Aligned) ==
        0);
  CHECK(values.front().value == 42 && values.back().value == 42);
  dagflow::small_vector<char, 1> bytes;
  bytes.push_back('a');
  bytes.push_back(bytes.front());
  CHECK(reinterpret_cast<std::uintptr_t>(bytes.data()) %
            DAGFLOW_CACHE_LINE_SIZE ==
        0);
  CHECK(bytes.front() == 'a' && bytes.back() == 'a');
}

void vector_inline_reuse() {
  struct Value {
    const int value;
    int& alive;
    Value(int n, int& count) : value(n), alive(count) { ++alive; }
    Value(Value&& other) : Value(other.value, other.alive) {}
    ~Value() { --alive; }
  };
  int alive = 0;
  {
    dagflow::small_vector<Value, 2> values{};
    const auto& view = values;
    CHECK(values.begin() == values.end() && view.cbegin() == view.cend());
    CHECK(values.data() == view.data());
    for (int round = 0; round < 3; ++round) {
      auto& first = values.emplace_back(round, alive);
      CHECK(&first == values.data() && view.front().value == round);
      values.emplace_back(10, alive);
      CHECK(alive == 2 && view.end() - view.begin() == 2);
      values.pop_back();
      values.emplace_back(20, alive);
      CHECK(view[1].value == 20 && view.begin()[1].value == 20);
      auto moved = std::move(values);
      CHECK(values.empty() && values.begin() == values.end());
      CHECK(alive == 2 && moved.front().value == round);
      CHECK(moved.back().value == 20);
      values = std::move(moved);
      values.clear();
      CHECK(alive == 0 && view.begin() == view.end());
    }
    values.emplace_back(42, alive);
  }
  CHECK(alive == 0);
}

struct ThrowingValue {
  static inline int alive = 0;
  static inline int copy_budget = 100;
  static inline int move_budget = 100;
  int value;
  explicit ThrowingValue(int n) : value(n) { ++alive; }
  ThrowingValue(const ThrowingValue& other) : value(other.value) {
    if (copy_budget-- == 0) throw std::runtime_error("copy");
    ++alive;
  }
  ThrowingValue(ThrowingValue&& other) : value(other.value) {
    if (move_budget-- == 0) throw std::runtime_error("move");
    other.value = -1;
    ++alive;
  }
  ~ThrowingValue() { --alive; }
};

void vector_exception_lifetime() {
  {
    dagflow::small_vector<ThrowingValue, 2> values;
    values.emplace_back(1);
    values.emplace_back(2);
    ThrowingValue::copy_budget = 1;
    bool threw = false;
    try {
      values.reserve(8);
    } catch (const std::runtime_error&) {
      threw = true;
    }
    CHECK(threw && values.size() == 2 && values.capacity() == 2);
    CHECK(values[0].value == 1 && values[1].value == 2);
    CHECK(ThrowingValue::alive == 2);

    ThrowingValue::move_budget = 1;
    threw = false;
    try {
      auto destination = std::move(values);
    } catch (const std::runtime_error&) {
      threw = true;
    }
    CHECK(threw && values.size() == 2 && ThrowingValue::alive == 2);
  }
  CHECK(ThrowingValue::alive == 0);
}

void queue_move_ownership() {
  dagflow::detail::ring_mpmc<std::unique_ptr<int>, 2> queue;
  auto first = std::make_unique<int>(1), second = std::make_unique<int>(2);
  auto third = std::make_unique<int>(3);
  CHECK(queue.try_push(std::move(first)) && !first);
  CHECK(queue.try_push(std::move(second)) && !second);
  CHECK(!queue.try_push(std::move(third)) && third && *third == 3);
  std::unique_ptr<int> value;
  CHECK(queue.try_pop(value) && *value == 1);
  CHECK(queue.try_pop(value) && *value == 2);
  CHECK(!queue.try_pop(value) && *value == 2);
}

void callable_lifetime() {
  int result = 0;
  dagflow::small_function<void()> function(
      [value = std::make_unique<int>(7), &result] { result += *value; });
  auto moved = std::move(function);
  CHECK(!function && moved);
  moved();
  CHECK(result == 7);
  function = std::move(moved);
  CHECK(!moved && function);
  function();
  CHECK(result == 14);
  function.reset();
  CHECK(!function);
}

void callable_arguments() {
  int result = 0;
  dagflow::small_function<void(int&, std::unique_ptr<int>)> function(
      [](int& destination, std::unique_ptr<int> value) {
        destination += *value;
      });
  auto moved = std::move(function);
  moved(result, std::make_unique<int>(9));
  CHECK(result == 9 && !function);
  dagflow::small_function<void(std::size_t)> indexed(
      [&result](std::size_t index) { result += static_cast<int>(index); });
  indexed(4);
  CHECK(result == 13);
}

int main() {
  vector_storage();
  vector_alignment();
  vector_inline_reuse();
  vector_exception_lifetime();
  queue_move_ownership();
  callable_lifetime();
  callable_arguments();
}
