#include <algorithm>
#include <array>
#include <barrier>
#include <cstdint>
#include <dagflow/detail/chase_lev_deque.hpp>
#include <dagflow/detail/ring_mpmc.hpp>
#include <deque>
#include <thread>
#include <utility>

#include "property_support.hpp"

namespace {
using dagflow::test::property::Command;
using dagflow::test::property::Random;
using dagflow::test::property::require;
using dagflow::test::property::run;

// Moving from this value is observable: a rejected publication must retain
// custody in the caller, and a failed acquisition must leave output unchanged.
struct Token {
  int value = -1;
  Token() = default;
  explicit Token(int input) : value(input) {}
  Token(const Token&) = default;
  Token& operator=(const Token&) noexcept = default;
  Token(Token&& other) noexcept : value(std::exchange(other.value, -1)) {}
  Token& operator=(Token&& other) noexcept {
    value = std::exchange(other.value, -1);
    return *this;
  }
};

template <std::size_t Capacity>
struct RingMachine {
  enum Op {
    PushCopy,
    PushMove,
    Pop,
    Fill,
    Drain,
    CompetingConsumers,
    CompetingProducers,
    Count
  };
  dagflow::detail::ring_mpmc<Token, Capacity> queue;
  std::deque<int> model;
  int next_token = 0;

  Command next(Random& random) const {
    return {static_cast<unsigned>(random.bounded(Count)), 0, 0};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a == 0 && c.b == 0;
  }
  void push(bool move) {
    Token input(++next_token);
    const bool expected = model.size() < Capacity;
    const bool success =
        move ? queue.try_push(std::move(input)) : queue.try_push(input);
    require(success == expected, "ring bounded admission");
    require(input.value == (move && success ? -1 : next_token),
            "ring publication transfers custody only on success");
    if (success) model.push_back(next_token);
  }
  void pop() {
    Token output;
    const bool success = queue.try_pop(output);
    require(success == !model.empty(), "ring availability without publishers");
    if (success) {
      require(output.value == model.front(), "ring FIFO acquisition");
      model.pop_front();
    } else {
      require(output.value == -1, "ring failed pop preserves output");
    }
  }
  void competing_consumers() {
    std::barrier gate(3);
    std::array<Token, 2> output;
    std::array<bool, 2> success{};
    std::thread first([&] {
      gate.arrive_and_wait();
      success[0] = queue.try_pop(output[0]);
    });
    std::thread second([&] {
      gate.arrive_and_wait();
      success[1] = queue.try_pop(output[1]);
    });
    gate.arrive_and_wait();
    first.join();
    second.join();

    // No publisher is in flight, so every result must admit one of the two
    // serial consumer orders. Comparing just a set would miss FIFO violations.
    bool legal_history = false;
    std::deque<int> remaining;
    for (unsigned order = 0; order != 2; ++order) {
      auto candidate = model;
      bool legal = true;
      for (unsigned step = 0; step != 2; ++step) {
        const auto index = order ^ step;
        const bool available = !candidate.empty();
        if (success[index] != available) legal = false;
        const int expected = available ? candidate.front() : -1;
        if (output[index].value != expected) legal = false;
        if (available) candidate.pop_front();
      }
      if (legal) {
        legal_history = true;
        remaining = std::move(candidate);
      }
    }
    require(legal_history, "ring consumer history has a legal FIFO ordering");
    model = std::move(remaining);
  }
  void competing_producers() {
    std::barrier gate(3);
    const std::array<int, 2> identities{++next_token, ++next_token};
    std::array<Token, 2> input{Token(identities[0]), Token(identities[1])};
    std::array<bool, 2> success{};
    std::thread first([&] {
      gate.arrive_and_wait();
      success[0] = queue.try_push(std::move(input[0]));
    });
    std::thread second([&] {
      gate.arrive_and_wait();
      success[1] = queue.try_push(std::move(input[1]));
    });
    gate.arrive_and_wait();
    first.join();
    second.join();
    const auto free = Capacity - model.size();
    require(static_cast<unsigned>(success[0]) + success[1] ==
                std::min<std::size_t>(2, free),
            "competing producers occupy exactly the available slots");
    for (unsigned i = 0; i != 2; ++i)
      require(input[i].value == (success[i] ? -1 : identities[i]),
              "competing producer retains rejected token");
    // Observe queue order, then validate against both legal producer orders.
    std::deque<int> observed;
    Token output;
    while (queue.try_pop(output)) {
      observed.push_back(output.value);
      require(observed.size() <= Capacity, "ring cannot duplicate packets");
    }
    bool legal_history = false;
    for (unsigned order = 0; order != 2; ++order) {
      auto candidate = model;
      for (unsigned step = 0; step != 2; ++step) {
        const auto index = order ^ step;
        if (success[index]) candidate.push_back(identities[index]);
      }
      legal_history |= candidate == observed;
    }
    require(legal_history, "producer history preserves prior FIFO prefix");
    model.clear();
  }
  void apply(const Command& c) {
    switch (c.op) {
      case PushCopy:
        push(false);
        break;
      case PushMove:
        push(true);
        break;
      case Pop:
        pop();
        break;
      case Fill:
        while (model.size() != Capacity) push(true);
        push(true);  // Full rejection is a transition with retained custody.
        break;
      case Drain:
        while (!model.empty()) pop();
        pop();
        break;
      case CompetingConsumers:
        competing_consumers();
        break;
      case CompetingProducers:
        competing_producers();
        break;
    }
  }
  void invariant() const {
    require(model.size() <= Capacity, "ring bounded model");
    require(queue.capacity() == Capacity, "ring fixed capacity");
    require(queue.empty() == model.empty(), "ring quiescent emptiness");
  }
};

template <std::size_t Capacity>
struct DequeMachine {
  enum Op {
    Push,
    Pop,
    Steal,
    Fill,
    DrainOwner,
    DrainThief,
    LastItemRace,
    TwoThieves,
    Count
  };
  dagflow::detail::chase_lev_deque<int, Capacity> queue;
  std::deque<int> model;
  int next_token = 0;

  Command next(Random& random) const {
    return {static_cast<unsigned>(random.bounded(Count)), 0, 0};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a == 0 && c.b == 0;
  }
  void push() {
    const int input = ++next_token;
    const bool expected = model.size() < Capacity;
    require(queue.try_push(input) == expected, "deque bounded admission");
    if (expected) model.push_back(input);
  }
  void take(bool thief) {
    int output = -1;
    const bool success =
        thief ? queue.try_steal(output) : queue.try_pop(output);
    require(success == !model.empty(), "deque sequential availability");
    if (success) {
      require(output == (thief ? model.front() : model.back()),
              "deque FIFO steal / LIFO owner pop");
      if (thief)
        model.pop_front();
      else
        model.pop_back();
    } else {
      require(output == -1, "deque failed acquisition preserves output");
    }
  }
  void race(bool owner) {
    if (owner) {
      while (!model.empty()) take(false);
      push();
    }
    std::barrier gate(2);
    std::array<int, 2> output{-1, -1};
    std::array<bool, 2> success{};
    std::thread thief([&] {
      gate.arrive_and_wait();
      success[1] = queue.try_steal(output[1]);
    });
    gate.arrive_and_wait();
    // Keep every owner operation on the creating thread.
    success[0] = owner ? queue.try_pop(output[0]) : queue.try_steal(output[0]);
    thief.join();
    if (owner) {
      require(success[0] != success[1], "last item has exactly one custodian");
      const unsigned winner = success[0] ? 0 : 1;
      require(output[winner] == model.front(), "last item identity");
      require(output[1 - winner] == -1, "loser does not acquire custody");
      model.pop_front();
      return;
    }
    // A failed steal may mean a lost CAS even with another item available.
    // Successful steals must still be a unique FIFO prefix; there are only
    // two participants, so enumerate both possible successful orders.
    bool legal_history = false;
    std::deque<int> remaining;
    for (unsigned order = 0; order != 2; ++order) {
      auto candidate = model;
      bool legal = true;
      for (unsigned step = 0; step != 2; ++step) {
        const auto index = order ^ step;
        if (success[index]) {
          if (candidate.empty() || candidate.front() != output[index]) {
            legal = false;
          } else {
            candidate.pop_front();
          }
        } else if (output[index] != -1) {
          legal = false;
        }
      }
      if (legal) {
        legal_history = true;
        remaining = std::move(candidate);
      }
    }
    require(legal_history, "thieves acquire a unique FIFO prefix");
    require(model.empty() || success[0] || success[1],
            "at least one thief progresses without a competing owner");
    model = std::move(remaining);
  }
  void apply(const Command& c) {
    switch (c.op) {
      case Push:
        push();
        break;
      case Pop:
        take(false);
        break;
      case Steal:
        take(true);
        break;
      case Fill:
        while (model.size() != Capacity) push();
        push();
        break;
      case DrainOwner:
      case DrainThief:
        while (!model.empty()) take(c.op == DrainThief);
        take(c.op == DrainThief);
        break;
      case LastItemRace:
        race(true);
        break;
      case TwoThieves:
        race(false);
        break;
    }
  }
  void invariant() const {
    require(queue.capacity() == Capacity, "deque fixed capacity");
    require(queue.free_capacity() == Capacity - model.size(),
            "deque quiescent free capacity");
    require(queue.empty() == model.empty(), "deque quiescent emptiness");
  }
};
}  // namespace

int main() {
  run<RingMachine<2>>(
      "ring_mpmc capacity 2 state and publication/acquisition histories", 32,
      64);
  run<RingMachine<8>>(
      "ring_mpmc capacity 8 state and publication/acquisition histories", 32,
      64);
  run<DequeMachine<2>>("chase_lev capacity 2 state and custody races", 32, 64);
  run<DequeMachine<8>>("chase_lev capacity 8 state and custody races", 32, 64);
}
