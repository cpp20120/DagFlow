#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <cstdint>
#include <cstdio>
#include <dagflow/detail/ring_mpmc.hpp>
#include <deque>
#include <thread>
#include <vector>

#include "property_support.hpp"

namespace {
using namespace dagflow::test::property;

// Positive (accepted/acquired) operations are checked against bounded FIFO
// invocation/response histories. A failed nonblocking try_push/try_pop may be
// transient: ring_mpmc explicitly permits a busy, unpublished head/slot.
// We only require failed operations to obey full/empty semantics when they
// cannot overlap another operation. This is NOT full linearizability of the
// success/failure status against a strict abstract bounded queue.
struct Event {
  bool push{};
  int token{};
  bool success{};
  int result{-1};
  std::uint64_t invoked{}, returned{};
};
constexpr unsigned operations = 6;
constexpr int sentinel = -777;

bool legal_sequence(const std::array<Event, operations>& history,
                    unsigned used, std::deque<int>& state,
                    const std::deque<int>& final_state) {
  if (used == (1u << operations) - 1) return state == final_state;
  for (unsigned i = 0; i < operations; ++i) {
    if (used & (1u << i)) continue;
    bool blocked = false;
    // A must precede B when A returned before B was invoked.
    for (unsigned j = 0; j < operations; ++j)
      if (j != i && !(used & (1u << j)) &&
          history[j].returned < history[i].invoked)
        blocked = true;
    if (blocked) continue;
    const auto& event = history[i];
    std::deque<int> next = state;
    bool concurrent = false;
    for (unsigned j = 0; j < operations; ++j)
      if (j != i && history[j].invoked < event.returned &&
          event.invoked < history[j].returned)
        concurrent = true;
    if (event.push) {
      if (event.success) {
        if (next.size() >= 2) continue;
        next.push_back(event.token);
      } else if (next.size() < 2 && !concurrent) {
        continue; // Uncontended false-full is not allowed.
      }
    } else {
      if (event.success) {
        if (next.empty() || next.front() != event.result) continue;
        next.pop_front();
      } else if (event.result != sentinel || (!next.empty() && !concurrent)) {
        continue; // Failed pop preserves output; false-empty needs contention.
      }
    }
    if (legal_sequence(history, used | (1u << i), next, final_state)) return true;
  }
  return false;
}

struct QueueHistoryMachine {
  enum { ConcurrentHistory, Count };
  unsigned rounds{};
  static inline std::atomic<unsigned> overlapping{0},
      push_success{0}, push_failure{0}, pop_success{0}, pop_failure{0};

  Command next(Random& rng) const {
    return {ConcurrentHistory, rng.bounded(1u << operations), rng.bounded(3)};
  }
  bool precondition(const Command& c) const {
    return c.op < Count && c.a < (1u << operations) && c.b < 3;
  }
  void apply(const Command& c) {
    dagflow::detail::ring_mpmc<int, 2> queue;
    std::deque<int> starting;
    for (unsigned i = 0; i < c.b; ++i) {
      require(queue.try_push(100 + static_cast<int>(i)), "initial queue admission");
      starting.push_back(100 + static_cast<int>(i));
    }
    std::array<Event, operations> history{};
    for (unsigned i = 0; i < operations; ++i) {
      history[i].push = (c.a >> i) & 1;
      history[i].token = 1000 + static_cast<int>(i);
    }
    std::atomic<std::uint64_t> clock{1};
    std::barrier start(3);
    auto run_thread = [&](unsigned thread_id) {
      start.arrive_and_wait();
      for (unsigned step = 0; step < 3; ++step) {
        const unsigned i = thread_id * 3 + step;
        auto& e = history[i];
        int output = sentinel;
        e.invoked = clock.fetch_add(1, std::memory_order_seq_cst);
        if (e.push)
          e.success = queue.try_push(e.token);
        else
          e.success = queue.try_pop(output);
        e.result = e.push ? sentinel : output;
        e.returned = clock.fetch_add(1, std::memory_order_seq_cst);
        if (e.push) (e.success ? push_success : push_failure).fetch_add(1);
        else (e.success ? pop_success : pop_failure).fetch_add(1);
        if (step == 0) std::this_thread::yield();
      }
    };
    std::thread a(run_thread, 0), b(run_thread, 1);
    start.arrive_and_wait();
    a.join();
    b.join();
    for (unsigned i = 0; i < 3; ++i)
      for (unsigned j = 3; j < operations; ++j)
        if (history[i].invoked < history[j].returned &&
            history[j].invoked < history[i].returned)
          overlapping.fetch_add(1);

    // The drained final state is another observable constraint; a history
    // that fits intermediate return values but leaves the wrong queue fails.
    std::deque<int> final_state;
    int output{};
    while (queue.try_pop(output)) final_state.push_back(output);
    auto candidate = starting;
    require(legal_sequence(history, 0, candidate, final_state),
            "successful queue operations preserve FIFO and real-time order; failures respect contention contract");
    ++rounds;
  }
  void invariant() const { require(rounds <= 10000, "bounded concurrent histories"); }
};

// Known bad histories must be rejected by the checker, independent of any
// tested queue execution. This is mutation calibration, not SUT mutation.
void verify_checker_mutations() {
  std::array<Event, operations> events{};
  for (unsigned i = 0; i < operations; ++i) {
    events[i].invoked = 2 * i + 1;
    events[i].returned = 2 * i + 2;
  }
  events[0].push = true; events[0].token = 10; events[0].success = true;
  events[1].push = true; events[1].token = 11; events[1].success = true;
  events[2].result = 10; events[2].success = true;
  events[3].result = 11; events[3].success = true;
  events[4].result = sentinel; events[4].success = false;
  events[5].result = sentinel; events[5].success = false;
  std::deque<int> empty;
  require(legal_sequence(events, 0, empty, empty), "valid sequential FIFO history passes");
  auto mutated = events;
  mutated[2].result = 11; // FIFO inversion.
  require(!legal_sequence(mutated, 0, empty, empty), "checker rejects FIFO inversion");
  mutated = events;
  mutated[4].success = true; mutated[4].result = 10; // Phantom duplicate.
  require(!legal_sequence(mutated, 0, empty, empty), "checker rejects duplicate acquisition");
  mutated = events;
  mutated[2].returned = events[2].invoked + 1;
  mutated[2].result = sentinel; mutated[2].success = false;
  require(!legal_sequence(mutated, 0, empty, empty), "checker rejects false empty after publication");
}
}

int main() {
  verify_checker_mutations();
  run<QueueHistoryMachine>("concurrent history: two producers/consumers, bounded FIFO", 16, 24);
  std::printf("history coverage: push success=%u reject=%u pop success=%u empty=%u overlaps=%u\n",
              QueueHistoryMachine::push_success.load(),
              QueueHistoryMachine::push_failure.load(),
              QueueHistoryMachine::pop_success.load(),
              QueueHistoryMachine::pop_failure.load(),
              QueueHistoryMachine::overlapping.load());
}
