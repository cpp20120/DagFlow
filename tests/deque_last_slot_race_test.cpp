#include <atomic>
#include <barrier>
#include <cstdint>
#include <dagflow/detail/chase_lev_deque.hpp>
#include <thread>

#include "support.hpp"

// In each generation the owner and one thief race for the ONLY element.
// The loser must not overwrite the caller's output, and custody must move
// to precisely one party. A persistent thief keeps the history inexpensive.
int main() {
  using Queue = dagflow::detail::chase_lev_deque<std::uint32_t, 2>;
  Queue queue;
  // Owner pops a non-final item before the last-item CAS protocol. This
  // exercises both sides of t == b independently of thread scheduling.
  {
    std::uint32_t out = 0;
    CHECK(queue.try_push(1));
    CHECK(queue.try_push(2));
    CHECK(!queue.try_push(3));
    CHECK(queue.try_pop(out) && out == 2);
    CHECK(queue.try_pop(out) && out == 1);
    CHECK(queue.empty());
  }
  constexpr unsigned rounds = 12000;
  constexpr std::uint32_t untouched = 0xfeedbeefu;
  std::barrier rendezvous(2);
  std::atomic<unsigned> stolen{0};
  std::uint32_t thief_value = untouched;
  std::thread thief([&] {
    for (unsigned round = 0; round < rounds; ++round) {
      rendezvous.arrive_and_wait();
      std::uint32_t result = untouched;
      const bool claimed = queue.try_steal(result);
      if (claimed) {
        CHECK(result == round + 1);
        stolen.fetch_add(1, std::memory_order_relaxed);
      } else {
        CHECK(result == untouched);
      }
      thief_value = result;
      rendezvous.arrive_and_wait();
    }
  });
  unsigned popped = 0;
  for (unsigned round = 0; round < rounds; ++round) {
    CHECK(queue.empty());
    CHECK(queue.try_push(round + 1));
    rendezvous.arrive_and_wait();
    std::uint32_t result = untouched;
    const bool claimed = queue.try_pop(result);
    if (claimed) {
      CHECK(result == round + 1);
      ++popped;
    } else {
      CHECK(result == untouched);
    }
    rendezvous.arrive_and_wait();
    CHECK(claimed != (thief_value != untouched));
    CHECK(queue.empty());
  }
  thief.join();
  CHECK(popped + stolen.load() == rounds);
}
