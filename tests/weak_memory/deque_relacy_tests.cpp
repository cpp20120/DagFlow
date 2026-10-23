// The runner generates this header from the current production deque, changing
// only the atomic backend. No schedule controller or runtime hooks are enabled.
#include <string_view>

#include "model_support.hpp"

// The source-derived header uses the atomic backend declared above.
#include "deque_under_test.hpp"

template <bool CheckPayload>
struct Publication : rl::test_suite<Publication<CheckPayload>, 2> {
  dagflow::detail::chase_lev_deque<int, 2> queue;
  rl::var<int> payload;

  void before() { payload($) = 0; }
  void thread(unsigned actor) {
    if (actor == 0) {
      if constexpr (CheckPayload) payload($) = 42;
      RL_ASSERT(queue.try_push(1));
    } else {
      int value = -1;
      if (queue.try_steal(value)) {
        RL_ASSERT(value == 1);
        if constexpr (CheckPayload) RL_ASSERT(payload($) == 42);
      } else {
        RL_ASSERT(value == -1);
      }
    }
  }
};

template <unsigned Items>
struct Arbitration : rl::test_suite<Arbitration<Items>, 2> {
  dagflow::detail::chase_lev_deque<int, 2> queue;
  rl::var<int> acquired[Items + 1];

  void before() {
    for (unsigned i = 0; i < Items; ++i) RL_ASSERT(queue.try_push(i + 1));
    for (auto& item : acquired) item($) = -1;
  }
  void thread(unsigned actor) {
    const unsigned attempts = actor == 0 ? 1 : Items;
    for (unsigned i = 0; i < attempts; ++i) {
      int value = -1;
      const bool success =
          actor == 0 ? queue.try_pop(value) : queue.try_steal(value);
      if (success) {
        RL_ASSERT(value >= 1 && value <= int(Items));
        acquired[actor + i]($) = value;
      } else {
        RL_ASSERT(value == -1);
      }
    }
  }
  void after() {
    unsigned seen = 0;
    const auto record = [&](int value) {
      RL_ASSERT(value >= 1 && value <= int(Items));
      const unsigned bit = 1u << (value - 1);
      RL_ASSERT((seen & bit) == 0);
      seen |= bit;
    };
    for (auto& item : acquired) {
      const int value = item($);
      if (value != -1) record(value);
    }
    // All simulated threads have joined; quiescent ownership returns here.
    int remaining = -1;
    for (unsigned i = 0; i < Items; ++i) {
      if (!queue.try_pop(remaining)) break;
      record(remaining);
    }
    RL_ASSERT(seen == (1u << Items) - 1);
  }
};

int main(int argc, char** argv) {
  if (argc != 2) return 2;
  const std::string_view suite(argv[1]);
  if (suite == "publication") return check_model<Publication<true>>(argv[1]);
  if (suite == "slot-publication")
    return check_model<Publication<false>>(argv[1]);
  if (suite == "last-item") return check_model<Arbitration<1>>(argv[1]);
  if (suite == "two-items") return check_model<Arbitration<2>>(argv[1]);
  return 2;
}
