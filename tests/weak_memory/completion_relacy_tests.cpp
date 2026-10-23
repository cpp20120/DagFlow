#include <string_view>

#include "completion_under_test.hpp"
#include "model_support.hpp"

void initialize(CompletionState& state, unsigned credits = 1) {
  state.error($) = 0;
  state.credits.store(credits, rl::memory_order_relaxed);
  // One observing reference pins stack storage throughout each fixture.
  state.references.store(credits + 1, rl::memory_order_relaxed);
}
void terminal(CompletionState& state) {
  RL_ASSERT(state.credits.load(rl::memory_order_relaxed) == 0);
  RL_ASSERT(state.ready.load(rl::memory_order_relaxed));
  RL_ASSERT(state.references.load(rl::memory_order_relaxed) == 1);
  RL_ASSERT(!state.payload && !state.destroy_payload &&
            state.dependents.empty());
  state.release();
}

struct Aggregate : rl::test_suite<Aggregate, 2> {
  CompletionState state;
  rl::var<int> effects[2], cleaned;
  void before() {
    initialize(state, 2);
    for (auto& value : effects) value($) = 0;
    cleaned($) = 0;
    state.payload = this;
    state.destroy_payload = [](void* p) noexcept {
      auto& fixture = *static_cast<Aggregate*>(p);
      RL_ASSERT(fixture.effects[0]($) == 1 && fixture.effects[1]($) == 1);
      RL_ASSERT(fixture.cleaned($) == 0);
      fixture.cleaned($) = 1;
    };
  }
  void thread(unsigned actor) {
    effects[actor]($) = 1;
    CompletionState::retire(&state);
  }
  void after() {
    RL_ASSERT(cleaned($) == 1);
    terminal(state);
  }
};

template <bool Wait>
struct Visibility : rl::test_suite<Visibility<Wait>, 2> {
  CompletionState state;
  rl::var<int> effect, cleaned;
  void before() {
    initialize(state);
    effect($) = 0;
    cleaned($) = 0;
    state.payload = this;
    state.destroy_payload = [](void* p) noexcept {
      auto& fixture = *static_cast<Visibility*>(p);
      fixture.cleaned($) = 1;
    };
  }
  void thread(unsigned actor) {
    if (actor == 0) {
      effect($) = 42;
      CompletionState::retire(&state);
    } else {
      if constexpr (Wait) state.wait();
      if (Wait || model_ready(&state)) {
        RL_ASSERT(effect($) == 42 && cleaned($) == 1);
      }
    }
  }
  void after() { terminal(state); }
};

template <bool WithError>
struct Registration : rl::test_suite<Registration<WithError>, 2> {
  CompletionState source, dependent;
  rl::var<int> source_cleaned, dependent_cleaned;
  void before() {
    initialize(source);
    initialize(dependent);
    source_cleaned($) = 0;
    dependent_cleaned($) = 0;
    source.payload = dependent.payload = this;
    source.destroy_payload = [](void* p) noexcept {
      static_cast<Registration*>(p)->source_cleaned($) = 1;
    };
    dependent.destroy_payload = [](void* p) noexcept {
      auto& fixture = *static_cast<Registration*>(p);
      // Early edge delivery follows source cleanup in its retiring thread;
      // late registration needs the closed gate to acquire that cleanup.
      RL_ASSERT(fixture.source_cleaned($) == 1);
      RL_ASSERT(fixture.dependent_cleaned($) == 0);
      fixture.dependent_cleaned($) = 1;
    };
    if constexpr (WithError) source.set_error(7);
  }
  void thread(unsigned actor) {
    if (actor == 0)
      CompletionState::retire(&source);
    else
      source.add_dependent(CompletionCredit(&dependent));
  }
  void after() {
    RL_ASSERT(dependent_cleaned($) == 1);
    RL_ASSERT(dependent.get_error() == (WithError ? 7 : 0));
    terminal(source);
    terminal(dependent);
  }
};

struct Cascade : rl::test_suite<Cascade, 2> {
  CompletionState dependent;
  rl::var<int> effects[2], cleaned;
  void before() {
    initialize(dependent, 2);
    cleaned($) = 0;
    for (unsigned i = 0; i < 2; ++i) {
      effects[i]($) = 0;
    }
    dependent.payload = this;
    dependent.destroy_payload = [](void* p) noexcept {
      auto& fixture = *static_cast<Cascade*>(p);
      RL_ASSERT(fixture.effects[0]($) == 1 && fixture.effects[1]($) == 1);
      RL_ASSERT(fixture.cleaned($) == 0);
      fixture.cleaned($) = 1;
    };
  }
  void thread(unsigned actor) {
    effects[actor]($) = 1;
    // Project two already-terminal sources onto their actual edge-arrival
    // blocks. Registration/source epilogues have separate full-search fixtures.
    model_deliver_edge(CompletionCredit(&dependent));
  }
  void after() {
    RL_ASSERT(cleaned($) == 1);
    terminal(dependent);
  }
};

int main(int argc, char** argv) {
  if (argc != 2) return 2;
  const std::string_view suite(argv[1]);
  if (suite == "aggregate") return check_model<Aggregate>(argv[1]);
  if (suite == "ready") return check_model<Visibility<false>>(argv[1]);
  if (suite == "wait") return check_model<Visibility<true>>(argv[1]);
  if (suite == "registration") return check_model<Registration<false>>(argv[1]);
  if (suite == "registration-error")
    return check_model<Registration<true>>(argv[1]);
  if (suite == "cascade") return check_model<Cascade>(argv[1]);
  return 2;
}
