#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

// Stateless bounded exploration. Exactly one actor runs between checkpoints.
// Every alternative runnable actor is replayed in a fresh runtime fixture.
// Checkpoints must not leave a lock held that another scheduled actor needs.
// This explores SC operation segments, NOT weak-memory reorderings.
namespace dagflow::test::schedule {
using Trace = std::vector<unsigned>;
inline thread_local unsigned actor_id = 0;
inline bool matched_suite = false;

inline void verify_selection() {
  if (std::getenv("DAGFLOW_SCHEDULE_SUITE") && !matched_suite) {
    std::fprintf(stderr, "unknown DAGFLOW_SCHEDULE_SUITE\n");
    std::exit(2);
  }
}

class Controller {
  enum State { Starting, Parked, Running, BlockingTail, Done };
  std::string name_;
  Trace prefix_, chosen_;
  std::vector<Trace>& pending_;
  std::vector<State> states_;
  std::vector<std::string_view> labels_;
  mutable std::recursive_mutex mutex_;
  std::condition_variable_any changed_;
  bool replay_;

  template <class Predicate>
  void wait(std::unique_lock<std::recursive_mutex>& lock, Predicate predicate) {
    if (!changed_.wait_for(lock, std::chrono::seconds(5), predicate))
      fail("actor did not reach its next checkpoint", true);
  }

 public:
  Controller(std::string_view name, unsigned actors, Trace prefix,
             std::vector<Trace>& pending, bool replay)
      : name_(name),
        prefix_(std::move(prefix)),
        pending_(pending),
        states_(actors, Starting),
        labels_(actors, "start"),
        replay_(replay) {}

  [[noreturn]] void fail(std::string_view message, bool timeout = false) const {
    std::lock_guard lock(mutex_);
    std::fprintf(stderr, "%s: suite=%s trace=",
                 timeout ? "schedule timeout" : "schedule property failed",
                 name_.c_str());
    for (std::size_t i = 0; i < chosen_.size(); ++i)
      std::fprintf(stderr, "%s%u", i ? "," : "", chosen_[i]);
    std::fprintf(stderr, " invariant=%.*s\n", int(message.size()),
                 message.data());
    std::fflush(stderr);
    // A broken protocol must not hang during thread/pool teardown. The runner
    // distinguishes assertion failures (86) from watchdog failures (87).
    std::_Exit(timeout ? 87 : 86);
  }
  void require(bool condition, std::string_view message) const {
    if (!condition) fail(message);
  }
  void checkpoint(unsigned actor, std::string_view label) {
    std::unique_lock lock(mutex_);
    labels_[actor] = label;
    states_[actor] = Parked;
    changed_.notify_all();
    wait(lock, [&] { return states_[actor] == Running; });
  }
  void checkpoint(std::string_view label) { checkpoint(actor_id, label); }
  // Only for a terminal blocking wait: no more checkpoints in this actor.
  // Other actors must be allowed to run to satisfy the wait. CV internals in
  // this tail are not enumerated; the runtime mutex supplies their ordering.
  void blocking_tail(unsigned actor) {
    std::lock_guard lock(mutex_);
    states_[actor] = BlockingTail;
    changed_.notify_all();
  }
  void complete(unsigned actor) {
    std::lock_guard lock(mutex_);
    states_[actor] = Done;
    changed_.notify_all();
  }
  template <class Function>
  std::thread actor(unsigned id, Function function) {
    return std::thread([this, id, function = std::move(function)]() mutable {
      actor_id = id;
      checkpoint(id, "start");
      try {
        function();
      } catch (const std::exception& error) {
        fail(error.what());
      } catch (...) {
        fail("unexpected actor exception");
      }
      complete(id);
    });
  }
  void drive() {
    std::unique_lock lock(mutex_);
    wait(lock, [&] {
      for (auto state : states_)
        if (state == Starting) return false;
      return true;
    });
    for (;;) {
      std::vector<unsigned> enabled;
      for (unsigned i = 0; i < states_.size(); ++i)
        if (states_[i] == Parked) enabled.push_back(i);
      if (enabled.empty()) {
        wait(lock, [&] {
          for (auto state : states_)
            if (state != Done) return false;
          return true;
        });
        break;
      }
      require(chosen_.size() < 64, "schedule exceeds declared 64-step bound");
      unsigned selected = enabled.front();
      if (chosen_.size() < prefix_.size()) {
        selected = prefix_[chosen_.size()];
        require(selected < states_.size() && states_[selected] == Parked,
                "replay selects an unavailable actor");
      } else if (!replay_) {
        for (auto alternative : enabled) {
          if (alternative == selected) continue;
          auto branch = chosen_;
          branch.push_back(alternative);
          pending_.push_back(std::move(branch));
        }
      }
      chosen_.push_back(selected);
      states_[selected] = Running;
      changed_.notify_all();
      wait(lock, [&] { return states_[selected] != Running; });
    }
    require(chosen_.size() >= prefix_.size(), "replay has unused decisions");
  }
};

template <class Fixture>
void explore(std::string_view name, unsigned actors, Fixture fixture) {
  const char* selected = std::getenv("DAGFLOW_SCHEDULE_SUITE");
  if (selected && name != selected) return;
  matched_suite = true;
  const char* replay = std::getenv("DAGFLOW_SCHEDULE_TRACE");
  if (replay && !selected) {
    std::fprintf(stderr,
                 "DAGFLOW_SCHEDULE_TRACE requires DAGFLOW_SCHEDULE_SUITE\n");
    std::exit(2);
  }
  Trace prefix;
  if (replay) {
    std::istringstream input(replay);
    std::string word;
    while (std::getline(input, word, ',')) {
      std::size_t end = 0;
      const auto actor = std::stoul(word, &end);
      if (end != word.size() || actor >= actors) std::exit(2);
      prefix.push_back(static_cast<unsigned>(actor));
    }
  }
  std::vector<Trace> pending{prefix};
  std::size_t histories = 0;
  while (!pending.empty()) {
    prefix = std::move(pending.back());
    pending.pop_back();
    Controller controller(name, actors, std::move(prefix), pending, replay);
    controller.require(histories < 20000,
                       "exploration exceeds 20000 histories");
    fixture(controller);
    ++histories;
  }
  std::printf(
      "schedule suite=%.*s histories=%zu exhaustive-at-checkpoints=%s\n",
      int(name.size()), name.data(), histories, replay ? "replay" : "yes");
}
}  // namespace dagflow::test::schedule
