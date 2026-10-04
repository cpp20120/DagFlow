#pragma once

#include <atomic>
#include <exception>
#include <optional>
#include <type_traits>
#include <utility>

#include "dagflow/detail/runtime_memory.hpp"
#include "dagflow/thread_pool.hpp"

namespace dagflow {

/// One-shot structured task group. The pool must outlive the scope.
/// External spawn/close/cancel/wait may race while this object remains alive.
/// Destruction closes admission and joins; external callers must stop using the
/// scope object before destruction. Children use Context, not a captured scope.
class TaskScope {
  struct State;

 public:
  class Context {
   public:
    Context(const Context&) = delete;
    Context& operator=(const Context&) = delete;
    Context(Context&&) = delete;
    Context& operator=(Context&&) = delete;

    /// Valid only during this callback. A child may spawn descendants after
    /// external admission closes, but not after cancellation.
    template <class F>
    bool spawn(F&& function, SubmitOptions options = {}) {
      auto publication = reserve(*state_, &credit_);
      if (!publication) return false;
      return publish(*state_, std::move(publication),
                     std::forward<F>(function), options);
    }
    template <class F>
    bool submit(F&& function, SubmitOptions options = {}) {
      return spawn(std::forward<F>(function), options);
    }
    [[nodiscard]] bool cancelled() const noexcept;
    void cancel() noexcept;

   private:
    Context(State& state, const detail::CompletionCredit& credit) noexcept
        : state_(&state), credit_(credit) {}
    State* state_;
    const detail::CompletionCredit& credit_;
    friend class TaskScope;
  };

  explicit TaskScope(Pool& pool);
  ~TaskScope() noexcept;
  TaskScope(const TaskScope&) = delete;
  TaskScope& operator=(const TaskScope&) = delete;
  TaskScope(TaskScope&&) = delete;
  TaskScope& operator=(TaskScope&&) = delete;

  /// Returns false if admission is closed/cancelled. Construction/publication
  /// failure cancels the scope, records its first error and rethrows.
  /// The callback accepts Context& (for child spawning) or no arguments.
  template <class F>
  bool spawn(F&& function, SubmitOptions options = {}) {
    auto publication = reserve(*state_, nullptr);
    if (!publication) return false;
    return publish(*state_, std::move(publication),
                   std::forward<F>(function), options);
  }
  template <class F>
  bool submit(F&& function, SubmitOptions options = {}) {
    return spawn(std::forward<F>(function), options);
  }

  /// Close external admission, retire its sentinel, and return an observer.
  /// Existing children retain authority to spawn descendants until cancelled.
  Handle close() noexcept;
  /// Close and wait without rethrowing task errors. Self-join is rejected.
  void wait();
  /// Close, wait, then rethrow the first task/publication error.
  void join();
  /// Close admission and request cooperative cancellation. No exception alone.
  void cancel() noexcept;
  [[nodiscard]] bool cancelled() const noexcept;
  [[nodiscard]] std::exception_ptr last_error() const noexcept;
  /// Observation grants no authority to spawn. Until close/cancel, the open
  /// external admission sentinel keeps this completion unready.
  [[nodiscard]] Handle completion() const noexcept;

 private:
  // Tracks callbacks, publication and callable destruction on this thread,
  // including nested scopes/cooperative helping, to reject self/ancestor join.
  struct ActiveFrame {
    explicit ActiveFrame(State& state) noexcept;
    ~ActiveFrame();
    State* state;
    ActiveFrame* previous;
    static thread_local ActiveFrame* current;
  };

  static detail::CompletionCredit reserve(
      State& state, const detail::CompletionCredit* parent);
  static void record_error(State& state, const detail::CompletionCredit& credit,
                           std::exception_ptr failure) noexcept;
  static bool is_cancelled(const State& state) noexcept;
  static Pool& pool(State& state) noexcept;
  static void request_cancel(State& state) noexcept;
  static void close_admission(State& state, bool cancel = false) noexcept;
  static bool active_here(const State& state) noexcept;

  template <class F>
  static void invoke(State& state, detail::CompletionCredit& credit, F& function) {
    ActiveFrame frame(state);
    if (is_cancelled(state)) return;
    Context context(state, credit);
    try {
      if constexpr (std::is_invocable_v<F&, Context&>)
        function(context);
      else
        function();
    } catch (...) {
      record_error(state, credit, std::current_exception());
    }
  }

  template <class F>
  struct ScopedTask {
    State* state;
    std::optional<F> function;

    template <class U>
    ScopedTask(State& owner, U&& f)
        : state(&owner), function(std::in_place, std::forward<U>(f)) {}
    ScopedTask(const ScopedTask&) = delete;
    ScopedTask(ScopedTask&&) noexcept(std::is_nothrow_move_constructible_v<F>) = default;
    ~ScopedTask() noexcept {
      // The executor still holds its credit while destroying this target.
      // Keep self-join detection active during user capture cleanup as well.
      ActiveFrame frame(*state);
      function.reset();
    }
    void operator()(detail::CompletionCredit& credit) {
      invoke(*state, credit, *function);
    }
  };

  template <class F>
  static bool publish(State& state, detail::CompletionCredit publication,
                      F&& function, SubmitOptions options) {
    using Fn = std::decay_t<F>;
    static_assert(std::is_invocable_v<Fn&, Context&> || std::is_invocable_v<Fn&>,
                  "Scope callback must accept Context& or no arguments");
    static_assert(std::is_nothrow_destructible_v<Fn>,
                  "Scope callback destruction must not throw");
    ActiveFrame publishing(state);
    try {
      // Keep publication alive until task construction has cleaned up the
      // concrete callable, including when packet allocation/construction fails.
      pool(state).enqueue(ScopedTask<Fn>{state, std::forward<F>(function)},
                          options, publication.fork());
    } catch (...) {
      record_error(state, publication, std::current_exception());
      throw;
    }
    return true;
    // Local wrappers/frames die first; publication retires last. No subsequent
    // scope-state access is permitted by this publishing path.
  }

  detail::OwnedObject<State> state_;
};

}  // namespace dagflow
