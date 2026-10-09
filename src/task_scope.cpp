#include <dagflow/task_scope.hpp>

#include <mutex>
#include <stdexcept>

namespace dagflow {

struct TaskScope::State {
  explicit State(Pool& owner)
      : owner(owner), root(detail::CompletionCredit::create()),
        completion(root.handle()) {}

  Pool& owner;
  static constexpr uint64_t closed_bit = uint64_t{1} << 63;
  static constexpr uint64_t cancelled_bit = uint64_t{1} << 62;
  static constexpr uint64_t publisher_mask = cancelled_bit - 1;
  // Reservations protect access to root, not task lifetime. After close the
  // closer (zero reservations) or last publisher alone retires the sentinel.
  std::atomic<uint64_t> admission{0};
  detail::CompletionCredit root;
  const Handle completion;
  mutable std::mutex error_mutex;
  std::exception_ptr error;
};

thread_local TaskScope::ActiveFrame* TaskScope::ActiveFrame::current = nullptr;

TaskScope::ActiveFrame::ActiveFrame(State& state) noexcept
    : state(&state), previous(current) { current = this; }
TaskScope::ActiveFrame::~ActiveFrame() { current = previous; }

bool TaskScope::active_here(const State& state) noexcept {
  for (auto* frame = ActiveFrame::current; frame; frame = frame->previous)
    if (frame->state == &state) return true;
  return false;
}

TaskScope::TaskScope(Pool& pool) : state_(detail::make_owned<State>(pool)) {}

TaskScope::~TaskScope() noexcept {
  // Destruction from one's own work cannot satisfy structured join.
  if (active_here(*state_)) std::terminate();
  close_admission(*state_);
  state_->owner.wait(state_->completion);
  // All user callbacks, captures and publishing borrows have ended. Remaining
  // pool epilogues may touch pool/completion bookkeeping, never this state.
}

detail::CompletionCredit TaskScope::reserve(
    State& state, const detail::CompletionCredit* parent) {
  auto admission = state.admission.load(std::memory_order_acquire);
  if (parent) {
    // This observation is the child admission point. The live parent protects
    // state and authority to fork even if close/cancel retires root afterward.
    if (admission & State::cancelled_bit) return {};
    return parent->fork();
  }
  for (;;) {
    if (admission & State::closed_bit) return {};
    if ((admission & State::publisher_mask) == State::publisher_mask)
      throw std::overflow_error("TaskScope: too many concurrent publishers");
    if (state.admission.compare_exchange_weak(admission, admission + 1,
                                             std::memory_order_acquire,
                                             std::memory_order_acquire))
      break;
  }
  auto publication = state.root.fork();  // noexcept; reservation still owns root access.
  const auto previous = state.admission.fetch_sub(1, std::memory_order_acq_rel);
  if ((previous & State::closed_bit) && (previous & State::publisher_mask) == 1)
    state.root.finish();
  return publication;
}

void TaskScope::close_admission(State& state, bool cancel) noexcept {
  const auto flags = State::closed_bit | (cancel ? State::cancelled_bit : 0);
  const auto previous = state.admission.fetch_or(flags, std::memory_order_acq_rel);
  if (!(previous & State::closed_bit) && !(previous & State::publisher_mask))
    state.root.finish();
  // With active reservations, their last release retires root. close never
  // waits for publishers; their own credits keep accepted work alive.
}

void TaskScope::request_cancel(State& state) noexcept {
  close_admission(state, true);
}

void TaskScope::record_error(State& state,
                             const detail::CompletionCredit& credit,
                             std::exception_ptr failure) noexcept {
  std::exception_ptr first;
  {
    std::lock_guard lock(state.error_mutex);
    if (!state.error) state.error = std::move(failure);
    first = state.error;
    // last_error() must not expose a failure before its cancellation request.
    // Our live credit prevents sentinel retirement from completing the scope.
    request_cancel(state);
  }
  // The caller's credit protects state and prevents readiness until this
  // first error has reached the common completion state.
  credit.fail(std::move(first));
}

bool TaskScope::is_cancelled(const State& state) noexcept {
  return (state.admission.load(std::memory_order_acquire) & State::cancelled_bit) != 0;
}
Pool& TaskScope::pool(State& state) noexcept { return state.owner; }

Handle TaskScope::close() noexcept {
  auto result = state_->completion;
  close_admission(*state_);
  return result;
}
void TaskScope::wait() {
  if (active_here(*state_))
    throw std::logic_error("TaskScope: cannot join its own active work");
  auto handle = close();
  state_->owner.wait(handle);
}
void TaskScope::join() {
  wait();
  state_->completion.rethrow_if_failed();
}
void TaskScope::cancel() noexcept { request_cancel(*state_); }
bool TaskScope::cancelled() const noexcept { return is_cancelled(*state_); }
std::exception_ptr TaskScope::last_error() const noexcept {
  std::lock_guard lock(state_->error_mutex);
  return state_->error;
}
Handle TaskScope::completion() const noexcept { return state_->completion; }

bool TaskScope::Context::cancelled() const noexcept { return is_cancelled(*state_); }
void TaskScope::Context::cancel() noexcept { request_cancel(*state_); }

}  // namespace dagflow
