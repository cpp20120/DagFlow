#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>
#include <type_traits>

#include <dagflow/detail/runtime_memory.hpp>

namespace dagflow {

class Handle;
namespace detail {
struct CompletionState;

/// One move-only obligation to complete an operation. A live credit protects
/// both operation storage and the authority to fork more credits (C-01..03).
/// Move transfers that obligation; destruction retires it exactly once.
class CompletionCredit {
 public:
  CompletionCredit() noexcept = default;
  CompletionCredit(const CompletionCredit&) = delete;
  CompletionCredit& operator=(const CompletionCredit&) = delete;
  CompletionCredit(CompletionCredit&& other) noexcept
      : state_(std::exchange(other.state_, nullptr)) {}
  CompletionCredit& operator=(CompletionCredit&& other) noexcept {
    if (this != &other) {
      finish();
      state_ = std::exchange(other.state_, nullptr);
    }
    return *this;
  }
  ~CompletionCredit() { finish(); }

  /// Creates the initial publication/sentinel credit.
  [[nodiscard]] static CompletionCredit create();
  /// The operation exclusively owns payload until the final credit retires.
  /// Payload destruction precedes publication of Handle::ready().
  template <class T, class Deleter>
  [[nodiscard]] static CompletionCredit create(std::unique_ptr<T, Deleter> payload) {
    static_assert(std::is_empty_v<Deleter> && std::is_nothrow_default_constructible_v<Deleter>);
    auto credit = create_erased(payload.get(), [](void* value) noexcept {
      Deleter{}(static_cast<T*>(value));
    });
    payload.release();
    return credit;
  }
  /// Requires this live credit. No increment through a completed Handle.
  [[nodiscard]] CompletionCredit fork() const noexcept;
  [[nodiscard]] Handle handle() const noexcept;
  explicit operator bool() const noexcept { return state_ != nullptr; }
  void fail(std::exception_ptr error) const noexcept;
  void finish() noexcept;

 private:
  explicit CompletionCredit(CompletionState* state) noexcept : state_(state) {}
  static CompletionCredit create_erased(void* payload,
                                       void (*destroy)(void*) noexcept);
  CompletionState* state_{};
  friend struct CompletionState;
};

/// Storage references and liveness credits are separate counts. Only Handle
/// copies and live credits own storage; borrowed pointers never acquire it.
struct CompletionState {
  std::atomic<std::size_t> references{1};
  std::atomic<std::size_t> credits{1};
  std::atomic<bool> ready{false};
  std::atomic<bool> has_error{false};
  enum class Dependents : unsigned char { unused, registered, closed };
  std::atomic<Dependents> dependent_gate{Dependents::unused};
  // Cold paths only: error capture and combine's dependent registration.
  std::mutex mutex;
  std::exception_ptr error;
  std::vector<CompletionCredit, RuntimeAllocator<CompletionCredit>> dependents;
  // A terminal worklist carries the retiring credit's storage reference.
  CompletionState* next_terminal{};
  void* payload{};
  void (*destroy_payload)(void*) noexcept{};

  void retain() noexcept;
  void release() noexcept;
  void set_error(std::exception_ptr failure) noexcept;
  [[nodiscard]] std::exception_ptr get_error() noexcept;
  void wait();
  /// Consumes a dependent credit, either storing it or retiring immediately.
  void add_dependent(CompletionCredit dependent);
  /// Consumes one credit AND its storage reference; iterative for deep joins.
  static void retire(CompletionState* state) noexcept;

  ~CompletionState();
  friend class CompletionCredit;
};
}  // namespace detail

/// Copyable observation of completion/error, not ownership of a task or graph.
/// A completed Handle can outlive the Pool. Move transfers a storage reference;
/// copy retains one. It never grants authority to create new work.
class Handle {
 public:
  Handle() noexcept = default;
  Handle(const Handle& other) noexcept : state_(other.state_) {
    if (state_) { state_->retain();
    }
  }
  Handle(Handle&& other) noexcept
      : state_(std::exchange(other.state_, nullptr)) {}
  Handle& operator=(const Handle& other) noexcept {
    Handle copy(other);
    swap(copy);
    return *this;
  }
  Handle& operator=(Handle&& other) noexcept {
    if (this != &other) {
      Handle moved(std::move(other));
      swap(moved);
    }
    return *this;
  }
  ~Handle() {
    if (state_) state_->release();
  }
  void swap(Handle& other) noexcept { std::swap(state_, other.state_); }
  [[nodiscard]] bool valid() const noexcept { return state_ != nullptr; }
  explicit operator bool() const noexcept { return valid(); }
  [[nodiscard]] bool ready() const noexcept {
    return !state_ || state_->ready.load(std::memory_order_acquire);
  }
  /// Call after Pool::wait(); waiting itself does not rethrow task failures.
  void rethrow_if_failed() const {
    if (state_)
      if (auto error = state_->get_error()) std::rethrow_exception(error);
  }

 private:
  explicit Handle(detail::CompletionState* state) noexcept : state_(state) {
    if (state_) state_->retain();
  }
  detail::CompletionState* state_{};
  friend class Pool;
  friend class detail::CompletionCredit;
};

}  // namespace dagflow
