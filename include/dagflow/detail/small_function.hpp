#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

#include <dagflow/config.hpp>
#include <dagflow/detail/runtime_memory.hpp>

namespace dagflow::detail {

template <class Sig, std::size_t StorageSize, std::size_t StorageAlign,
          bool AllowHeap>
class basic_function;

template <class R, class... Args, std::size_t StorageSize,
          std::size_t StorageAlign, bool AllowHeap>
class basic_function<R(Args...), StorageSize, StorageAlign, AllowHeap> {
  static_assert(StorageSize > 0, "Function storage must be nonempty");
  static_assert(StorageAlign > 0 && (StorageAlign & (StorageAlign - 1)) == 0,
                "Function alignment must be a power of two");
  static_assert(!AllowHeap || (StorageSize >= sizeof(void*) &&
                               StorageAlign >= alignof(void*)),
                "small_function storage must fit and align a pointer");

  template <class F>
  static constexpr bool inline_eligible =
      sizeof(F) <= StorageSize && alignof(F) <= StorageAlign &&
      std::is_nothrow_move_constructible_v<F>;

  template <class F>
  static constexpr bool accepts =
      std::is_constructible_v<std::decay_t<F>, F> &&
      std::is_invocable_r_v<R, std::decay_t<F>&, Args...> &&
      std::is_nothrow_destructible_v<std::decay_t<F>> &&
      (AllowHeap || inline_eligible<std::decay_t<F>>);

 public:
  using result_type = R;

  // User-provided: value-initialization must not zero the raw buffer.
  basic_function() noexcept {}
  basic_function(std::nullptr_t) noexcept {}
  basic_function(const basic_function&) = delete;
  basic_function& operator=(const basic_function&) = delete;
  basic_function(basic_function&& other) noexcept { move_from(other); }
  basic_function& operator=(basic_function&& other) noexcept {
    if (this != &other) {
      reset();
      move_from(other);
    }
    return *this;
  }
  template <class F>
    requires(!std::is_same_v<std::decay_t<F>, basic_function> && accepts<F>)
  basic_function(F&& function) {
    construct<std::decay_t<F>>(std::forward<F>(function));
  }
  ~basic_function() { reset(); }

  explicit operator bool() const noexcept { return ops_ != nullptr; }
  // A nonempty, invocable target is a precondition; no bad_function_call path.
  R operator()(Args... args) {
    return ops_->call(storage_, std::forward<Args>(args)...);
  }
  void reset() noexcept {
    if (const auto* ops = std::exchange(ops_, nullptr)) ops->destroy(storage_);
  }

  // Construct before commit. If allocation/construction fails, the previous
  // target is retained. Commit is a nonthrowing wrapper move.
  template <class F>
    requires(!std::is_same_v<std::decay_t<F>, basic_function> && accepts<F>)
  void emplace(F&& function) {
    basic_function replacement(std::forward<F>(function));
    *this = std::move(replacement);
  }

 private:
  using CallFn = R (*)(void*, Args&&...);
  using MoveFn = void (*)(void*, void*) noexcept;
  using DestroyFn = void (*)(void*) noexcept;
  struct Ops {
    CallFn call;
    MoveFn move;
    DestroyFn destroy;
  };

  // Only for an already constructed F (or F* in the spilled representation).
  template <class F>
  static F* ptr(void* storage) noexcept {
    return std::launder(reinterpret_cast<F*>(storage));
  }
  template <class F>
  static F* target(void* storage) noexcept {
    if constexpr (inline_eligible<F>)
      return ptr<F>(storage);
    else
      return *ptr<F*>(storage);
  }
  template <class F>
  static R call(void* storage, Args&&... args) {
    if constexpr (std::is_void_v<R>)
      std::invoke(*target<F>(storage), std::forward<Args>(args)...);
    else
      return std::invoke(*target<F>(storage), std::forward<Args>(args)...);
  }
  template <class F>
  static void move(void* destination, void* source) noexcept {
    if constexpr (inline_eligible<F>) {
      std::construct_at(reinterpret_cast<F*>(destination),
                        std::move(*ptr<F>(source)));
      std::destroy_at(ptr<F>(source));
    } else {
      std::construct_at(reinterpret_cast<F**>(destination), *ptr<F*>(source));
      std::destroy_at(ptr<F*>(source));
    }
  }
  template <class F>
  static void destroy(void* storage) noexcept {
    if constexpr (inline_eligible<F>) {
      std::destroy_at(ptr<F>(storage));
    } else {
      F* object = *ptr<F*>(storage);
      std::destroy_at(object);
      deallocate_bytes(object, alignof(F));
      std::destroy_at(ptr<F*>(storage));
    }
  }
  template <class F>
  inline static constexpr Ops operations{&call<F>, &move<F>, &destroy<F>};

  template <class F, class U>
  void construct(U&& function) {
    if constexpr (inline_eligible<F>) {
      std::construct_at(reinterpret_cast<F*>(storage_),
                        std::forward<U>(function));
    } else {
      auto* object = static_cast<F*>(allocate_bytes(sizeof(F), alignof(F)));
      try {
        std::construct_at(object, std::forward<U>(function));
      } catch (...) {
        deallocate_bytes(object, alignof(F));
        throw;
      }
      std::construct_at(reinterpret_cast<F**>(storage_), object);
    }
    ops_ = &operations<F>;
  }
  void move_from(basic_function& other) noexcept {
    if (const auto* ops = std::exchange(other.ops_, nullptr)) {
      ops->move(storage_, other.storage_);
      ops_ = ops;
    }
  }

  alignas(StorageAlign) std::byte storage_[StorageSize];
  const Ops* ops_{nullptr};
};
}  // namespace dagflow::detail

namespace dagflow {

/// Move-only callable with SBO; oversized, over-aligned or potentially throwing
/// move targets use runtime_memory. Wrapper moves are always noexcept.
/// StorageSize/StorageAlign must accommodate a pointer. Target destructors must
/// be noexcept. Empty invocation is a precondition violation.
template <class Sig, std::size_t StorageSize = DAGFLOW_DEFAULT_FUNCTION_STORAGE_SIZE,
          std::size_t StorageAlign = alignof(std::max_align_t)>
using small_function =
    detail::basic_function<Sig, StorageSize, StorageAlign, true>;

}  // namespace dagflow
