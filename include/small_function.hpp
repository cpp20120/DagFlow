#pragma once
/**
 * @file small_function.hpp
 * @brief Small, move-only function wrapper with small-buffer optimization for
 * signature `void()`.
 *
 * @details
 * This is a compact function wrapper specialized for `void()` callables.
 * It is move-only by design, which allows a light-weight implementation with:
 *  - inline storage (Small Buffer Optimization) of size `StorageSize`;
 *  - three function pointers for call / move / destroy;
 *  - no dynamic allocation when the callable fits into the inline buffer.
 *
 * Limitations:
 *  - Only the exact signature `void()` is supported by this specialization.
 *  - The stored callable type must be invocable as `void(FnT&)`.
 *  - The size of the callable must be <= StorageSize, otherwise a static_assert
 * fires.
 */

#include <cstddef>
#include <new>
#include <type_traits>
#include <utility>

namespace dagflow {
/**
 * @brief Primary template declaration (unspecialized).
 */
template <typename Sig, std::size_t StorageSize = 64>
class small_function;  // primary
/**
 * @brief Small, move-only function wrapper with small-buffer optimization
 * @brief Specialization for signature `void()`.
 *
 * @tparam StorageSize Size of the inline storage in bytes.
 */
template <std::size_t StorageSize>
class small_function<void(), StorageSize> {
 public:
  /// Default-construct an empty function.
  small_function() noexcept = default;
  /// Construct an empty function from nullptr for symmetry.
  small_function(std::nullptr_t) noexcept {}
  small_function(const small_function&) = delete;
  small_function& operator=(const small_function&) = delete;
  /// Move-construct from another small_function, stealing its callable.
  small_function(small_function&& other) noexcept {
	move_from(std::move(other));
  }
  /// Move-assign from another small_function, destroying current callable
  /// first.
  small_function& operator=(small_function&& other) noexcept {
	if (this != &other) {
	  reset();
	  move_from(std::move(other));
	}
	return *this;
  }
  /**
   * @brief Construct from any callable F that is not small_function itself.
   * @tparam F Callable type.
   * @param f  Callable instance, perfectly forwarded.
   */
  template <typename F, typename = std::enable_if_t<
							!std::is_same_v<std::decay_t<F>, small_function>>>
  small_function(F&& f) {
	emplace(std::forward<F>(f));
  }
  /// Destroy and release any stored callable.
  ~small_function() { reset(); }
  /// @return true if a callable is currently stored.
  explicit operator bool() const noexcept { return call_ != nullptr; }
  /// Invoke the stored callable. Behavior is undefined if empty.
  void operator()() { call_(storage_); }
  /// Destroy the stored callable and make this wrapper empty.
  void reset() noexcept {
	if (destroy_) {
	  destroy_(storage_, nullptr);
	  destroy_ = nullptr;
	  call_ = nullptr;
	  move_ = nullptr;
	}
  }
  /**
   * @brief In-place construct a callable into the inline storage.
   * @tparam F Callable type.
   * @param f  Callable instance to store.
   *
   * Destroys any currently stored callable first. The callable must fit
   * into the inline storage and be invocable as `void()`.
   */
  template <typename F>
  void emplace(F&& f) {
	reset();
	using FnT = std::decay_t<F>;
	static_assert(std::is_invocable_r_v<void, FnT&>, "Callable must be void()");
	static_assert(sizeof(FnT) <= StorageSize,
				  "Callable too large for small_function");

	new (storage_) FnT(std::forward<F>(f));

	call_ = [](void* s) { (*reinterpret_cast<FnT*>(s))(); };

	move_ = [](void* dst, void* src) {
	  new (dst) FnT(std::move(*reinterpret_cast<FnT*>(src)));
	  reinterpret_cast<FnT*>(src)->~FnT();
	};

	destroy_ = [](void* s, void*) { reinterpret_cast<FnT*>(s)->~FnT(); };
  }

 private:
  /// Type of the call thunk.
  using CallFn = void (*)(void*);
  /// Type of the move/destroy thunk.
  using OpFn = void (*)(void*, void*);
  /// Helper to steal the callable from another instance.
  void move_from(small_function&& other) noexcept {
	if (other.call_) {
	  move_ = other.move_;
	  destroy_ = other.destroy_;
	  call_ = other.call_;
	  move_(storage_, other.storage_);
	  other.call_ = nullptr;
	  other.move_ = nullptr;
	  other.destroy_ = nullptr;
	}
  }
  /// Inline storage buffer properly aligned for any type storable here.
  alignas(std::max_align_t) unsigned char storage_[StorageSize];
  /// Call thunk pointer or nullptr if empty.
  CallFn call_ = nullptr;
  /// Move thunk pointer or nullptr if empty.
  OpFn move_ = nullptr;
  /// Destroy thunk pointer or nullptr if empty.
  OpFn destroy_ = nullptr;
};

}  // namespace tp
