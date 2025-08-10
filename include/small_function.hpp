#pragma once
#include <cstddef>
#include <new>
#include <type_traits>
#include <utility>

namespace tp {

template <typename Sig, std::size_t StorageSize = 64>
class small_function;  // primary

template <std::size_t StorageSize>
class small_function<void(), StorageSize> {
 public:
  small_function() noexcept = default;
  small_function(std::nullptr_t) noexcept {}

  small_function(const small_function&) = delete;
  small_function& operator=(const small_function&) = delete;

  small_function(small_function&& other) noexcept {
	move_from(std::move(other));
  }
  small_function& operator=(small_function&& other) noexcept {
	if (this != &other) {
	  reset();
	  move_from(std::move(other));
	}
	return *this;
  }

  template <typename F, typename = std::enable_if_t<
							!std::is_same_v<std::decay_t<F>, small_function>>>
  small_function(F&& f) {
	emplace(std::forward<F>(f));
  }

  ~small_function() { reset(); }

  explicit operator bool() const noexcept { return call_ != nullptr; }
  void operator()() { call_(storage_); }

  void reset() noexcept {
	if (destroy_) {
	  destroy_(storage_, nullptr);
	  destroy_ = nullptr;
	  call_ = nullptr;
	  move_ = nullptr;
	}
  }

  template <typename F>
  void emplace(F&& f) {
	reset();
	using FnT = std::decay_t<F>;
	static_assert(std::is_invocable_r_v<void, FnT&>, "Callable must be void()");
	static_assert(sizeof(FnT) <= StorageSize, "Callable too large");

	new (storage_) FnT(std::forward<F>(f));

	call_ = [](void* s) { (*reinterpret_cast<FnT*>(s))(); };

	move_ = [](void* dst, void* src) {
	  new (dst) FnT(std::move(*reinterpret_cast<FnT*>(src)));
	  reinterpret_cast<FnT*>(src)->~FnT();
	};

	destroy_ = [](void* s, void*) { reinterpret_cast<FnT*>(s)->~FnT(); };
  }

 private:
  using CallFn = void (*)(void*);
  using OpFn = void (*)(void*, void*);

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

  alignas(std::max_align_t) unsigned char storage_[StorageSize];
  CallFn call_ = nullptr;
  OpFn move_ = nullptr;
  OpFn destroy_ = nullptr;
};

}  // namespace tp
