#pragma once
#include <array>
#include <cstddef>
#include <new>
#include <type_traits>
#include <utility>
#include <vector>

namespace tp {

template <typename T, std::size_t N = 4>
class SmallVec {
 public:
  SmallVec() = default;
  SmallVec(const SmallVec&) = delete;
  SmallVec& operator=(const SmallVec&) = delete;

  SmallVec(SmallVec&& other) noexcept { move_from(std::move(other)); }
  SmallVec& operator=(SmallVec&& other) noexcept {
	if (this != &other) {
	  destroy_sso();
	  hv_.clear();
	  spilled_ = false;
	  sz_ = 0;
	  move_from(std::move(other));
	}
	return *this;
  }

  ~SmallVec() { destroy_sso(); }

  void push_back(const T& v) {
	if (use_vec()) {
	  hv_.push_back(v);
	  return;
	}
	if (sz_ < N) {
	  ::new (sso_ptr(sz_)) T(v);
	  ++sz_;
	  return;
	}
	spill_to_vec();
	hv_.push_back(v);
  }

  void push_back(T&& v) {
	if (use_vec()) {
	  hv_.push_back(std::move(v));
	  return;
	}
	if (sz_ < N) {
	  ::new (sso_ptr(sz_)) T(std::move(v));
	  ++sz_;
	  return;
	}
	spill_to_vec();
	hv_.push_back(std::move(v));
  }

  std::size_t size() const noexcept { return use_vec() ? hv_.size() : sz_; }
  bool empty() const noexcept { return size() == 0; }

  T* data() noexcept {
	return use_vec() ? hv_.data() : reinterpret_cast<T*>(sso_[0].data);
  }
  const T* data() const noexcept {
	return use_vec() ? hv_.data() : reinterpret_cast<const T*>(sso_[0].data);
  }

  T& operator[](std::size_t i) noexcept { return data()[i]; }
  const T& operator[](std::size_t i) const noexcept { return data()[i]; }

  T* begin() noexcept { return data(); }
  T* end() noexcept { return data() + size(); }
  const T* begin() const noexcept { return data(); }
  const T* end() const noexcept { return data() + size(); }

  void clear() noexcept {
	if (!use_vec()) {
	  destroy_sso();
	  sz_ = 0;
	} else {
	  hv_.clear();
	}
  }

 private:
  struct alignas(alignof(T)) Slot {
	std::byte data[sizeof(T)];
  };

  bool use_vec() const noexcept { return spilled_; }

  void* sso_ptr(std::size_t i) noexcept {
	return static_cast<void*>(sso_[i].data);
  }
  const void* sso_ptr(std::size_t i) const noexcept {
	return static_cast<const void*>(sso_[i].data);
  }

  void spill_to_vec() {
	if (spilled_) return;
	hv_.reserve(N * 2);
	for (std::size_t i = 0; i < sz_; ++i) {
	  T* src = reinterpret_cast<T*>(sso_ptr(i));
	  hv_.push_back(std::move(*src));
	  src->~T();
	}
	sz_ = 0;
	spilled_ = true;
  }

  void destroy_sso() noexcept {
	if (!spilled_) {
	  for (std::size_t i = 0; i < sz_; ++i) {
		T* ptr = reinterpret_cast<T*>(sso_ptr(i));
		ptr->~T();
	  }
	}
  }

  void move_from(SmallVec&& other) {
	if (other.spilled_) {
	  hv_ = std::move(other.hv_);
	  spilled_ = true;
	  other.hv_.clear();
	  other.spilled_ = false;
	  other.sz_ = 0;
	} else {
	  for (std::size_t i = 0; i < other.sz_; ++i) {
		::new (sso_ptr(i))
			T(std::move(*reinterpret_cast<T*>(other.sso_ptr(i))));
		reinterpret_cast<T*>(other.sso_ptr(i))->~T();
	  }
	  sz_ = other.sz_;
	  spilled_ = false;
	  other.sz_ = 0;
	}
  }

  std::array<Slot, N> sso_{};
  std::vector<T> hv_{};
  std::size_t sz_{0};
  bool spilled_{false};
};

}  // namespace tp
