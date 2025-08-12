#pragma once
/**
 * @file small_vec.hpp
 * @brief Small vector with inline storage (SSO) that spills to the heap when
 * size exceeds N.
 *
 * @details
 * For up to N elements this container stores objects inside an inline buffer
 * without any heap allocations. When the size would exceed N, the container
 * performs a one-time spill: it moves the existing inline elements into an
 * owned std::vector, and from that point on uses the vector for storage
 * exclusively. Destructors are invoked appropriately in both SSO and spilled
 * modes.
 *
 * Complexity:
 *  - push_back: O(1) amortized; O(size) during the first spill due to moves.
 *  - operator[], data(), begin(), end(): O(1).
 *
 * Exception safety:
 *  - Strong guarantees during moves where applicable; inline elements are
 * destroyed only after successful move into the heap vector.
 */

#include <array>
#include <cstddef>
#include <new>
#include <type_traits>
#include <utility>
#include <vector>

namespace tp {
/**
 * @brief Small vector with inline storage (SSO) that spills to the heap when
 * size exceeds N
 * @tparam T element type pameter
 * @tparam N size of element for sso
 */
template <typename T, std::size_t N = 4>
class SmallVec {
 public:
  /// Construct an empty container.
  SmallVec() = default;
  SmallVec(const SmallVec&) = delete;
  SmallVec& operator=(const SmallVec&) = delete;
  /// Move-construct by stealing storage from @p other.
  SmallVec(SmallVec&& other) noexcept { move_from(std::move(other)); }
  /// Move-assign by destroying current elements and stealing from @p other.
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
  /// Destroy the container and all contained elements.
  ~SmallVec() { destroy_sso(); }
  /**
   * @brief Append a copy of @p v at the end.
   * @param v Element to copy.
   */
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
  /**
   * @brief Append a moved value @p v at the end.
   * @param v Element to move from.
   */
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
  /// @return Current number of elements.
  std::size_t size() const noexcept { return use_vec() ? hv_.size() : sz_; }
  /// @return true if the container is empty.
  bool empty() const noexcept { return size() == 0; }
  /// @return Pointer to the contiguous storage for read/write access.
  T* data() noexcept {
	return use_vec() ? hv_.data() : reinterpret_cast<T*>(sso_[0].data);
  }
  /// @return Pointer to the contiguous storage (const).
  const T* data() const noexcept {
	return use_vec() ? hv_.data() : reinterpret_cast<const T*>(sso_[0].data);
  }
  /// Random-access element reference.
  T& operator[](std::size_t i) noexcept { return data()[i]; }
  /// Random-access const element reference.
  const T& operator[](std::size_t i) const noexcept { return data()[i]; }

  /// @return Iterator to the first element.
  T* begin() noexcept { return data(); }
  /// @return Iterator one past the last element.
  T* end() noexcept { return data() + size(); }
  /// @return Const iterator to the first element.
  const T* begin() const noexcept { return data(); }
  /// @return Const iterator one past the last element.
  const T* end() const noexcept { return data() + size(); }
  /**
   * @brief Remove all elements.
   *
   * In SSO mode, destroys inline elements and resets the size to zero.
   * In spilled mode, clears the underlying vector.
   */
  void clear() noexcept {
	if (!use_vec()) {
	  destroy_sso();
	  sz_ = 0;
	} else {
	  hv_.clear();
	}
  }

 private:
  /// Inline slot with proper alignment for T.
  struct alignas(alignof(T)) Slot {
	std::byte data[sizeof(T)];
  };
  /// @return true if the container is in spilled mode and uses the heap vector.
  bool use_vec() const noexcept { return spilled_; }
  /// @return void* to the inline storage slot @p i.
  void* sso_ptr(std::size_t i) noexcept {
	return static_cast<void*>(sso_[i].data);
  }
  /// @return const void* to the inline storage slot @p i.
  const void* sso_ptr(std::size_t i) const noexcept {
	return static_cast<const void*>(sso_[i].data);
  }
  /// Perform the first spill: move all inline elements into the heap vector.
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
  /// Destroy inline elements if we are still in SSO mode.
  void destroy_sso() noexcept {
	if (!spilled_) {
	  for (std::size_t i = 0; i < sz_; ++i) {
		auto* ptr = reinterpret_cast<T*>(sso_ptr(i));
		ptr->~T();
	  }
	}
  }
  /// Steal storage state from @p other.
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

  /// Inline storage for up to N elements.
  std::array<Slot, N> sso_{};
  /// Heap vector used after the first spill.
  std::vector<T> hv_{};
  /// Number of elements when in SSO mode (undefined when spilled_ is true).
  std::size_t sz_{0};
  /// Whether storage has spilled to the heap vector.
  bool spilled_{false};
};

}  // namespace tp
