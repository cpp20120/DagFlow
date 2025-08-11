/**
 * @file small_vec.hpp
 * @brief A small vector optimization container similar to std::vector but with
 * stack storage for small sizes.
 */
#pragma once
#include <array>
#include <cstddef>
#include <new>
#include <type_traits>
#include <utility>
#include <vector>

namespace tp {
/**
 * @class SmallVec
 * @brief A vector-like container with small size optimization.
 *
 * @tparam T The type of elements stored in the container.
 * @tparam N The number of elements to store on the stack before spilling to
 * heap. Defaults to 4.
 *
 * This container provides similar functionality to std::vector but stores the
 * first N elements on the stack (using small size optimization) before spilling
 * over to heap allocation.
 */
template <typename T, std::size_t N = 4>
class SmallVec {
 public:
  /** @brief Default constructor (empty vector). */
  SmallVec() = default;
  /** @brief Deleted copy constructor (non-copyable). */
  SmallVec(const SmallVec&) = delete;
  /** @brief Deleted copy assignment (non-copyable). */
  SmallVec& operator=(const SmallVec&) = delete;

  /** @brief Move constructor. */
  SmallVec(SmallVec&& other) noexcept { move_from(std::move(other)); }
  /** @brief Move assignment operator. */
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
  /** @brief Destructor (destroys elements in either storage). */
  ~SmallVec() { destroy_sso(); }
  /**
   * @brief Append a copy of a value to the vector.
   * @param v Value to copy.
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
   * @brief Append a value by move.
   * @param v Value to move.
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
  /**
   * @brief Get the number of elements in the vector.
   * @return Element count.
   */
  std::size_t size() const noexcept { return use_vec() ? hv_.size() : sz_; }
  /**
   * @brief Check if the vector is empty.
   * @return True if empty.
   */
  bool empty() const noexcept { return size() == 0; }

  /** @brief Access underlying storage pointer (mutable). */
  T* data() noexcept {
	return use_vec() ? hv_.data() : reinterpret_cast<T*>(sso_[0].data);
  }

  /** @brief Access underlying storage pointer (const). */
  const T* data() const noexcept {
	return use_vec() ? hv_.data() : reinterpret_cast<const T*>(sso_[0].data);
  }
  /** @brief Element access (mutable). */
  T& operator[](std::size_t i) noexcept { return data()[i]; }
  /** @brief Element access (const). */
  const T& operator[](std::size_t i) const noexcept { return data()[i]; }

  /** @brief Iterator to first element (mutable). */
  T* begin() noexcept { return data(); }
  /** @brief Iterator to one-past-last element (mutable). */
  T* end() noexcept { return data() + size(); }
  /** @brief Iterator to first element (const). */
  const T* begin() const noexcept { return data(); }
  /** @brief Iterator to one-past-last element (const). */
  const T* end() const noexcept { return data() + size(); }
  /**
   * @brief Clear all elements.
   *
   * Destroys objects in inline storage or clears heap vector.
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
  /**
   * @struct Slot
   * @brief Inline storage slot for one element, aligned to @p alignof(T).
   */
  struct alignas(alignof(T)) Slot {
	std::byte data[sizeof(T)];
  };
  /** @brief Returns true if currently using heap storage. */
  bool use_vec() const noexcept { return spilled_; }
  /** @brief Get pointer to inline slot @p i (mutable). */
  void* sso_ptr(std::size_t i) noexcept {
	return static_cast<void*>(sso_[i].data);
  }
  /** @brief Get pointer to inline slot @p i (const). */
  const void* sso_ptr(std::size_t i) const noexcept {
	return static_cast<const void*>(sso_[i].data);
  }
  /** @brief Move elements from inline storage to heap vector. */
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
  /** @brief Destroy elements in inline storage. */
  void destroy_sso() noexcept {
	if (!spilled_) {
	  for (std::size_t i = 0; i < sz_; ++i) {
		T* ptr = reinterpret_cast<T*>(sso_ptr(i));
		ptr->~T();
	  }
	}
  }
  /** @brief Move contents from another SmallVec. */
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

  std::array<Slot, N> sso_{};  ///< Inline storage buffer.
  std::vector<T> hv_{};		   ///< Heap storage (active after spill).
  std::size_t sz_{0};		   ///< Number of elements in inline storage.
  bool spilled_{false};		   ///< True if using heap storage.
};

}  // namespace tp
