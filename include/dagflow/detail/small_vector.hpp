#pragma once

#include <cstddef>
#include <limits>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include <dagflow/config.hpp>
#include <dagflow/detail/runtime_memory.hpp>

namespace dagflow {

/// Move-only vector with N inline elements and cache-aligned heap storage. Heap
/// storage uses runtime_memory. reserve(n) grows to exactly n elements;
/// automatic growth doubles capacity. clear() retains allocated storage.
/// Moving throwing, move-only elements provides the basic guarantee.
template <class T, std::size_t N = DAGFLOW_DEFAULT_SMALL_VECTOR_CAPACITY>
class small_vector {
  static_assert(N > 0, "Inline capacity must be positive");

 public:
  using value_type = T;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;
  using reference = T&;
  using const_reference = const T&;
  using pointer = T*;
  using const_pointer = const T*;
  using iterator = pointer;
  using const_iterator = const_pointer;

  // User-provided so value-initialization (small_vector{}) also skips zeroing
  // the raw inline storage. Metadata has its own member initializers.
  small_vector() noexcept {}
  small_vector(const small_vector&) = delete;
  small_vector& operator=(const small_vector&) = delete;
  small_vector(small_vector&& other) noexcept(
      std::is_nothrow_move_constructible_v<T>) {
    move_from(std::move(other));
  }
  small_vector& operator=(small_vector&& other) noexcept(
      std::is_nothrow_move_constructible_v<T>) {
    if (this != &other) {
      release_storage();
      move_from(std::move(other));
    }
    return *this;
  }
  ~small_vector() { release_storage(); }

  template <class... Args>
  reference emplace_back(Args&&... args) {
    if (size_ < capacity_) {
      pointer slot = heap_ ? heap_ + size_ : inline_slot(size_);
      auto* item = std::construct_at(slot, std::forward<Args>(args)...);
      ++size_;
      return *item;
    }
    if (size_ == max_size()) throw std::length_error("small_vector capacity");
    const auto next = capacity_ > max_size() / 2 ? max_size() : capacity_ * 2;
    pointer storage = allocate_storage(next);
    // Construct the new element before relocation: args may alias our storage.
    try {
      std::construct_at(storage + size_, std::forward<Args>(args)...);
    } catch (...) {
      deallocate_storage(storage);
      throw;
    }
    try {
      relocate_to(storage);
    } catch (...) {
      std::destroy_at(storage + size_);
      deallocate_storage(storage);
      throw;
    }
    replace_storage(storage, next);
    ++size_;
    return back();
  }
  void push_back(const_reference value) { emplace_back(value); }
  void push_back(T&& value) { emplace_back(std::move(value)); }
  void pop_back() noexcept {
    std::destroy_at(heap_ ? heap_ + size_ - 1 : inline_ptr(size_ - 1));
    --size_;
  }

  void reserve(size_type requested) {
    if (requested <= capacity()) return;
    pointer storage = allocate_storage(requested);
    try {
      relocate_to(storage);
    } catch (...) {
      deallocate_storage(storage);
      throw;
    }
    replace_storage(storage, requested);
  }
  void clear() noexcept {
    destroy_current_elements();
    size_ = 0;
  }

  [[nodiscard]] size_type size() const noexcept { return size_; }
  [[nodiscard]] size_type capacity() const noexcept { return capacity_; }
  [[nodiscard]] static constexpr size_type max_size() noexcept {
    return static_cast<size_type>(std::numeric_limits<difference_type>::max()) /
           sizeof(T);
  }
  [[nodiscard]] bool empty() const noexcept { return size() == 0; }
  pointer data() noexcept {
    if (heap_) return heap_;
    return size_ != 0 ? inline_ptr(0) : inline_slot(0);
  }
  const_pointer data() const noexcept {
    if (heap_) return heap_;
    return size_ != 0 ? inline_ptr(0) : inline_slot(0);
  }
  reference operator[](size_type index) noexcept {
    return heap_ ? heap_[index] : *inline_ptr(index);
  }
  const_reference operator[](size_type index) const noexcept {
    return heap_ ? heap_[index] : *inline_ptr(index);
  }
  reference at(size_type index) {
    if (index >= size()) throw std::out_of_range("small_vector::at");
    return (*this)[index];
  }
  const_reference at(size_type index) const {
    if (index >= size()) throw std::out_of_range("small_vector::at");
    return (*this)[index];
  }
  reference front() noexcept { return (*this)[0]; }
  const_reference front() const noexcept { return (*this)[0]; }
  reference back() noexcept { return (*this)[size() - 1]; }
  const_reference back() const noexcept { return (*this)[size() - 1]; }
  iterator begin() noexcept { return data(); }
  iterator end() noexcept { return empty() ? data() : data() + size_; }
  const_iterator begin() const noexcept { return data(); }
  const_iterator end() const noexcept {
    return empty() ? data() : data() + size_;
  }
  const_iterator cbegin() const noexcept { return begin(); }
  const_iterator cend() const noexcept { return end(); }

 private:
  static constexpr size_type heap_alignment =
      alignof(T) > DAGFLOW_CACHE_LINE_SIZE ? alignof(T)
                                            : DAGFLOW_CACHE_LINE_SIZE;
  static_assert(N <= max_size(), "Inline capacity is too large");

  static pointer allocate_storage(size_type capacity) {
    if (capacity > max_size()) throw std::length_error("small_vector capacity");
    return static_cast<pointer>(
        detail::allocate_bytes(sizeof(T) * capacity, heap_alignment));
  }
  static void deallocate_storage(pointer storage) noexcept {
    if (storage) detail::deallocate_bytes(storage, heap_alignment);
  }
  static void destroy_elements(pointer storage, size_type count) noexcept {
    while (count != 0) std::destroy_at(storage + --count);
  }
  void destroy_current_elements() noexcept {
    if (heap_) {
      destroy_elements(heap_, size_);
    } else {
      for (size_type count = size_; count != 0; --count)
        std::destroy_at(inline_ptr(count - 1));
    }
  }
  void relocate_to(pointer storage) {
    size_type constructed = 0;
    try {
      for (; constructed < size_; ++constructed)
        std::construct_at(storage + constructed,
                          std::move_if_noexcept((*this)[constructed]));
    } catch (...) {
      destroy_elements(storage, constructed);
      throw;
    }
  }
  void replace_storage(pointer storage, size_type capacity) noexcept {
    destroy_current_elements();
    deallocate_storage(heap_);
    heap_ = storage;
    capacity_ = capacity;
  }
  void release_storage() noexcept {
    clear();
    deallocate_storage(heap_);
    heap_ = nullptr;
    capacity_ = N;
  }
  // Raw slot address, including before a T has been constructed there.
  pointer inline_slot(size_type index) noexcept {
    return reinterpret_cast<pointer>(inline_storage_ + sizeof(T) * index);
  }
  const_pointer inline_slot(size_type index) const noexcept {
    return reinterpret_cast<const_pointer>(inline_storage_ + sizeof(T) * index);
  }
  // Only for live inline elements; never for an empty buffer or end().
  pointer inline_ptr(size_type index) noexcept {
    return std::launder(inline_slot(index)); // T* -> T*
  }
  const_pointer inline_ptr(size_type index) const noexcept {
    return std::launder(inline_slot(index));
  }
  void move_from(small_vector&& other) {
    if (other.heap_) {
      heap_ = std::exchange(other.heap_, nullptr);
      size_ = std::exchange(other.size_, 0);
      capacity_ = std::exchange(other.capacity_, N);
      return;
    }
    try {
      while (size_ < other.size_) {
        std::construct_at(inline_slot(size_), std::move(other[size_]));
        ++size_;
      }
    } catch (...) {
      clear();
      throw;
    }
    other.clear();
  }

  alignas(T) std::byte inline_storage_[sizeof(T) * N];
  size_type size_{0};
  size_type capacity_{N};
  pointer heap_{nullptr};
};

}  // namespace dagflow
