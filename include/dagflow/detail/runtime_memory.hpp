#pragma once

#include <cstddef>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace dagflow::detail {

// Thin allocation dispatch to the selected library. No slabs, caches, free
// lists, ownership counters or thread-affine heaps are implemented here.
// alignment must be a nonzero power of two supported by the backend.
// Zero bytes reserves a minimal aligned block, freed with deallocate_bytes.
[[nodiscard]] void* allocate_bytes(std::size_t bytes, std::size_t alignment);
void deallocate_bytes(void* pointer, std::size_t alignment) noexcept;
[[nodiscard]] const char* allocator_backend() noexcept;

/// STL storage uses the same process-wide backend as runtime objects. No pool
/// pointer is retained: completion containers may be freed after their Pool,
/// and any thread may deallocate storage allocated by another instance.
template <class T>
struct RuntimeAllocator {
  using value_type = T;
  using is_always_equal = std::true_type;
  using propagate_on_container_move_assignment = std::true_type;

  RuntimeAllocator() noexcept = default;
  template <class U>
  RuntimeAllocator(const RuntimeAllocator<U>&) noexcept {}

  [[nodiscard]] T* allocate(std::size_t count) {
    if (count > std::numeric_limits<std::size_t>::max() / sizeof(T))
      throw std::bad_array_new_length{};
    return static_cast<T*>(allocate_bytes(count * sizeof(T), alignof(T)));
  }
  void deallocate(T* storage, std::size_t) noexcept {
    deallocate_bytes(storage, alignof(T));
  }
  template <class U>
  bool operator==(const RuntimeAllocator<U>&) const noexcept { return true; }
};

template <class T>
struct ObjectDeleter {
  void operator()(T* object) const noexcept {
    if (!object) return;
    std::destroy_at(object);
    deallocate_bytes(object, alignof(T));
  }
};

template <class T>
using OwnedObject = std::unique_ptr<T, ObjectDeleter<T>>;

template <class T>
struct ArrayDeleter {
  std::size_t count{0};
  void operator()(T* objects) const noexcept {
    if (!objects) return;
    std::destroy_n(objects, count);
    deallocate_bytes(objects, alignof(T));
  }
};

template <class T>
using OwnedArray = std::unique_ptr<T[], ArrayDeleter<T>>;

/// Contiguous, default-initialized objects allocated by the runtime backend.
/// The owner destroys every element, including for non-movable atomic state.
/// Scalar elements are uninitialized; callers must fill them before reading.
template <class T>
[[nodiscard]] OwnedArray<T> make_owned_array(std::size_t count) {
  if (count == 0) return {};
  if (count >
      static_cast<std::size_t>(std::numeric_limits<std::ptrdiff_t>::max()) /
          sizeof(T))
    throw std::length_error("runtime_memory: array too large");
  auto* storage =
      static_cast<T*>(allocate_bytes(sizeof(T) * count, alignof(T)));
  try {
    std::uninitialized_default_construct_n(storage, count);
  } catch (...) {
    deallocate_bytes(storage, alignof(T));
    throw;
  }
  return OwnedArray<T>{storage, ArrayDeleter<T>{count}};
}

/// One owner of a constructed object; release() explicitly transfers custody.
/// Constructor failure returns storage to the same allocation backend.
template <class T, class... Args>
[[nodiscard]] OwnedObject<T> make_owned(Args&&... args) {
  void* storage = allocate_bytes(sizeof(T), alignof(T));
  try {
    return OwnedObject<T>{std::construct_at(static_cast<T*>(storage),
                                            std::forward<Args>(args)...)};
  } catch (...) {
    deallocate_bytes(storage, alignof(T));
    throw;
  }
}

}  // namespace dagflow::detail
