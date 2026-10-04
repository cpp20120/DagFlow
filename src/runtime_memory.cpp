#include "dagflow/detail/runtime_memory.hpp"
#include "dagflow/detail/runtime_diagnostics.hpp"

#include <new>

#if defined(DAGFLOW_USE_MIMALLOC)
#include <mimalloc.h>
#elif defined(DAGFLOW_USE_TBBMALLOC)
#include <oneapi/tbb/scalable_allocator.h>
#endif

namespace dagflow::detail {

void* allocate_bytes(std::size_t bytes, std::size_t alignment) {
  // STL allocators may receive count == 0. tbbmalloc's aligned API rejects
  // zero bytes; reserve a minimal block so all backends share the same contract.
  if (bytes == 0) bytes = 1;
#if defined(DAGFLOW_USE_MIMALLOC)
  // Ordinary object sizes are multiples of their natural alignment. Route
  // those requests directly into mimalloc's native small-object size classes.
  // Arbitrary byte buffers and over-aligned objects keep explicit alignment.
  const bool natural = alignment <= alignof(std::max_align_t) &&
                       bytes >= alignment && (bytes & (alignment - 1)) == 0;
  void* memory = natural
      ? (bytes <= MI_SMALL_SIZE_MAX ? mi_malloc_small(bytes) : mi_malloc(bytes))
      : mi_malloc_aligned(bytes, alignment);
#elif defined(DAGFLOW_USE_TBBMALLOC)
  // tbbmalloc requires at least pointer alignment, including for byte-sized T.
  void* memory = scalable_aligned_malloc(
      bytes, alignment < sizeof(void*) ? sizeof(void*) : alignment);
#else
  void* memory = alignment > __STDCPP_DEFAULT_NEW_ALIGNMENT__
                     ? ::operator new(bytes, std::align_val_t{alignment})
                     : ::operator new(bytes);
#endif
#if defined(DAGFLOW_USE_MIMALLOC) || defined(DAGFLOW_USE_TBBMALLOC)
  if (!memory) throw std::bad_alloc{};
#endif
  // Diagnostic builds count successful runtime-boundary requests, not backend
  // size-class rounding, allocator-internal metadata or allocations outside
  // this boundary. The hooks compile away in ordinary timing builds.
  runtime_count(RuntimeEvent::memory_allocations);
  runtime_count(RuntimeEvent::memory_requested_bytes, bytes);
  return memory;
}

void deallocate_bytes(void* pointer, std::size_t alignment) noexcept {
  if (pointer) runtime_count(RuntimeEvent::memory_deallocations);
#if defined(DAGFLOW_USE_MIMALLOC)
  (void)alignment;
  mi_free(pointer);
#elif defined(DAGFLOW_USE_TBBMALLOC)
  (void)alignment;
  scalable_aligned_free(pointer);
#else
  if (alignment > __STDCPP_DEFAULT_NEW_ALIGNMENT__)
    ::operator delete(pointer, std::align_val_t{alignment});
  else
    ::operator delete(pointer);
#endif
}

const char* allocator_backend() noexcept {
#if defined(DAGFLOW_USE_MIMALLOC)
  return "mimalloc";
#elif defined(DAGFLOW_USE_TBBMALLOC)
  return "tbbmalloc";
#else
  return "system";
#endif
}

}  // namespace dagflow::detail
