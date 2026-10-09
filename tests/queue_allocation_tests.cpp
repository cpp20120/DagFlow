#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <new>

#include <dagflow/detail/chase_lev_deque.hpp>
#include <dagflow/detail/ring_mpmc.hpp>

namespace {
std::atomic<bool> tracking{false};
std::atomic<unsigned> allocations{0}, deallocations{0};
void* allocate(std::size_t size, std::size_t alignment) {
  if (tracking.load(std::memory_order_relaxed)) ++allocations;
  size = size ? size : 1;
  void* result;
  if (alignment <= alignof(std::max_align_t))
    result = std::malloc(size);
  else {
    const auto rounded = ((size + alignment - 1) / alignment) * alignment;
    result = std::aligned_alloc(alignment, rounded);
  }
  if (!result) throw std::bad_alloc();
  return result;
}
void deallocate(void* ptr) noexcept {
  if (ptr && tracking.load(std::memory_order_relaxed)) ++deallocations;
  std::free(ptr);
}
void check(bool ok) {
  if (!ok) std::abort();
}
}  // namespace

void* operator new(std::size_t n) {
  return allocate(n, alignof(std::max_align_t));
}
void* operator new[](std::size_t n) {
  return allocate(n, alignof(std::max_align_t));
}
void* operator new(std::size_t n, std::align_val_t a) {
  return allocate(n, std::size_t(a));
}
void* operator new[](std::size_t n, std::align_val_t a) {
  return allocate(n, std::size_t(a));
}
void operator delete(void* p) noexcept { deallocate(p); }
void operator delete[](void* p) noexcept { deallocate(p); }
void operator delete(void* p, std::size_t) noexcept { deallocate(p); }
void operator delete[](void* p, std::size_t) noexcept { deallocate(p); }
void operator delete(void* p, std::align_val_t) noexcept { deallocate(p); }
void operator delete[](void* p, std::align_val_t) noexcept { deallocate(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept {
  deallocate(p);
}
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept {
  deallocate(p);
}

int main() {
  dagflow::detail::chase_lev_deque<int, 8> deque;
  dagflow::detail::ring_mpmc<int, 8> ring;
  tracking.store(true);
  for (int round = 0; round < 10000; ++round) {
    for (int i = 0; i < 8; ++i) {
      check(deque.try_push(i));
      check(ring.try_push(i));
    }
    check(!deque.try_push(9));
    check(!ring.try_push(9));
    for (int i = 0; i < 8; ++i) {
      int value = -1;
      check(i % 2 ? deque.try_pop(value) : deque.try_steal(value));
      check(ring.try_pop(value));
    }
  }
  tracking.store(false);
  std::printf("queue operations: %u allocations, %u deallocations\n",
              allocations.load(), deallocations.load());
  check(allocations.load() == 0 && deallocations.load() == 0);
}
