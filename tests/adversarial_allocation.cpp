#include <new>
#include <dagflow/detail/runtime_memory.hpp>
#include "adversarial_allocation.hpp"

namespace dagflow::detail {
void* allocate_bytes(std::size_t bytes, std::size_t alignment) {
  auto& fault = adversarial::allocation::fault;
  if (fault.remaining == 0) {
    const auto failed = fault;
    fault = {}; // Failure is one-shot; rollback itself may allocate.
    if (failed.on_failure) failed.on_failure(failed.context);
    throw std::bad_alloc{};
  }
  if (fault.remaining > 0) --fault.remaining;
  if (!bytes) bytes = 1;
  void* memory = alignment > __STDCPP_DEFAULT_NEW_ALIGNMENT__
      ? ::operator new(bytes, std::align_val_t{alignment}) : ::operator new(bytes);
  ++adversarial::allocation::live;
  return memory;
}

void deallocate_bytes(void* memory, std::size_t alignment) noexcept {
  if (!memory) return;
  --adversarial::allocation::live;
  if (alignment > __STDCPP_DEFAULT_NEW_ALIGNMENT__)
    ::operator delete(memory, std::align_val_t{alignment});
  else ::operator delete(memory);
}

const char* allocator_backend() noexcept { return "adversarial-test"; }
}
