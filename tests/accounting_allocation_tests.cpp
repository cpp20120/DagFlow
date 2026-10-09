#include <array>
#include <cstddef>
#include <new>

#include <dagflow/detail/idle_accounting.hpp>
#include "support.hpp"

static int budget = -1;
static std::size_t allocations = 0, live = 0;
namespace dagflow::detail {
void* allocate_bytes(std::size_t bytes, std::size_t alignment) {
  if (budget == 0) throw std::bad_alloc();
  if (budget > 0) --budget;
  auto* result = ::operator new(bytes, std::align_val_t{alignment});
  ++allocations;
  ++live;
  return result;
}
void deallocate_bytes(void* pointer, std::size_t alignment) noexcept {
  --live;
  ::operator delete(pointer, std::align_val_t{alignment});
}
}  // namespace dagflow::detail
int main() {
  using dagflow::detail::IdleAccounting;
  budget = 0;
  bool failed = false;
  try { IdleAccounting accounting(1); }
  catch (const std::bad_alloc&) { failed = true; }
  CHECK(failed && live == 0);
  budget = -1;
  {
    IdleAccounting accounting(1);
    budget = 0;
    failed = false;
    try { accounting.publish_external(); }
    catch (const std::bad_alloc&) { failed = true; }
    CHECK(failed && live == 1);
    accounting.wait();  // Failed registration must not publish a phantom task.
    budget = -1;
    accounting.publish_external();
    const auto registered = allocations;
    for (int i = 0; i < 100; ++i) {
      accounting.publish_external();
      accounting.retire(0);
    }
    CHECK(allocations == registered);
    accounting.retire(0);
    accounting.wait();
  }
  CHECK(live == 0);
  {
    std::array<dagflow::detail::OwnedObject<IdleAccounting>, 8> pools;
    for (auto& p : pools) {
      p = dagflow::detail::make_owned<IdleAccounting>(1);
      p->publish_external();
      p->retire(0);
    }
    const auto registered = allocations;
    for (auto& p : pools) {  // TLS eviction reuses the existing producer lane.
      p->publish_external();
      p->retire(0);
      p->wait();
    }
    CHECK(allocations == registered);
  }
  CHECK(live == 0);
}
