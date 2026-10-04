#include <cstddef>
#include <new>

#include "dagflow/detail/parking_lot.hpp"
#include "dagflow/detail/scheduler.hpp"
#include "support.hpp"

std::size_t allocations = 0, live = 0;
int budget = -1;
namespace dagflow::detail {
void* allocate_bytes(std::size_t bytes, std::size_t alignment) {
  if (budget == 0) throw std::bad_alloc();
  if (budget > 0) --budget;
  auto* pointer = ::operator new(bytes, std::align_val_t{alignment});
  ++allocations;
  ++live;
  return pointer;
}
void deallocate_bytes(void* pointer, std::size_t alignment) noexcept {
  --live;
  ::operator delete(pointer, std::align_val_t{alignment});
}
}  // namespace dagflow::detail
int main() {
  for (int failure = 0; failure < 6; ++failure) {
    budget = failure;
    bool caught = false;
    try {
      dagflow::detail::Scheduler topology(4, 2, 32);
      dagflow::detail::ParkingLot parking(topology);
    } catch (const std::bad_alloc&) {
      caught = true;
    }
    CHECK(caught && live == 0);
  }
  budget = -1;
  for (auto workers : {1u, 4u, 130u}) {
    allocations = 0;
    {
      dagflow::detail::Scheduler topology(workers, 3, 32);
      CHECK(allocations == 3);  // Local[], Shard[], CSR members[].
      dagflow::detail::ParkingLot parking(topology);
      CHECK(allocations == 6);  // Waiter[], Domain[], idle mask words[].
      CHECK(live == 6);
    }
    CHECK(live == 0);
  }
}
