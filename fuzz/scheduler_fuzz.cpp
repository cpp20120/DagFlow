#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>
#include <dagflow/detail/scheduler.hpp>
#include "byte_reader.hpp"
#include "check.hpp"
#include "concurrency.hpp"
using dagflow::fuzz::check;
namespace {
void exercise(dagflow::fuzz::Bytes& b) {
  const unsigned workers=1+b.bound(4);
  const unsigned shards=1+b.bound(7);
  dagflow::detail::Scheduler scheduler(workers,shards,1+b.bound(8));
  dagflow::fuzz::Perturb perturb(b.next());
  std::array<dagflow::detail::ScheduledTask,96> packets;
  std::array<std::atomic<unsigned>,96> seen{};
  const unsigned count=1+b.bound(96);
  unsigned accepted=0;
  // Only worker 0 accesses its local queue in this phase: its push/pop
  // ownership rule is preserved. Afterwards all ingress is shared.
  for(unsigned i=0;i<count;++i) {
    auto& packet=packets[i];
    packet.prio=b.bit()?dagflow::Priority::High:dagflow::Priority::Normal;
    if(i<8 && b.bit()) {
      check(scheduler.try_submit_local(0,&packet));
    } else if(b.bit()) {
      scheduler.submit_overflow(b.bound(shards),&packet);
    } else {
      check(scheduler.try_submit_external(b.bound(shards),&packet));
    }
    ++accepted;
  }
  std::atomic<unsigned> consumed{0};
  std::vector<std::thread> executors;
  for(unsigned w=0;w<workers;++w) {
    executors.emplace_back([&,w] {
      for(unsigned attempt=0;attempt<20000 && consumed.load(std::memory_order_acquire)<accepted;++attempt) {
        bool recruit=false;
        auto* packet=scheduler.try_acquire(w,recruit);
        if(!packet) { if((attempt&15)==0)std::this_thread::yield(); continue; }
        const auto index=static_cast<std::size_t>(packet-packets.data());
        check(index<count && packet==&packets[index]);
        check(seen[index].fetch_add(1,std::memory_order_acq_rel)==0);
        consumed.fetch_add(1,std::memory_order_release);
      }
    });
  }
  for(auto& t:executors)t.join();
  check(consumed==accepted);
  for(unsigned i=0;i<count;++i)check(seen[i]==1);
  for(unsigned w=0;w<workers;++w)check(scheduler.try_acquire(w)==nullptr);
}
}
extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data,std::size_t size){
  if(size>4096)return 0; dagflow::fuzz::Bytes b(data,size);exercise(b);return 0;
}
