#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <latch>
#include <thread>
#include <vector>
#include <dagflow/detail/parking_lot.hpp>
#include <dagflow/detail/scheduler.hpp>
#include "byte_reader.hpp"
#include "check.hpp"
#include "concurrency.hpp"
using dagflow::fuzz::check;
namespace {
void exercise(dagflow::fuzz::Bytes& b) {
  // Word boundary coverage without constructing hundreds of OS threads.
  constexpr std::array<unsigned,9> sizes{1,2,3,63,64,65,127,128,129};
  const unsigned workers=sizes[b.bound(sizes.size())];
  const unsigned shards=1+b.bound(5);
  std::vector<std::uint32_t> assignment(workers);
  for(unsigned i=0;i<workers;++i) assignment[i]=b.bound(shards);
  dagflow::detail::Scheduler scheduler(workers,shards,1+b.bound(4),assignment);
  dagflow::detail::ParkingLot lot(scheduler);
  dagflow::fuzz::Perturb perturb(b.next());
  const unsigned chosen=b.bound(workers);
  const auto home=scheduler.home_shard(chosen);
  std::atomic<bool> stop{false};
  // After a completed prepare(), this is the sole announced waiter. A
  // wake_one(home) must claim its registration and bump its epoch.
  const auto epoch=lot.prepare(chosen);
  lot.wake_one(home);
  check(lot.fuzz_epoch(chosen)!=epoch);
  lot.wait(chosen,epoch,100,stop);
  const auto epoch2=lot.prepare(chosen);
  lot.cancel(chosen);
  lot.wake_one(home);
  check(lot.fuzz_epoch(chosen)==epoch2);
  // Exercise CV handshake on a separate thread with a real announced waiter.
  std::latch announced(1), proceed(1);
  std::atomic<std::uint64_t> observed{0};
  std::thread waiter([&] {
    const auto before=lot.prepare(chosen);
    observed.store(before,std::memory_order_release);
    announced.count_down();
    proceed.wait();
    lot.wait(chosen,before,200000,stop);
  });
  announced.wait();
  if (b.bit()) {
    // Signal after prepare but before the waiter enters wait(): epoch is the
    // remembered notification, so no CV transition is required.
    lot.wake_one(home);
    proceed.count_down();
  } else {
    // Also exercise the actual sleeping/CV notification path. Polling a
    // fuzz-only observer does not mutate waiter state or consume custody.
    proceed.count_down();
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(30);
    while (!lot.fuzz_sleeping(chosen) &&
           std::chrono::steady_clock::now() < deadline)
      std::this_thread::yield();
    // If the OS starved the waiter, this still tests wake-before-CV.
    lot.wake_one(home);
  }
  waiter.join();
  check(lot.fuzz_epoch(chosen)!=observed.load(std::memory_order_acquire));
  // Multiple announced waiters spanning more than one uint64_t word.
  std::vector<unsigned> indices;
  const unsigned count=1+b.bound(8);
  for(unsigned i=0;i<count;++i) {
    const unsigned id=(chosen+i)%workers;
    if(std::find(indices.begin(),indices.end(),id)==indices.end()) {
      lot.prepare(id); indices.push_back(id);
    }
  }
  lot.wake_all();
  for(auto id:indices) lot.cancel(id);
}
}
extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data,std::size_t size){
  if(size>4096)return 0;
  dagflow::fuzz::Bytes b(data,size); exercise(b);return 0;
}
