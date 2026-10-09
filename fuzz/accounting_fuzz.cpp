#include <atomic>
#include <cstdint>
#include <latch>
#include <thread>
#include <vector>
#include <dagflow/detail/idle_accounting.hpp>
#include "byte_reader.hpp"
#include "check.hpp"
#include "concurrency.hpp"
using dagflow::fuzz::check;
namespace {
void exercise(dagflow::fuzz::Bytes& b) {
  const unsigned workers=1+b.bound(4);
  const unsigned rounds=1+b.bound(4);
  dagflow::fuzz::Perturb perturb(b.next());
  for(unsigned pass=0;pass<rounds;++pass) {
    // Repeated identities on the same threads exercise bounded TLS caches.
    dagflow::detail::IdleAccounting accounting(workers);
    const unsigned producers=1+b.bound(4);
    const unsigned each=1+b.bound(24);
    std::vector<std::thread> publishers;
    for(unsigned p=0;p<producers;++p) {
      publishers.emplace_back([&,p] {
        for(unsigned n=0;n<each;++n) {
          accounting.publish_external();
          if((p+n)%4==0)std::this_thread::yield();
        }
      });
    }
    for(auto& t:publishers)t.join();
    // All publications have finished. A waiter must not observe quiescence
    // until those exact credits are retired, even if workers race the scan.
    std::atomic<unsigned> work_done{0};
    std::atomic<bool> false_idle{false};
    std::thread observer([&] {
      accounting.wait();
      if(work_done.load(std::memory_order_acquire)!=producers*each)
        false_idle.store(true,std::memory_order_relaxed);
    });
    const unsigned total=producers*each;
    std::vector<std::thread> executors;
    for(unsigned w=0;w<workers;++w) {
      executors.emplace_back([&,w] {
        for(unsigned n=w;n<total;n+=workers) {
          // Simulate payload completion before epilogue retirement.
          work_done.fetch_add(1,std::memory_order_release);
          accounting.retire(w);
          accounting.worker_idle();
          if((n&3)==0)std::this_thread::yield();
        }
      });
    }
    for(auto& t:executors)t.join();
    observer.join();
    check(!false_idle && work_done.load()==total);
    accounting.wait();
  }
}
}
extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data,std::size_t size){
  if(size>4096)return 0; dagflow::fuzz::Bytes b(data,size);exercise(b);return 0;
}
