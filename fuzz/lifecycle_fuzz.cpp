#include <array>
#include <atomic>
#include <cstdint>
#include <latch>
#include <stdexcept>
#include <thread>
#include <vector>
#include <dagflow/thread_pool.hpp>
#include "byte_reader.hpp"
#include "check.hpp"
#include "concurrency.hpp"
using dagflow::fuzz::check;
namespace {
void exercise(dagflow::fuzz::Bytes& b) {
  const unsigned workers=1+b.bound(4);
  const unsigned publishers=2+b.bound(3);
  const unsigned jobs=1+b.bound(32);
  dagflow::Config cfg;
  cfg.threads=workers;cfg.shards=1+b.bound(6);cfg.pin_threads=false;
  cfg.idle_us_min=0;cfg.idle_us_max=100;
  dagflow::Pool pool(cfg);
  dagflow::fuzz::Perturb perturb(b.next());
  std::array<std::atomic<unsigned>,256> seen{};
  std::array<std::atomic<bool>,256> accepted{};
  std::atomic<unsigned> children{0};
  std::latch start(1);
  std::vector<std::thread> threads;
  for(unsigned p=0;p<publishers;++p) {
    threads.emplace_back([&,p] {
      start.wait();
      for(unsigned i=0;i<jobs;++i) {
        const unsigned id=p*jobs+i;
        try {
          pool.submit_detached([&,id] {
            check(seen[id].fetch_add(1)==0);
            if((id&3)==0) {
              pool.submit_detached([&] { children.fetch_add(1); });
            }
          });
          accepted[id].store(true,std::memory_order_release);
        } catch(const std::logic_error&) { // close rejects external callers
          break;
        }
        if((id&3)==0)std::this_thread::yield();
      }
    });
  }
  const auto close_yields = b.bound(16);
  const auto double_shutdown = b.bit();
  std::thread closer([&] {
    start.wait();
    for(unsigned i=0;i<close_yields;++i)std::this_thread::yield();
    pool.close();
  });
  start.count_down();
  // shutdown may race with admitted external publishers; their thread objects
  // must still join before Pool storage is destroyed (public precondition).
  std::thread shutdown_a([&] { pool.shutdown(); });
  if(double_shutdown) {
    std::thread shutdown_b([&] { pool.shutdown(); });
    shutdown_b.join();
  }
  for(auto& thread:threads)thread.join();
  closer.join();shutdown_a.join();
  pool.shutdown();
  check(pool.closed());
  unsigned expected_children=0;
  for(unsigned id=0;id<publishers*jobs;++id) {
    const auto accepted_job=accepted[id].load(std::memory_order_acquire);
    check(seen[id].load(std::memory_order_acquire)==unsigned(accepted_job));
    if(accepted_job && (id&3)==0)++expected_children;
  }
  check(children.load()==expected_children);
}
}
extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data,std::size_t size){
  if(size>4096)return 0; dagflow::fuzz::Bytes b(data,size);exercise(b);return 0;
}
