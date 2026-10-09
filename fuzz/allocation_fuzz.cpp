#include <array>
#include <atomic>
#include <span>
#include <vector>
#include <cstdint>
#include <exception>
#include <new>
#include <stdexcept>
#include <dagflow/detail/fuzz_points.hpp>
#include <dagflow/task_graph.hpp>
#include "byte_reader.hpp"
#include "check.hpp"
using dagflow::fuzz::check;
namespace {
struct Budget {
  explicit Budget(int value) { dagflow::detail::fuzz_points::allocation_budget=value; }
  ~Budget(){ dagflow::detail::fuzz_points::allocation_budget=-1; }
};
void exercise(dagflow::fuzz::Bytes& b) {
  dagflow::Config cfg;cfg.threads=1+b.bound(2);cfg.pin_threads=false;
  cfg.idle_us_min=0;cfg.idle_us_max=100;
  dagflow::Pool pool(cfg);
  const unsigned rounds=1+b.bound(10);
  std::atomic<unsigned> completed{0};
  unsigned submitted=0;
  for(unsigned i=0;i<rounds;++i) {
    // Every injection is scoped to the calling producer thread; the worker's
    // cleanup and completion publication are never deliberately sabotaged.
    bool succeeded=false;
    try {
      Budget failure(b.bound(7));
      auto h=pool.submit([&completed] { completed.fetch_add(1); });
      succeeded=true;
      // h may leave scope before the task finishes: handles are observers.
    } catch (const std::bad_alloc&) {}
    submitted+=succeeded;
  }
  pool.wait_idle();
  check(completed.load()==submitted);
  // Accepted-prefix exception guarantee: a failed group construction can
  // leave earlier groups executing. After draining, executions must be a
  // contiguous prefix, never holes or duplicate invocations.
  struct Fn {
    std::atomic<unsigned>* counts;
    unsigned id;
    void operator()() { counts[id].fetch_add(1, std::memory_order_relaxed); }
  };
  std::array<std::atomic<unsigned>,96> invoked{};
  const unsigned batch_size = 65 + b.bound(32);
  std::vector<Fn> batch;
  batch.reserve(batch_size);
  for(unsigned i=0;i<batch_size;++i)batch.push_back(Fn{invoked.data(),i});
  bool complete=false;
  try {
    Budget failure(b.bound(120));
    pool.submit_batch_detached(std::span{batch});
    complete=true;
  } catch(const std::bad_alloc&) {}
  pool.wait_idle();
  bool missing=false;
  for(unsigned i=0;i<batch_size;++i) {
    auto calls=invoked[i].load(std::memory_order_acquire);
    check(calls<=1);
    if(!calls)missing=true;
    else check(!missing);  // No accepted task may appear after a gap.
    if(complete)check(calls==1);
  }
  // Failed seal must leave a graph recoverable for a subsequent seal/run.
  for(unsigned k=0;k<2;++k) {
    dagflow::TaskGraph graph(pool);
    unsigned a=0,b_count=0;
    auto first=graph.emplace([&]{++a;});
    auto second=graph.emplace([&]{++b_count;});
    graph.add_edge(first,second);
    bool sealed=false;
    try {
      Budget failure(b.bound(9));
      sealed=graph.seal();
    } catch (const std::bad_alloc&) {}
    check(graph.seal());
    pool.wait_and_rethrow(graph.run());
    check(a==1 && b_count==1);
    (void)sealed;
  }
  pool.shutdown();
}
}
extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data,std::size_t size){
  if(size>4096)return 0;dagflow::fuzz::Bytes b(data,size);exercise(b);return 0;
}
