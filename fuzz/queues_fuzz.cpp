#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <latch>
#include <thread>
#include <vector>
#include <dagflow/detail/chase_lev_deque.hpp>
#include <dagflow/detail/ring_mpmc.hpp>
#include "byte_reader.hpp"
#include "check.hpp"
#include "linear_history.hpp"

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  if (size > 4096) return 0;
  using dagflow::fuzz::check;
  dagflow::fuzz::Bytes bytes(data, size);
  dagflow::detail::ring_mpmc<unsigned, 8> ring;
  dagflow::detail::chase_lev_deque<unsigned, 8> deque;
  std::deque<unsigned> fifo, lifo;
  const auto steps = 1 + bytes.bound(128);
  for (unsigned i = 0; i < steps; ++i) {
    unsigned value = bytes.next(), out = 999;
    switch (bytes.bound(5)) {
      case 0: {
        bool accepted = ring.try_push(value);
        check(accepted == (fifo.size() < 8));
        if (accepted) fifo.push_back(value);
        break;
      }
      case 1: {
        bool found = ring.try_pop(out);
        check(found == !fifo.empty());
        if (found) { check(out == fifo.front()); fifo.pop_front(); }
        else check(out == 999);
        break;
      }
      case 2: {
        bool accepted = deque.try_push(value);
        check(accepted == (lifo.size() < 8));
        if (accepted) lifo.push_back(value);
        break;
      }
      default: {
        const bool steal = bytes.bit();
        const bool found = steal ? deque.try_steal(out) : deque.try_pop(out);
        check(found == !lifo.empty());
        if (found) {
          check(out == (steal ? lifo.front() : lifo.back()));
          if (steal) lifo.pop_front(); else lifo.pop_back();
        } else check(out == 999);
      }
    }
    check(ring.empty() == fifo.empty());
    check(deque.empty() == lifo.empty());
    check(deque.free_capacity() == 8 - lifo.size());
  }

  // Real publication races, with one attempt per operation and bounded work.
  // Unsuccessful pushes remain caller-owned; compare accepted/consumed sets
  // only after all publishers and consumers have stopped.
  dagflow::detail::ring_mpmc<unsigned, 8> concurrent;
  std::array<std::atomic<unsigned>, 128> seen{};
  std::array<bool, 128> accepted{};
  const auto threads = 2 + bytes.bound(3);
  std::vector<std::thread> workers;
  for (unsigned t = 0; t < threads; ++t) {
    workers.emplace_back([&, t] {
      for (unsigned i = 0; i < 32; ++i) {
        const auto id = t * 32 + i;
        accepted[id] = concurrent.try_push(id);
        unsigned value;
        if (concurrent.try_pop(value)) {
          check(value < threads * 32);
          check(seen[value].fetch_add(1) == 0);
        }
        if ((i + t) % 4 == 0) std::this_thread::yield();
      }
    });
  }
  for (auto& worker : workers) worker.join();
  unsigned value;
  while (concurrent.try_pop(value)) { check(value < threads * 32); check(seen[value].fetch_add(1) == 0); }
  for (unsigned i = 0; i < threads * 32; ++i) check(seen[i] == unsigned(accepted[i]));

  dagflow::detail::chase_lev_deque<unsigned, 8> stealing;
  for (auto& counter : seen) counter.store(0);
  accepted.fill(false);
  std::atomic<bool> done{false};
  workers.clear();
  auto consume = [&](unsigned id) { check(id < 128); check(seen[id].fetch_add(1) == 0); };
  for (unsigned t = 0; t < threads; ++t) {
    workers.emplace_back([&] {
      while (!done.load(std::memory_order_acquire)) {
        unsigned id;
        if (stealing.try_steal(id)) consume(id);
        else std::this_thread::yield();
      }
    });
  }
  for (unsigned i = 0; i < 128; ++i) {
    accepted[i] = stealing.try_push(i);
    if (bytes.bit() && stealing.try_pop(value)) consume(value);
  }
  done.store(true, std::memory_order_release);
  for (auto& worker : workers) worker.join();
  while (stealing.try_pop(value)) consume(value);
  for (unsigned i = 0; i < 128; ++i) check(seen[i] == unsigned(accepted[i]));
  // Eight completed operations, two concurrent callers. An independent
  // backtracking FIFO model finds whether *any* sequential history respects
  // real-time order and all outcomes, including failed try_push/try_pop.
  dagflow::detail::ring_mpmc<unsigned,2> history_queue;
  std::array<dagflow::fuzz::Operation,8> history{};
  std::atomic<unsigned> logical_clock{0};
  std::latch launch(1);
  std::thread histories[2];
  for(unsigned t=0;t<2;++t) {
    for(unsigned i=0;i<4;++i) {
      auto& op=history[t*4+i];
      op.push=bytes.bit();
      op.input=1+t*4+i; // unique payload IDs
    }
    histories[t]=std::thread([&,t] {
      launch.wait();
      for(unsigned i=0;i<4;++i) {
        auto& op=history[t*4+i];
        if(((t+i)&1)==0)std::this_thread::yield();
        op.begin=logical_clock.fetch_add(1,std::memory_order_seq_cst);
        if(op.push) op.success=history_queue.try_push(op.input);
        else op.success=history_queue.try_pop(op.result);
        op.end=logical_clock.fetch_add(1,std::memory_order_seq_cst);
      }
    });
  }
  launch.count_down();
  for(auto& t:histories)t.join();
  std::deque<unsigned> expected_fifo;
  check(dagflow::fuzz::linearizable(history,0,expected_fifo));
  return 0;
}
