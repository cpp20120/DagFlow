#include <array>
#include <atomic>
#include <barrier>
#include <cstddef>
#include <memory>
#include <thread>
#include <vector>

#include "adversarial_support.hpp"

int main() {
  constexpr unsigned rounds = 16, slots = 8, producers = 4;
  constexpr unsigned jobs = rounds * slots * producers;
  struct Storage { alignas(dagflow::Pool) std::byte bytes[sizeof(dagflow::Pool)]; };
  struct Payload {
    std::atomic<unsigned>& destroyed;
    ~Payload() { destroyed.fetch_add(1); }
  };
  struct Result { dagflow::Handle handle; unsigned id; bool failed; };
  std::array<Storage, slots> storage;
  std::array<dagflow::Pool*, slots> pools{};
  std::array<std::atomic<unsigned>, jobs> calls{}, children{}, destroyed{};
  std::barrier phase(producers + 1);
  std::vector<std::thread> threads;
  // These producers NEVER exit between generations. Their cached accounting
  // lanes must survive eight-pool cache collisions and recycled Pool addresses.
  for (unsigned p = 0; p < producers; ++p) {
    threads.emplace_back([&, p] {
      std::vector<Result> history;
      for (unsigned round = 0; round < rounds; ++round) {
        phase.arrive_and_wait(); // New objects have been constructed in place.
        for (unsigned offset = 0; offset < slots; ++offset) {
          const unsigned slot = (offset + p + round) % slots;
          auto* pool = pools[slot];
          const unsigned id = (round * slots + slot) * producers + p;
          const bool fail = id % 5 == 0;
          auto handle = pool->submit([&, pool, id, fail,
                                      payload = std::unique_ptr<Payload>(new Payload{destroyed[id]})] {
            CHECK(calls[id].fetch_add(1) == 0);
            pool->submit_detached([&, id] { CHECK(children[id].fetch_add(1) == 0); });
            if (fail) throw adversarial::Failure{id};
          });
          history.push_back({std::move(handle), id, fail});
        }
        phase.arrive_and_wait(); // No producer may touch storage during destroy.
        phase.arrive_and_wait(); // All pools have been destroyed, not just idle.
        for (const auto& result : history) {
          CHECK(result.handle.ready());
          if (result.failed) CHECK(adversarial::failure_id(result.handle) == result.id);
          else result.handle.rethrow_if_failed();
        }
      }
      // Old observer allocations are freed on the long-lived producer thread,
      // after every Pool generation and its OS worker threads have disappeared.
    });
  }
  for (unsigned round = 0; round < rounds; ++round) {
    for (unsigned slot = 0; slot < slots; ++slot) {
      auto cfg = adversarial::config(1 + (round + slot) % 2);
      cfg.shards = 5;
      cfg.worker_shards.assign(cfg.threads, (round + slot) % cfg.shards);
      pools[slot] = std::construct_at(reinterpret_cast<dagflow::Pool*>(storage[slot].bytes), cfg);
      CHECK(!pools[slot]->closed());
    }
    phase.arrive_and_wait();
    phase.arrive_and_wait();
    for (unsigned slot = slots; slot > 0; --slot) std::destroy_at(pools[slot - 1]);
    for (unsigned id = round * slots * producers; id < (round + 1) * slots * producers; ++id)
      CHECK(calls[id] == 1 && children[id] == 1 && destroyed[id] == 1);
    phase.arrive_and_wait();
  }
  for (auto& thread : threads) thread.join();
}
