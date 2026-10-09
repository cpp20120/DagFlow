
#include <array>
#include <atomic>
#include <barrier>
#include <memory>
#include <thread>
#include <vector>

#include "adversarial_support.hpp"

namespace {
constexpr unsigned sources = 8;

struct Observation {
  dagflow::Handle handle;
  unsigned mask;
};

struct Payload {
  std::atomic<unsigned>& destroyed;
  ~Payload() { CHECK(destroyed.fetch_add(1) == 0); }
};

void verify(const Observation& observation, unsigned failed) {
  CHECK(observation.handle.ready());

  if (!(observation.mask & failed)) {
    observation.handle.rethrow_if_failed();
    return;
  }

  const auto id = adversarial::failure_id(observation.handle);
  CHECK(id < sources && (observation.mask & failed & (1u << id)));

  // The first failure ID must remain stable across repeated reads/copies.
  CHECK(adversarial::failure_id(observation.handle) == id);

  auto copy = observation.handle;
  CHECK(adversarial::failure_id(copy) == id);
}
}

int main() {
  using Credit = dagflow::detail::CompletionCredit;

  for (unsigned round = 0; round < 16; ++round) {
    std::array<std::atomic<unsigned>, sources> destroyed{};
    std::array<std::exception_ptr, sources> errors{};
    std::array<std::vector<Observation>, 2> late;
    std::vector<Observation> early;

    const unsigned failed = round % 4 == 0
        ? 0
        : (round % 2 ? 0xa5 : 0xff);

    {
      dagflow::Pool pool(adversarial::config());
      std::array<Credit, sources> credits;
      std::array<dagflow::Handle, sources> handles;

      for (unsigned i = 0; i < sources; ++i) {
        errors[i] = std::make_exception_ptr(adversarial::Failure{i});
        credits[i] = Credit::create(
            std::unique_ptr<Payload>(new Payload{destroyed[i]}));
        handles[i] = credits[i].handle();
        early.push_back({handles[i], 1u << i});
      }

      std::vector<dagflow::Handle> diamonds;
      for (unsigned i = 0; i < sources; ++i) {
        const unsigned next = (i + 1) % sources;
        auto joined = pool.combine({
            handles[i], {}, handles[next], handles[i]
        });
        early.push_back({joined, (1u << i) | (1u << next)});
        diamonds.push_back(joined);
        diamonds.push_back(joined);
      }

      early.push_back({pool.combine(diamonds), 0xff});

      std::barrier start(sources + 3);
      std::vector<std::thread> threads;

      for (unsigned i = 0; i < sources; ++i) {
        threads.emplace_back([&, i] {
          start.arrive_and_wait();
          if ((i + round) % 2) std::this_thread::yield();

          if (failed & (1u << i)) {
            credits[i].fail(errors[i]);
            credits[i].fail(std::make_exception_ptr(
                adversarial::Failure{sources + i}));
          }
          credits[i].finish();
        });
      }

      for (unsigned registrar = 0; registrar < late.size(); ++registrar) {
        threads.emplace_back([&, registrar] {
          start.arrive_and_wait();

          for (unsigned j = 0; j < 128; ++j) {
            const unsigned a = (j + registrar + round) % sources;
            const unsigned b = (j / sources + registrar * 3) % sources;

            auto join = pool.combine({
                handles[a], handles[b], {}, handles[a]
            });
            auto diamond = pool.combine({
                join, handles[b], join
            });
            late[registrar].push_back({
                std::move(diamond), (1u << a) | (1u << b)
            });
          }
        });
      }

      start.arrive_and_wait();
      for (auto& thread : threads) thread.join();

      for (const auto& observation : early)
        verify(observation, failed);

      for (auto& count : destroyed)
        CHECK(count == 1);
    }

    // Graphs of observers and their exception payloads outlive the scheduler.
    std::thread observer([&] {
      for (const auto& observation : early)
        verify(observation, failed);

      for (auto& list : late) {
        for (const auto& observation : list)
          verify(observation, failed);
        list.clear();
      }
      early.clear();
    });
    observer.join();
  }
}
