#include <array>
#include <atomic>
#include <barrier>
#include <latch>
#include <memory>
#include <thread>
#include <vector>

#include "adversarial_support.hpp"

namespace {
constexpr unsigned writers = 8;
struct Payload {
  std::array<unsigned, writers>& writes;
  std::atomic<unsigned>& destroyed;
  std::latch &entered, &release;
  ~Payload() {
    // Plain writes must be visible through the credit retirement release chain.
    for (unsigned i = 0; i < writers; ++i) CHECK(writes[i] == i + 1);
    entered.count_down();
    release.wait();
    CHECK(destroyed.fetch_add(1) == 0);
  }
};
}

int main() {
  using Credit = dagflow::detail::CompletionCredit;
  dagflow::Pool pool(adversarial::config());
  for (unsigned round = 0; round < 24; ++round) {
    std::array<unsigned, writers> writes{};
    std::atomic<unsigned> destroyed{0};
    std::latch entered(1), release(1);
    auto root = Credit::create(std::unique_ptr<Payload>(new Payload{writes, destroyed, entered, release}));
    const auto source = root.handle();
    auto tail = pool.combine({source, {}, source});
    for (unsigned i = 0; i < 32; ++i) tail = pool.combine({tail, tail});
    if (round % 2) root.fail(std::make_exception_ptr(adversarial::Failure{round}));
    std::barrier start(writers + 5);
    std::vector<std::thread> threads;
    for (unsigned i = 0; i < writers; ++i) {
      threads.emplace_back([&, i, credit = root.fork()]() mutable {
        start.arrive_and_wait();
        writes[i] = i + 1;
        if ((i + round) % 2) std::this_thread::yield();
        credit.finish();
        credit.finish(); // Finished owners must not retire twice.
      });
    }
    for (unsigned i = 0; i < 2; ++i) {
      threads.emplace_back([&] {
        start.arrive_and_wait();
        for (unsigned j = 0; j < 256; ++j) {
          auto copy = source;
          auto moved = std::move(copy);
          CHECK(!copy.valid());
          copy = moved;
          moved = {};
        }
      });
      threads.emplace_back([&] {
        start.arrive_and_wait();
        pool.wait(tail);
        CHECK(destroyed.load() == 1);
        for (unsigned j = 0; j < writers; ++j) CHECK(writes[j] == j + 1);
      });
    }
    root.finish(); // The last credit now belongs to an unpredictable writer.
    start.arrive_and_wait();
    entered.wait();
    CHECK(!source.ready() && !tail.ready() && destroyed == 0);
    // Register while the terminal thread is inside user payload destruction.
    auto late = pool.combine({source, tail, source});
    CHECK(!late.ready());
    release.count_down();
    for (auto& thread : threads) thread.join();
    pool.wait(late);
    CHECK(source.ready() && tail.ready() && late.ready() && destroyed == 1);
    if (round % 2) CHECK(adversarial::failure_id(late) == round);
    else late.rethrow_if_failed();
  }
}
