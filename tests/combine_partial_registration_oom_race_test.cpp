#include <array>
#include <barrier>
#include <memory>
#include <thread>

#include "adversarial_allocation.hpp"
#include "adversarial_support.hpp"

int main() {
  using Credit = dagflow::detail::CompletionCredit;
  constexpr unsigned count = 8;
  struct Payload {
    unsigned& destroyed;
    ~Payload() { CHECK(++destroyed == 1); }
  };
  {
    dagflow::Pool pool(adversarial::config());
    const auto baseline = adversarial::allocation::live.load();
    // One result allocation, then one dependent-vector allocation per source.
    // Every prefix, including no installed edges, is abandoned on failure.
    for (int budget = 1; budget <= int(count); ++budget) {
      for (unsigned round = 0; round < 8; ++round) {
        {
          std::array<unsigned, count> destroyed{};
          std::array<Credit, count> credits;
          std::array<dagflow::Handle, count> handles;
          std::array<std::exception_ptr, count> errors;
          for (unsigned i = 0; i < count; ++i) {
            credits[i] = Credit::create(std::unique_ptr<Payload>(new Payload{destroyed[i]}));
            handles[i] = credits[i].handle();
            errors[i] = std::make_exception_ptr(adversarial::Failure{i});
          }
          std::barrier race(3);
          std::array<std::thread, 2> retirees;
          for (unsigned worker = 0; worker < retirees.size(); ++worker) {
            retirees[worker] = std::thread([&, worker] {
              race.arrive_and_wait();
              for (unsigned offset = worker; offset < count; offset += 2) {
                const unsigned i = round % 2 ? count - 1 - offset : offset;
                credits[i].fail(errors[i]);
                credits[i].finish();
              }
            });
          }
          adversarial::allocation::fault = {budget, [](void* context) noexcept {
            // This may run with one source's dependent mutex held. Release
            // the retire threads, but never wait for their retirement here.
            static_cast<std::barrier<>*>(context)->arrive_and_wait();
          }, &race};
          bool failed = false;
          try { (void)pool.combine(handles); }
          catch (const std::bad_alloc&) { failed = true; }
          adversarial::allocation::fault = {};
          CHECK(failed);
          for (auto& thread : retirees) thread.join();
          for (unsigned i = 0; i < count; ++i) {
            CHECK(destroyed[i] == 1);
            CHECK(adversarial::error(handles[i]) == errors[i]);
          }
          auto recovery = pool.combine(handles);
          CHECK(recovery.ready());
          CHECK(adversarial::error(recovery) == errors[0]);
        }
        // Includes the unobservable, partly registered result and its edges.
        CHECK(adversarial::allocation::live == baseline);
      }
    }
  }
  CHECK(adversarial::allocation::live == 0);
}
