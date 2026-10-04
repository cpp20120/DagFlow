#include <iostream>
#include <latch>
#include <thread>

#include <dagflow/dagflow.hpp>

int main() {
  dagflow::Pool pool({.threads = 2, .pin_threads = false});
  std::latch started(1);
  bool stopped = false;
  bool child_accepted = true;
  dagflow::TaskScope scope(pool);
  scope.spawn([&](dagflow::TaskScope::Context& ctx) {
    started.count_down();
    // In real work, check cancellation between bounded pieces of computation.
    while (!ctx.cancelled()) std::this_thread::yield();
    child_accepted = ctx.spawn([] {});
    stopped = true;  // An already running callback finishes cooperatively.
  });
  started.wait();  // Demonstrate cancellation of an actually running callback.
  scope.cancel();
  const bool external_accepted = scope.spawn([] {});
  scope.join();    // Cancellation alone does not throw; task errors still do.
  std::cout << "Stopped: " << stopped << "; child accepted: " << child_accepted
            << "; external accepted: " << external_accepted << '\n';
  return stopped && !child_accepted && !external_accepted ? 0 : 1;
}
