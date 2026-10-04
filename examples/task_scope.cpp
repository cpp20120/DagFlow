#include <array>
#include <iostream>
#include <stdexcept>

#include <dagflow/dagflow.hpp>

int main() {
  dagflow::Pool pool({.threads = 2, .pin_threads = false});
  std::array<int, 3> results{};  // Declare borrowed state before its scope.
  dagflow::TaskScope scope(pool);
  scope.spawn([&](dagflow::TaskScope::Context& ctx) {
    results[0] = 10;
    // Use the callback's Context for descendants; never retain Context itself.
    ctx.spawn([&](dagflow::TaskScope::Context& child) {
      results[1] = 20;
      child.spawn([&] { results[2] = 30; });
    });
  });
  auto done = scope.close();  // External admission stops; descendants may spawn.
  const bool accepted_after_close = scope.spawn([] {});
  scope.join();              // Waits for descendants and rethrows task failures.
  std::cout << "Joined: " << results[0] + results[1] + results[2]
            << "; ready: " << done.ready()
            << "; accepted after close: " << accepted_after_close << '\n';

  bool caught = false;
  try {
    dagflow::TaskScope failing(pool);
    failing.spawn([] { throw std::runtime_error("scope task failed"); });
    failing.join();
  } catch (const std::runtime_error& error) {
    caught = true;
    std::cout << "Caught: " << error.what() << '\n';
  }
  // Scope destruction also closes and joins, but does not rethrow errors.
  return results == std::array{10, 20, 30} && !accepted_after_close && caught ? 0 : 1;
}
