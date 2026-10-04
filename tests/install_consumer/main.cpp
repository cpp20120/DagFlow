#include <dagflow/dagflow.hpp>

int main() {
  dagflow::Pool pool({.threads = 2, .pin_threads = false});
  dagflow::TaskScope scope(pool);
  int value = 0;
  scope.spawn([&] { value = 42; });
  scope.join();
  return value == 42 ? 0 : 1;
}
