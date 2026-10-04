#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>

#include <dagflow/dagflow.hpp>

RC_GTEST_PROP(DagFlowCapability, TaskRoundTrip, (int expected)) {
  dagflow::Pool pool({.threads = 2, .pin_threads = false});
  dagflow::TaskScope scope(pool);
  int actual = 0;
  scope.spawn([&] { actual = expected; });
  scope.join();
  RC_ASSERT(actual == expected);
}
