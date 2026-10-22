#include <set>

#include "schedule_exploration_support.hpp"
#include "support.hpp"

int main() {
  namespace schedule = dagflow::test::schedule;
  std::set<schedule::Trace> observed;
  schedule::explore("enumerator-two-by-two", 2, [&](schedule::Controller& c) {
    schedule::Trace actual;
    auto first = c.actor(0, [&] {
      actual.push_back(0);
      c.checkpoint("second operation");
      actual.push_back(0);
    });
    auto second = c.actor(1, [&] {
      actual.push_back(1);
      c.checkpoint("second operation");
      actual.push_back(1);
    });
    c.drive();
    first.join();
    second.join();
    c.require(observed.insert(actual).second,
              "enumerator never repeats a history");
  });
  schedule::verify_selection();
  if (!std::getenv("DAGFLOW_SCHEDULE_TRACE")) {
    const std::set<schedule::Trace> expected{{0, 0, 1, 1}, {0, 1, 0, 1},
                                             {0, 1, 1, 0}, {1, 0, 0, 1},
                                             {1, 0, 1, 0}, {1, 1, 0, 0}};
    CHECK(observed == expected);
  }
}
