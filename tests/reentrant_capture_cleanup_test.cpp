#include <atomic>
#include <latch>
#include <memory>
#include <vector>

#include "adversarial_support.hpp"

namespace {
void failing_cleanup(unsigned workers) {
  dagflow::Pool pool(adversarial::config(workers));
  std::atomic<unsigned> parents{0}, children{0}, grandchildren{0};
  std::latch entered(1), release(1);
  struct ChildCapture {
    dagflow::Pool& pool;
    std::atomic<unsigned> &children, &grandchildren;
    ~ChildCapture() {
      children.fetch_add(1);
      auto* count = &grandchildren;
      pool.submit_detached([count] { CHECK(count->fetch_add(1) == 0); });
    }
  };
  struct ParentCapture {
    dagflow::Pool& pool;
    std::atomic<unsigned> &parents, &children, &grandchildren;
    ~ParentCapture() {
      CHECK(pool.closed());
      auto child = pool.submit([capture = std::unique_ptr<ChildCapture>(
                                   new ChildCapture{pool, children, grandchildren})] {
        throw adversarial::Failure{2};
      });
      pool.wait(child); // One-worker configuration must help from a destructor.
      CHECK(adversarial::failure_id(child) == 2);
      CHECK(children == 1);
      CHECK(parents.fetch_add(1) == 0);
    }
  };
  auto parent = pool.submit([&, capture = std::unique_ptr<ParentCapture>(
                                   new ParentCapture{pool, parents, children, grandchildren})] {
    entered.count_down();
    release.wait();
    throw adversarial::Failure{1};
  });
  entered.wait();
  pool.close();
  release.count_down();
  pool.wait(parent);
  CHECK(adversarial::failure_id(parent) == 1);
  CHECK(parents == 1 && children == 1);
  pool.shutdown();
  CHECK(grandchildren == 1);
}

void range_cleanup(bool stealing) {
  dagflow::Pool pool(adversarial::config());
  std::vector<unsigned> values(2 * DAGFLOW_DEFAULT_RANGE_CHUNK + 1);
  std::atomic<unsigned> cleanups{0};
  dagflow::Handle range;
  struct Capture {
    dagflow::Pool& pool;
    dagflow::Handle& range;
    std::vector<unsigned>& values;
    std::atomic<unsigned>& cleanups;
    ~Capture() {
      CHECK(range.valid() && !range.ready());
      auto child = pool.submit([this] {
        for (auto value : values) CHECK(value == 1);
      });
      pool.wait(child);
      child.rethrow_if_failed();
      CHECK(cleanups.fetch_add(1) == 0);
    }
  };
  // Only worker publishes the range, so its handle is assigned before any
  // chunk can execute and destroy the last callable owner.
  auto parent = pool.submit([&] {
    auto fn = [capture = std::unique_ptr<Capture>(new Capture{pool, range, values, cleanups})]
              (unsigned& value) { ++value; };
    if (stealing) range = pool.for_each_ws(values, std::move(fn));
    else range = pool.for_each(values, std::move(fn));
    pool.close();
  });
  pool.wait(parent);
  parent.rethrow_if_failed();
  pool.wait(range);
  range.rethrow_if_failed();
  CHECK(cleanups == 1);
  pool.shutdown();
}
}

int main() {
  failing_cleanup(1);
  failing_cleanup(4);
  range_cleanup(false);
  range_cleanup(true);
}
