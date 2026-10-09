#include <array>
#include <atomic>
#include <barrier>
#include <memory>
#include <thread>

#include "adversarial_support.hpp"

namespace {
struct Resource {
  dagflow::Pool& destination;
  dagflow::Handle sibling;
  std::atomic<bool>& source_destroyed;
  std::atomic<unsigned> &destroyed, &called;
  std::thread::id& releaser;
  ~Resource() {
    CHECK(source_destroyed && std::this_thread::get_id() == releaser);
    sibling = {}; // Nested completion-storage release from an exception owner.
    auto task = destination.submit([this] { CHECK(called.fetch_add(1) == 0); });
    destination.wait(task);
    task.rethrow_if_failed();
    CHECK(destroyed.fetch_add(1) == 0);
  }
};
struct Failure { std::shared_ptr<Resource> resource; };
}

int main() {
  dagflow::Pool destination(adversarial::config());
  for (unsigned round = 0; round < 24; ++round) {
    std::atomic<bool> source_destroyed{false};
    std::atomic<unsigned> destroyed{0}, called{0};
    std::thread::id releaser;
    std::weak_ptr<Resource> weak;
    std::array<dagflow::Handle, 8> observers;
    {
      dagflow::Pool source(adversarial::config(2));
      auto sibling = source.submit([] {});
      source.wait(sibling);
      auto resource = std::shared_ptr<Resource>(new Resource{
          destination, sibling, source_destroyed, destroyed, called, releaser});
      weak = resource;
      auto failed = source.submit([resource] { throw Failure{resource}; });
      source.wait(failed);
      resource.reset();
      auto fan_in = source.combine({failed, sibling, failed});
      source.wait(fan_in);
      for (auto& observer : observers) observer = source.combine({failed, fan_in, fan_in});
    }
    source_destroyed = true;
    CHECK(destroyed == 0 && !weak.expired());
    std::barrier start(observers.size());
    const auto verify = [](const dagflow::Handle& observer) {
      CHECK(observer.ready());
      bool caught = false;
      try { observer.rethrow_if_failed(); }
      catch (const Failure& failure) {
        CHECK(failure.resource && failure.resource->source_destroyed);
        caught = true;
      }
      CHECK(caught);
    };
    std::array<std::thread, observers.size() - 1> threads;
    for (unsigned i = 0; i < threads.size(); ++i) {
      threads[i] = std::thread([&, observer = std::move(observers[i + 1])]() mutable {
        start.arrive_and_wait();
        verify(observer);
        observer = {};
      });
    }
    std::thread last([&, observer = std::move(observers[0])]() mutable {
      start.arrive_and_wait();
      verify(observer);
      // Join also ends each thread's callable/catch/TLS lifetime before the
      // designated last observer releases the exception-owned resource.
      for (auto& thread : threads) thread.join();
      CHECK(destroyed == 0);
      releaser = std::this_thread::get_id();
      observer = {};
      CHECK(destroyed == 1 && called == 1 && weak.expired());
    });
    last.join();
    CHECK(destroyed == 1 && called == 1);
  }
}
