#include <atomic>
#include <barrier>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

#include <dagflow/detail/chase_lev_deque.hpp>
#include <dagflow/detail/ring_mpmc.hpp>
#include "support.hpp"

using dagflow::detail::chase_lev_deque;
using dagflow::detail::ring_mpmc;

void deque_boundaries() {
  chase_lev_deque<int, 4> q;
  int out = -1;
  for (int round = 0; round < 1000; ++round) {
    CHECK(!q.try_pop(out));
    CHECK(!q.try_steal(out));
    CHECK(out == -1);
    CHECK(q.free_capacity() == 4);
    for (int i = 0; i < 4; ++i) CHECK(q.try_push(i));
    CHECK(q.free_capacity() == 0);
    CHECK(!q.try_push(99));
    CHECK(q.try_steal(out) && out == 0);
    CHECK(q.try_pop(out) && out == 3);
    CHECK(q.try_steal(out) && out == 1);
    CHECK(q.try_pop(out) && out == 2);
    CHECK(q.empty());
    out = -1;
  }
}

void deque_last_element() {
  chase_lev_deque<int, 2> q;
  std::barrier phase(3);
  std::atomic<int> successes{0};
  constexpr int rounds = 5000;
  auto thief = [&] {
    for (int i = 0; i < rounds; ++i) {
      phase.arrive_and_wait();
      int out = -1;
      if (q.try_steal(out)) {
        CHECK(out == i);
        successes.fetch_add(1, std::memory_order_relaxed);
      } else
        CHECK(out == -1);
      phase.arrive_and_wait();
    }
  };
  std::thread a(thief), b(thief);
  for (int i = 0; i < rounds; ++i) {
    CHECK(q.try_push(i));
    phase.arrive_and_wait();
    int out = -1;
    if (q.try_pop(out)) {
      CHECK(out == i);
      successes.fetch_add(1, std::memory_order_relaxed);
    } else
      CHECK(out == -1);
    phase.arrive_and_wait();
    CHECK(successes.exchange(0) == 1);
    CHECK(q.empty());
  }
  a.join();
  b.join();
}

struct Item {
  std::size_t id;
  std::uint64_t payload;
};

void deque_wraparound_contention() {
  constexpr std::size_t count = 100000;
  chase_lev_deque<Item*, 8> q;
  std::vector<Item> items(count);
  std::vector<std::atomic<unsigned>> seen(count);
  std::atomic<std::size_t> consumed{0};
  std::atomic<bool> finished{false};
  auto consume = [&](Item* item) {
    CHECK(item >= items.data() && item < items.data() + count);
    CHECK(item->payload == (item->id ^ 0x9e3779b97f4a7c15ULL));
    CHECK(seen[item->id].fetch_add(1, std::memory_order_relaxed) == 0);
    consumed.fetch_add(1, std::memory_order_relaxed);
  };
  std::vector<std::thread> thieves;
  for (int i = 0; i < 4; ++i)
    thieves.emplace_back([&] {
      while (!finished.load(std::memory_order_acquire)) {
        Item* item = nullptr;
        if (q.try_steal(item))
          consume(item);
        else
          std::this_thread::yield();
      }
    });
  for (std::size_t i = 0; i < count; ++i) {
    items[i] = {i, i ^ 0x9e3779b97f4a7c15ULL};
    while (!q.try_push(&items[i])) {
      Item* item = nullptr;
      if (q.try_pop(item)) consume(item);
    }
    if (i % 3 == 0) {
      Item* item = nullptr;
      if (q.try_pop(item)) consume(item);
    }
  }
  while (consumed.load(std::memory_order_acquire) != count) {
    Item* item = nullptr;
    if (q.try_pop(item))
      consume(item);
    else
      std::this_thread::yield();
  }
  finished.store(true, std::memory_order_release);
  for (auto& t : thieves) t.join();
  CHECK(q.empty());
  for (const auto& n : seen) CHECK(n.load() == 1);
}

void ring_boundaries() {
  ring_mpmc<int, 2> q;
  static_assert(decltype(q)::capacity() == 2);
  for (int round = 0; round < 1000; ++round) {
    int value = -1;
    CHECK(!q.try_pop(value));
    CHECK(value == -1);
    CHECK(q.try_push(1));
    CHECK(q.try_push(2));
    CHECK(!q.try_push(3));
    CHECK(q.try_pop(value) && value == 1);
    CHECK(q.try_push(3));
    CHECK(q.try_pop(value) && value == 2);
    CHECK(q.try_pop(value) && value == 3);
    CHECK(!q.try_pop(value) && value == 3);
    CHECK(q.empty());
  }
}

// Pause a producer after it reserves a slot, but before it publishes data.
struct GatedValue {
  int value{};
  std::atomic<bool>* entered{};
  std::atomic<bool>* release{};
  GatedValue() = default;
  GatedValue(int v, std::atomic<bool>* e = nullptr,
             std::atomic<bool>* r = nullptr)
      : value(v), entered(e), release(r) {}
  GatedValue(GatedValue&&) noexcept = default;
  GatedValue& operator=(GatedValue&& other) noexcept {
    if (other.entered) {
      other.entered->store(true, std::memory_order_release);
      other.entered->notify_one();
      other.release->wait(false, std::memory_order_acquire);
    }
    value = other.value;
    entered = nullptr;
    release = nullptr;
    return *this;
  }
};

void ring_delayed_publication() {
  ring_mpmc<GatedValue, 4> q;
  std::atomic<bool> entered{false}, release{false};
  std::thread producer(
      [&] { CHECK(q.try_push(GatedValue{1, &entered, &release})); });
  entered.wait(false, std::memory_order_acquire);
  CHECK(q.try_push(GatedValue{2}));
  GatedValue first, second;
  CHECK(!q.try_pop(first));  // Unpublished head: no bypass and no waiting.
  release.store(true, std::memory_order_release);
  release.notify_one();
  producer.join();
  CHECK(q.try_pop(first) && first.value == 1);
  CHECK(q.try_pop(second) && second.value == 2);
  CHECK(!q.try_pop(second) && second.value == 2);
}

void ring_contention() {
  constexpr std::size_t producers = 4, per_producer = 25000;
  constexpr std::size_t count = producers * per_producer;
  ring_mpmc<Item*, 8> q;
  std::vector<Item> items(count);
  std::vector<std::atomic<unsigned>> seen(count);
  std::atomic<std::size_t> consumed{0};
  std::vector<std::thread> threads;
  for (std::size_t p = 0; p < producers; ++p)
    threads.emplace_back([&, p] {
      for (std::size_t j = 0; j < per_producer; ++j) {
        const auto i = p * per_producer + j;
        items[i] = {i, i ^ 0x9e3779b97f4a7c15ULL};
        while (!q.try_push(&items[i])) std::this_thread::yield();
      }
    });
  for (int c = 0; c < 4; ++c)
    threads.emplace_back([&] {
      while (consumed.load(std::memory_order_acquire) != count) {
        Item* item = nullptr;
        if (q.try_pop(item)) {
          CHECK(item->id < count);
          CHECK(item->payload == (item->id ^ 0x9e3779b97f4a7c15ULL));
          CHECK(seen[item->id].fetch_add(1, std::memory_order_relaxed) == 0);
          consumed.fetch_add(1, std::memory_order_release);
        } else
          std::this_thread::yield();
      }
    });
  for (auto& t : threads) t.join();
  for (const auto& n : seen) CHECK(n.load() == 1);
  CHECK(q.empty());
}

int main() {
  deque_boundaries();
  deque_last_element();
  deque_wraparound_contention();
  ring_boundaries();
  ring_delayed_publication();
  ring_contention();
  std::puts("queue tests passed");
}
