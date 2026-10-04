#include "dagflow/detail/idle_accounting.hpp"

#include <exception>
#include <limits>
#include <stdexcept>

namespace dagflow::detail {
namespace {
constexpr uint64_t first_identity = 1;
constexpr std::size_t producer_cache_slots = 4;

uint64_t unique_id() noexcept {
  static std::atomic<uint64_t> next_identity{first_identity};
  const auto value = next_identity.fetch_add(1, std::memory_order_relaxed);
  if (value == 0) std::terminate();  // Never reuse a cached identity.
  return value;
}
// Avoid sum overflow even when several lifetime counters approach UINT64_MAX.
struct Total {
  uint64_t low{0}, high{0};
  void add(uint64_t value) noexcept {
    const auto old = low;
    low += value;
    high += low < old;
  }
  bool operator==(const Total&) const = default;
};
}  // namespace

IdleAccounting::IdleAccounting(uint32_t workers)
    : identity_(unique_id()), workers_(make_owned_array<Lane>(workers)) {}
IdleAccounting::~IdleAccounting() {
  // Producer churn must not cause recursive list destruction.
  while (producers_) {
    auto next = std::move(producers_->next);
    producers_ = std::move(next);
  }
}
void IdleAccounting::publication_exhausted() {
  throw std::overflow_error("pool publication counter exhausted");
}
IdleAccounting::Lane& IdleAccounting::producer_lane() {
  struct Entry { uint64_t identity{0}; Lane* lane{nullptr}; };
  // Bounded TLS cache. Entries may outlive a pool, but are never dereferenced
  // without its unique identity matching. Cache eviction does not own a lane.
  static thread_local Entry cache[producer_cache_slots];
  auto& entry = cache[identity_ % producer_cache_slots];
  if (entry.identity == identity_) return *entry.lane;
  static thread_local const auto producer_id = unique_id();
  std::lock_guard lock(mutex_);
  for (auto* producer = producers_.get(); producer; producer = producer->next.get()) {
    if (producer->id == producer_id) {
      entry = {identity_, &producer->lane};
      return *entry.lane;
    }
  }
  auto producer = make_owned<Producer>(producer_id);
  producer->next = std::move(producers_);
  producers_ = std::move(producer);
  entry = {identity_, &producers_->lane};
  return *entry.lane;
}
void IdleAccounting::publish_external() { publish(producer_lane()); }
bool IdleAccounting::idle_locked() const noexcept {
  // Registry is fixed under mutex_. Retirements are monotonic: equal totals
  // in both collections mean every observed retirement lane stayed stable.
  // Acquire retirement first: each observed completion carries the publication
  // of its task (and any children published before its retirement).
  Total retired_before;
  Total published_total;
  Total retired_after;
  const auto count = workers_.get_deleter().count;
  for (std::size_t worker_index = 0; worker_index < count; ++worker_index)
    retired_before.add(
        workers_[worker_index].retired.load(std::memory_order_acquire));
  for (std::size_t worker_index = 0; worker_index < count; ++worker_index)
    published_total.add(
        workers_[worker_index].published.load(std::memory_order_acquire));
  for (auto* producer = producers_.get(); producer; producer = producer->next.get())
    published_total.add(
        producer->lane.published.load(std::memory_order_acquire));
  for (std::size_t worker_index = 0; worker_index < count; ++worker_index)
    retired_after.add(
        workers_[worker_index].retired.load(std::memory_order_acquire));
  return retired_before == retired_after && published_total == retired_after;
}
void IdleAccounting::worker_idle() {
  // Pair with wait()'s announcement fence. Either its snapshot observes our
  // retirement, or this probe sees the waiter and takes the mutex handshake.
  std::atomic_thread_fence(std::memory_order_seq_cst);
  if (waiters_.load(std::memory_order_relaxed) == 0) return;
  {
    std::lock_guard lock(mutex_);
  }
  cv_.notify_all();
}
void IdleAccounting::wait() {
  std::unique_lock lock(mutex_);
  waiters_.fetch_add(1, std::memory_order_relaxed);
  std::atomic_thread_fence(std::memory_order_seq_cst);
  try {
    while (!idle_locked()) cv_.wait(lock);
  } catch (...) {
    waiters_.fetch_sub(1, std::memory_order_relaxed);
    throw;
  }
  waiters_.fetch_sub(1, std::memory_order_relaxed);
}
}  // namespace dagflow::detail
