#include "dagflow/detail/parking_lot.hpp"

#include <bit>
#include <chrono>
#include <cstddef>

#if defined(__i386__) || defined(__x86_64__) || defined(_M_IX86) || defined(_M_X64)
#include <immintrin.h>
#endif

namespace dagflow::detail {
namespace {
// Stay registered during a short quiet interval, watching only our epoch.
// The active wait does not rescan topology; signalled spinners skip the CV.
constexpr unsigned epoch_spin_count = 128;
void spin_pause() noexcept {
#if defined(__i386__) || defined(__x86_64__) || defined(_M_IX86) || defined(_M_X64)
  _mm_pause();
#elif defined(__aarch64__) && (defined(__GNUC__) || defined(__clang__))
  asm volatile("yield");
#else
  std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}
constexpr uint32_t idle_word_bits = sizeof(uint64_t) * 8;
constexpr uint32_t idle_word_last_bit = idle_word_bits - 1;
constexpr uint64_t all_idle_bits = ~uint64_t{0};
}  // namespace

ParkingLot::ParkingLot(const Scheduler& topology)
    : members_(topology.worker_order()),
      waiters_(make_owned_array<Waiter>(topology.worker_count())),
      domains_(make_owned_array<Domain>(topology.shard_count())),
      idle_(make_owned_array<std::atomic<uint64_t>>(
          (uint64_t{topology.worker_count()} + idle_word_last_bit) /
          idle_word_bits)) {
  uint64_t worker_offset = 0;
  for (uint32_t shard_index = 0; shard_index < topology.shard_count();
       ++shard_index) {
    const auto shard_members = topology.members(shard_index);
    auto& domain = domains_[shard_index];
    if (!shard_members.empty()) {
      const auto last_worker_offset = worker_offset + shard_members.size() - 1;
      domain.first_word =
          static_cast<uint32_t>(worker_offset / idle_word_bits);
      domain.word_count = static_cast<uint32_t>(
          last_worker_offset / idle_word_bits -
          worker_offset / idle_word_bits + 1);
      domain.first_mask = all_idle_bits << (worker_offset % idle_word_bits);
      domain.last_mask =
          all_idle_bits >> (idle_word_last_bit -
                            last_worker_offset % idle_word_bits);
      if (domain.word_count == 1)
        domain.first_mask &= domain.last_mask;
    }
    for (auto worker : shard_members) {
      auto& waiter = waiters_[worker];
      waiter.word_index =
          static_cast<uint32_t>(worker_offset / idle_word_bits);
      waiter.bit = uint64_t{1} << (worker_offset % idle_word_bits);
      ++worker_offset;
    }
  }
  for (std::size_t word_index = 0;
       word_index < idle_.get_deleter().count; ++word_index)
    idle_[word_index].store(0, std::memory_order_relaxed);
}
uint64_t ParkingLot::prepare(uint32_t worker) noexcept {
  auto& waiter = waiters_[worker];
  const auto epoch = waiter.epoch.load(std::memory_order_seq_cst);
  idle_[waiter.word_index].fetch_or(waiter.bit, std::memory_order_seq_cst);
  // Pair with the publisher fence: registration precedes the final queue scan.
  std::atomic_thread_fence(std::memory_order_seq_cst);
  return epoch;
}
void ParkingLot::cancel(uint32_t worker) noexcept {
  auto& waiter = waiters_[worker];
  idle_[waiter.word_index].fetch_and(~waiter.bit, std::memory_order_seq_cst);
}
void ParkingLot::wait(uint32_t worker, uint64_t epoch, uint32_t timeout_us,
                      const std::atomic<bool>& stop) {
  auto& waiter = waiters_[worker];
  for (unsigned spin = 0; spin < epoch_spin_count; ++spin) {
    if (waiter.epoch.load(std::memory_order_acquire) != epoch ||
        stop.load(std::memory_order_acquire)) {
      cancel(worker);
      return;
    }
    spin_pause();
  }
  std::unique_lock lock(waiter.mutex);
  // SC pairs with signal's epoch increment followed by its sleeping probe:
  // either this predicate observes the signal or the notifier takes the mutex.
  waiter.sleeping.store(true, std::memory_order_seq_cst);
  runtime_count(RuntimeEvent::park_call);
  try {
    const bool signalled = waiter.cv.wait_for(lock, std::chrono::microseconds(timeout_us), [&] {
      return stop.load(std::memory_order_acquire) ||
             waiter.epoch.load(std::memory_order_seq_cst) != epoch;
    });
    if (!signalled) runtime_count(RuntimeEvent::park_timeout);
  } catch (...) {
    waiter.sleeping.store(false, std::memory_order_seq_cst);
    cancel(worker);
    throw;
  }
  waiter.sleeping.store(false, std::memory_order_seq_cst);
  cancel(worker);
}
void ParkingLot::signal(uint32_t worker, bool claimed) {
  auto& waiter = waiters_[worker];
  // Clear exactly the registration being notified, before changing the epoch.
  // A claimed bit must not be cleared again: the owner may already have
  // registered for a later wait, which needs to remain visible to publishers.
  if (!claimed)
    claimed =
        (idle_[waiter.word_index].fetch_and(
             ~waiter.bit, std::memory_order_seq_cst) &
         waiter.bit) != 0;
  waiter.epoch.fetch_add(1, std::memory_order_seq_cst);
  if (!claimed) return;
  runtime_count(RuntimeEvent::wake_signal);
  // Announced workers that are scanning/spinning observe the epoch directly.
  // The SC handshake above prevents skipping notification of a real sleeper.
  if (!waiter.sleeping.load(std::memory_order_seq_cst)) return;
  // Close the interval between predicate checking and cv.wait releasing mutex.
  {
    std::lock_guard lock(waiter.mutex);
  }
  waiter.cv.notify_one();
}
bool ParkingLot::wake_word(uint32_t word_index, uint64_t mask) {
  auto& idle_word = idle_[word_index];
  auto bits = idle_word.load(std::memory_order_relaxed);
  while (const auto eligible = bits & mask) {
    const auto bit = std::countr_zero(eligible);
    if (idle_word.compare_exchange_weak(bits,
                                        bits & ~(uint64_t{1} << bit),
                                        std::memory_order_seq_cst)) {
      signal(members_[std::size_t{word_index} * idle_word_bits + bit], true);
      return true;
    }
  }
  return false;
}
bool ParkingLot::wake_domain(uint32_t shard) {
  const auto& domain = domains_[shard];
  for (uint32_t domain_word_index = 0;
       domain_word_index < domain.word_count;
       ++domain_word_index) {
    auto mask = domain_word_index == 0 ? domain.first_mask : all_idle_bits;
    if (domain_word_index + 1 == domain.word_count)
      mask &= domain.last_mask;
    if (wake_word(domain.first_word + domain_word_index, mask)) return true;
  }
  return false;
}
void ParkingLot::wake_one(uint32_t shard) {
  runtime_count(RuntimeEvent::wake_call);
  // Publication precedes the idle probe. Together with prepare's fence this
  // prevents both publisher and waiter from missing each other's announcement.
  std::atomic_thread_fence(std::memory_order_seq_cst);
  if (idle_.get_deleter().count == 1) {
    // Up to 64 workers: one probe, precomputed domain mask, no topology walk.
    auto bits = idle_[0].load(std::memory_order_relaxed);
    while (bits) {
      const auto preferred = bits & domains_[shard].first_mask;
      const auto bit = std::countr_zero(preferred ? preferred : bits);
      if (idle_[0].compare_exchange_weak(bits, bits & ~(uint64_t{1} << bit),
                                       std::memory_order_seq_cst)) {
        signal(members_[bit], true);
        return;
      }
    }
    return;
  }
  if (wake_domain(shard)) return;
  // Empty or busy domains can recruit a remote worker. Scan compact words,
  // not domains: domain count may exceed worker count by an arbitrary factor.
  for (std::size_t word_index = 0;
       word_index < idle_.get_deleter().count; ++word_index)
    if (wake_word(static_cast<uint32_t>(word_index), all_idle_bits)) return;
}
void ParkingLot::wake_all() {
  for (uint32_t id = 0; id < members_.size(); ++id) signal(id, false);
}
}  // namespace dagflow::detail
