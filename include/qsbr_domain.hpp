#pragma once
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include "config.hpp"

namespace dagflow::detail {

/// @brief Epoch-based memory reclamation domain (Quiescent State-Based Reclamation).
///
/// `qsbr_domain` implements QSBR: deferred memory reclamation that is safe
/// for lock-free data structures. Threads periodically declare quiescent
/// points (moments when they hold no pointers into shared structures), which
/// allows the domain to advance a global epoch and free memory retired in
/// earlier epochs.
///
/// ### Lifecycle
/// 1. Each participating thread calls `acquire_thread_rec()` once to obtain
///    a `thread_rec*` bound to that thread.
/// 2. At safe points (e.g., between task executions) call `quiescent()` to
///    publish that the thread has observed the current epoch.
/// 3. When removing a node from a lock-free structure, call `retire()` to
///    schedule deferred deletion.
/// 4. Reclamation happens inside `try_advance_and_reclaim()` once every thread
///    has passed a quiescent point at or beyond the epoch under which the
///    record was retired.
///
/// Use `qsbr_section` as a RAII helper to call `quiescent()` on scope exit.
class qsbr_domain {
 public:
  /// Minimum number of retired records that triggers a reclamation attempt.
  static constexpr size_t kReclaimThreshold = 512;

  /// @brief A single deferred-deletion record.
  struct retire_record {
    void* p{};                ///< Pointer to the object to free.
    void (*deleter)(void*){}; ///< Type-erased deleter (typically `operator delete`).
    uint64_t epoch{};         ///< Global epoch at the time `retire()` was called.
  };

  /// @brief Per-thread registration state.
  ///
  /// Holds the thread's slot index into the global epoch array, its private
  /// list of retired records not yet freed, and a local operation counter used
  /// to amortize the cost of reclamation checks.
  struct thread_rec {
    size_t idx{};                          ///< Index into `epochs_` for this thread.
    std::vector<retire_record> retired;    ///< Records pending reclamation.
    std::atomic<uint64_t> local_epoch{0}; ///< Last epoch published by this thread.
    uint32_t local_ops{0};                ///< Operation counter; triggers checks every 256 ops.
  };

  /// @brief Returns the process-wide singleton domain.
  static qsbr_domain& instance() {
    static qsbr_domain dom;
    return dom;
  }

  /// @brief Registers the calling thread and returns its `thread_rec`.
  ///
  /// Thread-local: safe to call repeatedly; subsequent calls return the
  /// cached pointer without re-registering.
  ///
  /// @return Non-owning pointer to the thread's registration record.
  thread_rec* acquire_thread_rec() {
    thread_local thread_rec* tr = nullptr;
    if (tr) return tr;

    auto idx = register_thread();
    tr = new thread_rec{};
    tr->idx = idx;

    uint64_t const g = global_epoch_.load(std::memory_order_relaxed);
    tr->local_epoch.store(g, std::memory_order_relaxed);
    epochs_[idx].val.store(g, std::memory_order_relaxed);
    return tr;
  }

  /// @brief Declares a quiescent point for the calling thread.
  ///
  /// Publishes the current global epoch to this thread's slot, signalling
  /// to other threads that no pointer from a previous epoch is live.
  /// Every 256 calls, also attempts to advance the global epoch and reclaim
  /// retired objects if the retired list has reached `kReclaimThreshold`.
  ///
  /// @param tr  Thread record obtained from `acquire_thread_rec()`.
  void quiescent(thread_rec* tr) {
    uint64_t g = global_epoch_.load(std::memory_order_relaxed);
    epochs_[tr->idx].val.store(g, std::memory_order_release);

    if ((++tr->local_ops & 0xFFu) == 0 &&
        tr->retired.size() >= kReclaimThreshold) {
      try_advance_and_reclaim(tr);
    }
  }

  /// @brief Schedules @p p for deferred deletion.
  ///
  /// The object will be freed only once every registered thread has
  /// passed a quiescent point at an epoch greater than the one recorded here.
  ///
  /// @param tr      Thread record for the calling thread.
  /// @param p       Pointer to the object to retire.
  /// @param deleter Callable that frees @p p (e.g., `[](void* x){ delete static_cast<T*>(x); }`).
  void retire(thread_rec* tr, void* p, void (*deleter)(void*)) {
    auto e = global_epoch_.load(std::memory_order_acquire);
    tr->retired.push_back({p, deleter, e});
    if (tr->retired.size() >= kReclaimThreshold) {
      try_advance_and_reclaim(tr);
    }
  }

  /// @brief RAII guard that calls `quiescent()` on the owning domain when destroyed.
  ///
  /// Wrap a section of code that does not hold live pointers into shared
  /// lock-free structures:
  /// @code
  ///   {
  ///     qsbr_section qs(tr);
  ///     // ... safe point, no shared pointers held ...
  ///   } // quiescent() called here
  /// @endcode
  struct qsbr_section {
    thread_rec* tr{}; ///< Thread record passed at construction.

    /// @param r Thread record for the calling thread.
    explicit qsbr_section(thread_rec* r) : tr(r) {}

    /// Calls `qsbr_domain::instance().quiescent(tr)`.
    ~qsbr_section() { qsbr_domain::instance().quiescent(tr); }
  };

 private:
  /// @brief Cache-line-padded atomic epoch slot to eliminate false sharing.
  struct alignas(CACHE_LINE_SIZE) atomic_epoch {
    std::atomic<uint64_t> val;
    uint8_t pad[CACHE_LINE_SIZE - sizeof(std::atomic<uint64_t>)];

    atomic_epoch() noexcept : val(0) {}

    atomic_epoch(const atomic_epoch&) = delete;
    atomic_epoch& operator=(const atomic_epoch&) = delete;

    atomic_epoch(atomic_epoch&& other) noexcept {
      val.store(other.val.load(std::memory_order_relaxed),
                std::memory_order_relaxed);
    }

    atomic_epoch& operator=(atomic_epoch&& other) noexcept {
      val.store(other.val.load(std::memory_order_relaxed),
                std::memory_order_relaxed);
      return *this;
    }
  };

  /// Constructs the domain, pre-sizing the epoch table to `hardware_concurrency`.
  qsbr_domain() {
    auto n = std::thread::hardware_concurrency();
    epochs_.reserve(n);
    epochs_.resize(n);
    for (auto& e : epochs_) {
      e.val.store(0, std::memory_order_relaxed);
    }
  }

  /// @brief Assigns the next available index in `epochs_` to a new thread.
  /// Grows the vector if needed.
  /// @return The assigned index.
  size_t register_thread() {
    size_t id = next_idx_.fetch_add(1, std::memory_order_relaxed);
    if (id >= epochs_.size()) {
      epochs_.resize(id + 1);
      epochs_[id].val.store(0, std::memory_order_relaxed);
    }
    return id;
  }

  /// @brief Tries to advance the global epoch and reclaim safe records.
  ///
  /// Scans all epoch slots to find the minimum observed epoch. If every thread
  /// is up to date (`min_epoch >= g`), increments the global epoch via CAS.
  /// Then frees all retired records whose epoch is strictly less than
  /// `min_epoch`, keeping the rest for a future pass.
  ///
  /// @param tr  The calling thread's record (its `retired` list is compacted).
  void try_advance_and_reclaim(thread_rec* tr) {
    uint64_t g = global_epoch_.load(std::memory_order_acquire);
    uint64_t min_epoch = g;

    for (auto& e : epochs_) {
      uint64_t const v = e.val.load(std::memory_order_acquire);
      min_epoch = std::min(min_epoch, v);
    }

    if (min_epoch >= g) {
      global_epoch_.compare_exchange_strong(g, g + 1, std::memory_order_acq_rel,
                                            std::memory_order_relaxed);
    }

    std::vector<retire_record> keep;
    keep.reserve(tr->retired.size());
    for (auto& r : tr->retired) {
      if (r.epoch < min_epoch) {
        r.deleter(r.p);
      } else {
        keep.push_back(r);
      }
    }
    tr->retired.swap(keep);
  }

  std::atomic<uint64_t> global_epoch_{1};          ///< Monotonically increasing epoch counter.
  std::atomic<size_t> next_idx_{0};                ///< Next free slot in `epochs_`.
  std::vector<atomic_epoch> epochs_;               ///< Per-thread last-observed epoch (cache-line padded).
  std::atomic<size_t> ops_since_last_check_{0};    ///< (Reserved) global op counter, unused by current logic.
};

/// @brief Convenience alias for `qsbr_domain::qsbr_section`.
using qsbr_section = qsbr_domain::qsbr_section;

}  // namespace dagflow::detail
