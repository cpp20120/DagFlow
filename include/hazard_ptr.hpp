#pragma once
/**
 * @file hazard_ptr.hpp
 * @brief Hazard Pointers (HP) and QSBR (Quiescent State Based Reclamation)
 * domain.
 *
 * @details
 * This header implements a combined memory reclamation domain supporting:
 *  - Hazard Pointers (HP): readers protect pointers from reclamation by
 * publishing them into per-thread slots. Reclamation scans all published
 * hazards and frees only those retired pointers that are not currently
 * protected.
 *  - QSBR: epoch-based reclamation where writers retire pointers with an epoch
 * stamp, and reclamation occurs once all threads have observed at least that
 * epoch.
 *
 * The domain is global (singleton). Each thread acquires a thread record that
 * contains its HP slots and the retire lists for both HP and QSBR.
 *
 * Usage:
 *  - For lock-free reads of shared pointers, wrap the load in an `hp_guard`,
 * store it into a slot via `set`, and only then dereference safely.
 *  - When removing a node, call either `retire_hp` or `retire_qsbr` with a
 * deleter.
 *  - Use `qsbr_section` RAII to publish quiescent state after a critical
 * section.
 *
 * Performance:
 *  - Reclamation is performed in batches of `kReclaimThreshold`.
 *  - Epoch advancement is attempted opportunistically.
 */

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace tp::detail {
/**
 * @class hazard_domain
 * @brief Global domain that manages hazard pointer slots and QSBR epochs.
 *
 * @details
 * Threads register by acquiring a `thread_rec`. Each record points to its
 * `slot_block`, which contains a fixed number of hazard slots (`kHPPerThread`).
 * The domain maintains a registry of active blocks and their last published
 * epochs for QSBR.
 */
class hazard_domain {
 public:
  /// Number of hazard pointer slots per thread record.
  static constexpr size_t kHPPerThread = 2;
  /// Batch size before attempting reclamation.
  static constexpr size_t kReclaimThreshold = 128;
  /**
   * @struct slot_block
   * @brief A fixed-size array of hazard pointer slots for one thread.
   */
  struct slot_block {
	/// Array of hazard slots. Each slot holds a raw pointer protected by
	/// release/acquire semantics.
	std::array<std::atomic<void*>, kHPPerThread> slots;
	/// Initializes all slots to nullptr.
	slot_block() {
	  for (auto& s : slots) s.store(nullptr, std::memory_order_relaxed);
	}
  };
  /**
   * @struct retire_record
   * @brief A retired pointer with a deleter and an epoch stamp for QSBR.
   *
   * @var retire_record::p Retired pointer to reclaim later.
   * @var retire_record::deleter Function that deletes the retired pointer.
   * @var retire_record::epoch Epoch snapshot at retirement (used by QSBR); zero
   * for HP.
   */
  struct retire_record {
	void* p{};	///< p Retired pointer to reclaim later.
	void (*deleter)(void*){}; ///< deleter Function that deletes the retired pointer.
	uint64_t epoch{};  ///< epoch Epoch snapshot at retirement (used by QSBR); zero
  };
  /**
   * @struct thread_rec
   * @brief Per-thread record stored in thread-local storage.
   *
   * @var thread_rec::block Pointer to the slot_block registered in the domain.
   * @var thread_rec::retired_hp Batch of HP-retired nodes, reclaimed after
   * hazard scan.
   * @var thread_rec::retired_qsbr Batch of QSBR-retired nodes, reclaimed once
   * epochs advance.
   * @var thread_rec::local_epoch Last published epoch for QSBR progress.
   */
  struct thread_rec {
	slot_block* block{};  ///< Pointer to the slot_block registered in the domain.
	std::vector<retire_record> retired_hp;	///<  Batch of HP-retired nodes, reclaimed after hazard scan.
	std::vector<retire_record> retired_qsbr;  ///<	Batch of QSBR-retired nodes, reclaimed once	epochs advance.
	std::atomic<uint64_t> local_epoch{0};	  ///<	Last published epoch for QSBR progress.
	~thread_rec() { hazard_domain::instance().deregister(block); }
  };
  /**
   * @brief Get the global hazard domain instance.
   * @return Reference to the singleton instance.
   */
  static hazard_domain& instance() {
	static hazard_domain dom;
	return dom;
  }
  /**
   * @brief Acquire or create a thread-local record for the calling thread.
   * @return Pointer to the thread's record.
   */
  thread_rec* acquire_thread_rec() {
	thread_local thread_rec* tr = nullptr;
	if (tr) return tr;
	auto* blk = new slot_block();
	{
	  std::lock_guard lk(reg_mu_);
	  registry_.push_back(blk);
	  epochs_.push_back(0);
	  index_of_[blk] = epochs_.size() - 1;
	}
	tr = new thread_rec{};
	tr->block = blk;
	auto g = global_epoch_.load(std::memory_order_relaxed);
	tr->local_epoch.store(g, std::memory_order_relaxed);
	{
	  std::lock_guard lk(reg_mu_);
	  epochs_[index_of_[blk]] = g;
	}
	return tr;
  }
  /**
   * @brief Publish a pointer into hazard slot @p i for @p tr and return it.
   * @param tr Thread record.
   * @param i  Slot index in [0, kHPPerThread).
   * @param p  Pointer to protect.
   * @return   The same pointer for convenience.
   */
  static void* protect(thread_rec* tr, size_t i, void* p) {
	tr->block->slots[i].store(p, std::memory_order_release);
	return p;
  }
  /**
   * @brief Clear hazard slot @p i for @p tr.
   * @param tr Thread record.
   * @param i  Slot index.
   */
  static void clear(thread_rec* tr, size_t i) {
	tr->block->slots[i].store(nullptr, std::memory_order_release);
  }
  /**
   * @brief Publish a quiescent state for @p tr, possibly advance epochs and
   * reclaim QSBR.
   * @param tr Thread record.
   */
  void quiescent(thread_rec* tr) {
	const uint64_t g = global_epoch_.load(std::memory_order_acquire);
	tr->local_epoch.store(g, std::memory_order_release);
	size_t idx;
	{
	  std::lock_guard lk(reg_mu_);
	  idx = index_of_[tr->block];
	  if (idx < epochs_.size()) epochs_[idx] = g;
	}
	try_advance_global_epoch(g);
	try_advance_and_reclaim_qsbr(tr);
  }
  /// Advance the global epoch unconditionally.
  void advance_epoch() {
	global_epoch_.fetch_add(1, std::memory_order_acq_rel);
  }
  /// Attempt to advance the epoch based on all threads' published epochs.
  void maybe_advance() {
	const uint64_t g = global_epoch_.load(std::memory_order_acquire);
	try_advance_global_epoch(g);
  }
  /**
   * @brief Retire a pointer under the HP scheme.
   * @param tr Thread record.
   * @param p Pointer to reclaim later.
   * @param deleter Deleter function.
   *
   * Reclamation happens after a hazard scan finds that @p p is not protected
   * by any hazard slot in the domain.
   */
  void retire_hp(thread_rec* tr, void* p, void (*deleter)(void*)) {
	tr->retired_hp.push_back({p, deleter, 0});
	if (tr->retired_hp.size() >= kReclaimThreshold) scan_and_reclaim_hp(tr);
  }
  /**
   * @brief Retire a pointer under the QSBR scheme.
   * @param tr Thread record.
   * @param p Pointer to reclaim later.
   * @param deleter Deleter function.
   *
   * The pointer is reclaimed once all threads have published an epoch
   * greater than or equal to the epoch recorded at retirement time.
   */
  void retire_qsbr(thread_rec* tr, void* p, void (*deleter)(void*)) {
	auto e = global_epoch_.load(std::memory_order_acquire);
	tr->retired_qsbr.push_back({p, deleter, e});
	if (tr->retired_qsbr.size() >= kReclaimThreshold)
	  try_advance_global_epoch(e);
	try_advance_and_reclaim_qsbr(tr);
  }
  /**
   * @struct hp_guard
   * @brief RAII helper that sets a hazard slot on construction and clears on
   * destruction.
   *
   * @details
   * Typical usage:
   * @code
   * auto* tr = hazard_domain::instance().acquire_thread_rec();
   * hp_guard g(tr, 0);
   * node* p = static_cast<node*>(g.set(head_.load(std::memory_order_acquire)));
   * // safely dereference p...
   * @endcode
   */
  struct hp_guard {
	hazard_domain::thread_rec* tr{};
	size_t idx{};
	hp_guard(hazard_domain::thread_rec* r, size_t i) : tr(r), idx(i) {}
	/// Set the hazard slot to protect @p p and return @p p.
	void* set(void* p) const {
	  return hazard_domain::instance().protect(tr, idx, p);
	}
	~hp_guard() { hazard_domain::instance().clear(tr, idx); }
  };
  /**
   * @struct qsbr_section
   * @brief RAII helper publishing a quiescent state at scope exit.
   */
  struct qsbr_section {
	hazard_domain::thread_rec* tr{};
	explicit qsbr_section(hazard_domain::thread_rec* r) : tr(r) {}
	~qsbr_section() { hazard_domain::instance().quiescent(tr); }
  };
  /**
   * @brief Perform a global hazard scan and reclaim any HP-retired nodes not
   * currently protected.
   * @param tr Thread record whose HP retire list will be processed.
   */
  void scan_and_reclaim_hp(thread_rec* tr) {
	std::vector<void*> hazards;
	{
	  std::lock_guard lk(reg_mu_);
	  hazards.reserve(registry_.size() * kHPPerThread);
	  for (auto* blk : registry_) {
		for (size_t i = 0; i < kHPPerThread; ++i) {
		  void* p = blk->slots[i].load(std::memory_order_acquire);
		  if (p) hazards.push_back(p);
		}
	  }
	}
	std::sort(hazards.begin(), hazards.end());
	hazards.erase(std::unique(hazards.begin(), hazards.end()), hazards.end());

	std::vector<retire_record> keep;
	keep.reserve(tr->retired_hp.size());
	for (auto& r : tr->retired_hp) {
	  if (r.p && std::binary_search(hazards.begin(), hazards.end(), r.p)) {
		keep.push_back(r);
	  } else if (r.p) {
		r.deleter(r.p);
	  }
	}
	tr->retired_hp.swap(keep);
  }

 private:
  hazard_domain() = default;
  /// Deregister a slot block from the registry; compacts lazily.
  void deregister(slot_block* blk) {
	if (!blk) return;
	std::lock_guard lk(reg_mu_);
	size_t idx = index_of_[blk];
	if (idx < epochs_.size())
	  epochs_[idx] = global_epoch_.load(std::memory_order_relaxed);
	for (auto& s : blk->slots) s.store(nullptr, std::memory_order_relaxed);
	index_of_.erase(blk);
	registry_erase_pending_.push_back(blk);
	compact_if_needed();
  }
  /// Compact the registry when enough pending erasures accumulate.
  void compact_if_needed() {
	if (registry_erase_pending_.size() < 8) return;

	std::vector<slot_block*> new_registry;
	new_registry.reserve(registry_.size());
	std::vector<uint64_t> new_epochs;
	new_epochs.reserve(epochs_.size());

	for (auto* blk : registry_) {
	  if (blk && index_of_.count(blk)) {
		new_registry.push_back(blk);
		new_epochs.push_back(epochs_[index_of_[blk]]);
	  } else {
		delete blk;
	  }
	}
	registry_.swap(new_registry);
	epochs_.swap(new_epochs);
	index_of_.clear();
	for (size_t i = 0; i < registry_.size(); ++i) index_of_[registry_[i]] = i;
	registry_erase_pending_.clear();
  }
  /// Try to advance QSBR epoch and reclaim any eligible QSBR-retired nodes in
  /// @p tr.
  void try_advance_and_reclaim_qsbr(thread_rec* tr) {
	uint64_t g = global_epoch_.load(std::memory_order_acquire);
	uint64_t min_epoch = g;
	{
	  std::lock_guard lk(reg_mu_);
	  for (auto e : epochs_) min_epoch = std::min(min_epoch, e);
	}

	std::vector<retire_record> keep;
	keep.reserve(tr->retired_qsbr.size());
	for (auto& r : tr->retired_qsbr) {
	  if (r.epoch < min_epoch)
		r.deleter(r.p);
	  else
		keep.push_back(r);
	}
	tr->retired_qsbr.swap(keep);
  }
  /// Attempt to advance the global epoch if all published epochs are at least
  /// @p g_snapshot.
  void try_advance_global_epoch(uint64_t g_snapshot) {
	uint64_t min_epoch = g_snapshot;
	{
	  std::lock_guard lk(reg_mu_);
	  for (auto e : epochs_) min_epoch = std::min(min_epoch, e);
	}
	if (min_epoch >= g_snapshot) {
	  global_epoch_.compare_exchange_strong(g_snapshot, g_snapshot + 1,
											std::memory_order_acq_rel,
											std::memory_order_relaxed);
	}
  }

  /// Mutex protecting the registry of slot blocks and their epochs.
  std::mutex reg_mu_;
  /// Registered slot blocks for active threads.
  std::vector<slot_block*> registry_;
  /// Last published epochs for each slot block.
  std::vector<uint64_t> epochs_;
  /// Pending erasures for later compaction.
  std::vector<slot_block*> registry_erase_pending_;
  /// Map from slot block to its index in the epochs_ array.
  std::unordered_map<slot_block*, size_t> index_of_;
  /// Global epoch counter for QSBR.
  std::atomic<uint64_t> global_epoch_{1};
};
/// Convenience type alias for hp_guard RAII.
using hp_guard = hazard_domain::hp_guard;
/// Convenience type alias for qsbr_section RAII.
using qsbr_section = hazard_domain::qsbr_section;

}  // namespace tp::detail
