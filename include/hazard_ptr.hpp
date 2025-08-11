#pragma once
#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace tp::detail {
/**
 * @brief Hazard pointer domain for safe memory reclamation
 *
 * Implements both hazard pointer and QSBR (Quiescent State Based Reclamation)
 * techniques for managing object lifetimes in concurrent environments.
 */
class hazard_domain {
 public:
  static constexpr size_t kHPPerThread = 2;			///< HPs per thread
  static constexpr size_t kReclaimThreshold = 128;	///< Retire batch size
  /**
   * @brief Per-thread slot block for hazard pointers
   */
  struct slot_block {
	std::array<std::atomic<void*>, kHPPerThread> slots;	 ///< HP slots
	slot_block() {
	  for (auto& s : slots) s.store(nullptr, std::memory_order_relaxed);
	}
  };
  /**
   * @brief Record for retired objects
   */
  struct retire_record {
	void* p;				 ///< Pointer to retired object
	void (*deleter)(void*);	 ///< Deleter function
	uint64_t epoch;			 ///< Epoch of retirement
  };
  /**
   * @brief Per-thread state record
   */
  struct thread_rec {
	slot_block* block;						  ///< Associated HP block
	std::vector<retire_record> retired_hp;	  ///< HP-retired objects
	std::vector<retire_record> retired_qsbr;  ///< QSBR-retired objects
	std::atomic<uint64_t> local_epoch{0};	  ///< Thread's epoch

	~thread_rec() { hazard_domain::instance().deregister(block); }
  };
  /**
   * @brief Returns singleton instance
   */
  static hazard_domain& instance() {
	static hazard_domain dom;
	return dom;
  }
  /** @brief Acquires a thread-local record for the calling thread.
   *
   * @return Pointer to the thread's thread_rec.
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
   * @brief Protects a pointer with hazard pointer
   * @param tr Thread record
   * @param i HP slot index
   * @param p Pointer to protect
   * @return The protected pointer
   */
  static void* protect(thread_rec* tr, size_t i, void* p) {
	tr->block->slots[i].store(p, std::memory_order_release);
	return p;
  }
  /**
   * @brief Clears a hazard pointer slot
   * @param tr Thread record
   * @param i HP slot index
   */
  static void clear(thread_rec* tr, size_t i) {
	tr->block->slots[i].store(nullptr, std::memory_order_release);
  }
  /**
   * @brief Marks quiescent state for QSBR
   * @param tr Thread record
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
  /**
   * @brief Advances global epoch
   */
  void advance_epoch() {
	global_epoch_.fetch_add(1, std::memory_order_acq_rel);
  }

   void maybe_advance() {
	const uint64_t g = global_epoch_.load(std::memory_order_acquire);
	try_advance_global_epoch(g);
  }

  /**
   * @brief Retires an object using HP
   */
  void retire_hp(thread_rec* tr, void* p, void (*deleter)(void*)) {
	tr->retired_hp.push_back({p, deleter, 0});
	if (tr->retired_hp.size() >= kReclaimThreshold) scan_and_reclaim_hp(tr);
  }
  /**
   * @brief Retires an object using QSBR
   */
  void retire_qsbr(thread_rec* tr, void* p, void (*deleter)(void*)) {
	auto e = global_epoch_.load(std::memory_order_acquire);
	tr->retired_qsbr.push_back({p, deleter, e});
	if (tr->retired_qsbr.size() >= kReclaimThreshold)
	  try_advance_global_epoch(e);
	try_advance_and_reclaim_qsbr(tr);
  }
  /**
   * @brief RAII guard for hazard pointer
   */
  struct hp_guard {
	hazard_domain::thread_rec* tr{};  ///< Thread record
	size_t idx{};					  ///< HP slot index
	hp_guard(hazard_domain::thread_rec* r, size_t i) : tr(r), idx(i) {}
	void* set(void* p) const {
	  return hazard_domain::instance().protect(tr, idx, p);
	}  ///< Sets HP value
	~hp_guard() { hazard_domain::instance().clear(tr, idx); }
  };
  /** \struct qsbr_section
   * \brief RAII guard for a quiescent state section.
   */
  struct qsbr_section {
	hazard_domain::thread_rec* tr{};
	explicit qsbr_section(hazard_domain::thread_rec* r) : tr(r) {}
	~qsbr_section() { hazard_domain::instance().quiescent(tr); }
  };
  /** \brief Scans and reclaims retired objects for hazard pointers.
   *
   * \param tr The thread record for the calling thread.
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
  /** \brief Deregisters a slot block from the registry.
   *
   * \param blk The slot block to deregister.
   */
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
  /** \brief Compacts the registry if needed. */
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
  /** \brief Attempts to advance the epoch and reclaim QSBR-retired objects.
   *
   * \param tr The thread record for the calling thread.
   */
  void try_advance_and_reclaim_qsbr(thread_rec* tr) {
	uint64_t g = global_epoch_.load(std::memory_order_acquire);
	uint64_t min_epoch = g;
	{
	  std::lock_guard lk(reg_mu_);
	  for (auto e : epochs_) min_epoch = std::min(min_epoch, e);
	}
	if (min_epoch + 1 < g) {
	}

	std::vector<retire_record> keep;
	keep.reserve(tr->retired_qsbr.size());
	for (auto& r : tr->retired_qsbr) {
	  if (r.epoch < min_epoch) {
		r.deleter(r.p);
	  } else {
		keep.push_back(r);
	  }
	}
	tr->retired_qsbr.swap(keep);
  }
  /**
   * @brief Advances the global QSBR epoch if all threads have observed it.
   *
   * Checks whether every registered thread's local epoch is at least
   * @p g_snapshot. If so, increments the global epoch to mark a new
   * grace period for QSBR reclamation.
   *
   * @param g_snapshot Current global epoch value to validate against.
   *
   * @see try_advance_and_reclaim_qsbr
   */
  void try_advance_global_epoch(uint64_t g_snapshot) {
	uint64_t min_epoch = g_snapshot;
	{
	  std::lock_guard lk(reg_mu_);
	  for (auto e : epochs_) min_epoch = std::min(min_epoch, e);
	}
	if (min_epoch >= g_snapshot) {
	  // все потоки дошли до g_snapshot => продвинем на g+1
	  global_epoch_.compare_exchange_strong(g_snapshot, g_snapshot + 1,
											std::memory_order_acq_rel,
											std::memory_order_relaxed);
	}
  }

  std::mutex reg_mu_;				  /**< \brief Mutex for registry access. */
  std::vector<slot_block*> registry_; /**< \brief Registered slot blocks. */
  std::vector<uint64_t> epochs_;	  /**< \brief Epochs for QSBR. */
  std::vector<slot_block*>
	  registry_erase_pending_; /**< \brief Slot blocks pending erasure. */
  std::unordered_map<slot_block*, size_t>
	  index_of_; /**< \brief Index mapping for slot blocks. */
  std::atomic<uint64_t> global_epoch_{1}; /**< \brief Global epoch for QSBR. */
};

using hp_guard = hazard_domain::hp_guard;
using qsbr_section = hazard_domain::qsbr_section;

}  // namespace tp::detail
