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

class hazard_domain {
 public:
  static constexpr size_t kHPPerThread = 2;
  static constexpr size_t kReclaimThreshold = 128;

  struct slot_block {
	std::array<std::atomic<void*>, kHPPerThread> slots;
	slot_block() {
	  for (auto& s : slots) s.store(nullptr, std::memory_order_relaxed);
	}
  };

  struct retire_record {
	void* p{};
	void (*deleter)(void*){};
	uint64_t epoch{};
  };

  struct thread_rec {
	slot_block* block{};
	std::vector<retire_record> retired_hp{};
	std::vector<retire_record> retired_qsbr{};
	std::atomic<uint64_t> local_epoch{0};

	~thread_rec() { hazard_domain::instance().deregister(block); }
  };

  static hazard_domain& instance() {
	static hazard_domain dom;
	return dom;
  }

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
	tr->local_epoch.store(global_epoch_.load(std::memory_order_relaxed),
						  std::memory_order_relaxed);
	return tr;
  }

  static void* protect(thread_rec* tr, size_t i, void* p) {
	tr->block->slots[i].store(p, std::memory_order_release);
	return p;
  }
  static void clear(thread_rec* tr, size_t i) {
	tr->block->slots[i].store(nullptr, std::memory_order_release);
  }

  void quiescent(thread_rec* tr) {
	auto g = global_epoch_.load(std::memory_order_acquire);
	tr->local_epoch.store(g, std::memory_order_release);
	size_t idx;
	{
	  std::lock_guard lk(reg_mu_);
	  idx = index_of_[tr->block];
	  if (idx < epochs_.size()) epochs_[idx] = g;
	}
	try_advance_and_reclaim_qsbr(tr);
  }

  void advance_epoch() {
	global_epoch_.fetch_add(1, std::memory_order_acq_rel);
  }

  void retire_hp(thread_rec* tr, void* p, void (*deleter)(void*)) {
	tr->retired_hp.push_back({p, deleter, 0});
	if (tr->retired_hp.size() >= kReclaimThreshold) scan_and_reclaim_hp(tr);
  }

  void retire_qsbr(thread_rec* tr, void* p, void (*deleter)(void*)) {
	auto e = global_epoch_.load(std::memory_order_acquire);
	tr->retired_qsbr.push_back({p, deleter, e});
	if (tr->retired_qsbr.size() >= kReclaimThreshold)
	  try_advance_and_reclaim_qsbr(tr);
  }

  struct hp_guard {
	hazard_domain::thread_rec* tr{};
	size_t idx{};
	hp_guard(hazard_domain::thread_rec* r, size_t i) : tr(r), idx(i) {}
	void* set(void* p) const { return hazard_domain::instance().protect(tr, idx, p); }
	~hp_guard() { hazard_domain::instance().clear(tr, idx); }
  };

  struct qsbr_section {
	hazard_domain::thread_rec* tr{};
	explicit qsbr_section(hazard_domain::thread_rec* r) : tr(r) {}
	~qsbr_section() { hazard_domain::instance().quiescent(tr); }
  };

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

  std::mutex reg_mu_;
  std::vector<slot_block*> registry_;
  std::vector<uint64_t> epochs_;
  std::vector<slot_block*> registry_erase_pending_;
  std::unordered_map<slot_block*, size_t> index_of_;

  std::atomic<uint64_t> global_epoch_{1};
};

using hp_guard = hazard_domain::hp_guard;
using qsbr_section = hazard_domain::qsbr_section;

}  // namespace tp::detail
