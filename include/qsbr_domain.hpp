#pragma once
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

namespace dagflow::detail {

class qsbr_domain {
 public:
  static constexpr size_t kReclaimThreshold = 512;

  struct retire_record {
	void* p{};
	void (*deleter)(void*){};
	uint64_t epoch{};
  };

  struct thread_rec {
	size_t idx{};
	std::vector<retire_record> retired;
	std::atomic<uint64_t> local_epoch{0};
	uint32_t local_ops{0};
  };

  static qsbr_domain& instance() {
	static qsbr_domain dom;
	return dom;
  }

  thread_rec* acquire_thread_rec() {
	thread_local thread_rec* tr = nullptr;
	if (tr) return tr;

	auto idx = register_thread();
	tr = new thread_rec{};
	tr->idx = idx;

	uint64_t g = global_epoch_.load(std::memory_order_relaxed);
	tr->local_epoch.store(g, std::memory_order_relaxed);
	epochs_[idx].val.store(g, std::memory_order_relaxed);
	return tr;
  }

  void quiescent(thread_rec* tr) {
	uint64_t g = global_epoch_.load(std::memory_order_relaxed);
	epochs_[tr->idx].val.store(g, std::memory_order_release);

	if ((++tr->local_ops & 0xFFu) == 0 &&
		tr->retired.size() >= kReclaimThreshold) {
	  try_advance_and_reclaim(tr);
	}
  }

  void retire(thread_rec* tr, void* p, void (*deleter)(void*)) {
	auto e = global_epoch_.load(std::memory_order_acquire);
	tr->retired.push_back({p, deleter, e});
	if (tr->retired.size() >= kReclaimThreshold) {
	  try_advance_and_reclaim(tr);
	}
  }

  struct qsbr_section {
	thread_rec* tr{};
	explicit qsbr_section(thread_rec* r) : tr(r) {}
	~qsbr_section() { qsbr_domain::instance().quiescent(tr); }
  };

 private:
  struct alignas(64) atomic_epoch {
	std::atomic<uint64_t> val;
	uint8_t pad[64 - sizeof(std::atomic<uint64_t>)];

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

  qsbr_domain() {
	auto n = std::thread::hardware_concurrency();
	epochs_.reserve(n);
	epochs_.resize(n);
	for (auto& e : epochs_) {
	  e.val.store(0, std::memory_order_relaxed);
	}
  }

  size_t register_thread() {
	size_t id = next_idx_.fetch_add(1, std::memory_order_relaxed);
	if (id >= epochs_.size()) {
	  epochs_.resize(id + 1);
	  epochs_[id].val.store(0, std::memory_order_relaxed);
	}
	return id;
  }

  void try_advance_and_reclaim(thread_rec* tr) {
	uint64_t g = global_epoch_.load(std::memory_order_acquire);
	uint64_t min_epoch = g;

	for (auto& e : epochs_) {
	  uint64_t v = e.val.load(std::memory_order_acquire);
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

  std::atomic<uint64_t> global_epoch_{1};
  std::atomic<size_t> next_idx_{0};
  std::vector<atomic_epoch> epochs_;
  std::atomic<size_t> ops_since_last_check_{0};
};

using qsbr_section = qsbr_domain::qsbr_section;

}  // namespace dagflow::detail

