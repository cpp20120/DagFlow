#include "../include/thread_pool.hpp"

#include <random>
#include <span>
#include <utility>

#ifdef _WIN32
#include <windows.h>
#endif

namespace dagflow {

thread_local uint32_t Pool::tls_id_ = UINT32_MAX;
thread_local bool Pool::tls_in_pool_ = false;

static void pin_to_cpu(uint32_t idx) {
#if defined(_WIN32)
  DWORD_PTR mask =
	  (static_cast<DWORD_PTR>(1) << (idx % (8 * sizeof(DWORD_PTR))));
  SetThreadAffinityMask(GetCurrentThread(), mask);
#elif defined(__linux__)
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(idx % CPU_SETSIZE, &cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
#else
  (void)idx;
#endif
}

Pool::Pool(const Config& cfg) : cfg_(cfg) {
  const uint32_t n = cfg_.threads ? cfg_.threads : 1;
  const uint32_t m = cfg_.shards ? cfg_.shards : n;

  workers_.reserve(n);
  threads_.reserve(n);
  centrals_.reserve(m);

  for (uint32_t i = 0; i < n; ++i) {
	auto w = std::make_unique<Worker>();
	std::random_device rd;
	w->rng.seed(rd());
	w->backoff_us = cfg_.idle_us_min;
	workers_.emplace_back(std::move(w));
  }
  for (uint32_t i = 0; i < m; ++i) {
	centrals_.emplace_back(std::make_unique<CentralShard>());
  }
  for (uint32_t i = 0; i < n; ++i) {
	threads_.emplace_back([this, i] {
	  tls_id_ = i;
	  tls_in_pool_ = true;
	  if (cfg_.pin_threads) pin_to_cpu(i);
	  worker_loop(i);
	  tls_in_pool_ = false;
	  tls_id_ = UINT32_MAX;
	});
  }
}

Pool::~Pool() {
  stop_.store(true, std::memory_order_release);
  for (uint32_t i = 0; i < workers_.size(); ++i) notify_worker(i);
  {
	std::lock_guard lk(wait_mu_);
	wait_cv_.notify_all();
  }
  for (auto& t : threads_)
	if (t.joinable()) t.join();
}

Handle Pool::submit(Fn fn, void* arg, SubmitOptions opt) {
  return submit_impl(small_function<void()>{[=] { fn(arg); }}, opt);
}

Handle Pool::submit_impl(small_function<void()> job, SubmitOptions opt) {
  auto ctr = std::make_shared<Handle::Counter>();
  ctr->count.store(1, std::memory_order_relaxed);

  auto* t = new Task{};
  t->fn = std::move(job);
  t->prio = opt.priority;
  t->owned = opt.owned;
  t->done = ctr;

  dispatch(t, opt.affinity, true);
  submitted_.fetch_add(1, std::memory_order_relaxed);
  return Handle{std::move(ctr)};
}

void Pool::dispatch(Task* t, std::optional<uint32_t> affinity,
					bool rate_limit_notify) {
  if (tls_in_pool_) {
	auto& me = *workers_[tls_id_];
	if (t->prio == Priority::High)
	  me.deq_hi.push_bottom(t);
	else
	  me.deq_lo.push_bottom(t);
	notify_worker(tls_id_);
	return;
  }

  const auto m = static_cast<uint32_t>(centrals_.size());
  const uint32_t shard =
	  affinity ? (*affinity % m)
			   : (rr_.fetch_add(1, std::memory_order_relaxed) % m);
  if (t->prio == Priority::High)
	centrals_[shard]->hi.push(t);
  else
	centrals_[shard]->lo.push(t);

  const uint32_t W = static_cast<uint32_t>(workers_.size());
  const uint32_t base = shard % W;

  // всегда разбудим хотя бы одного — снижает холодный старт workflow
  notify_worker(base);

  // Фан-аут будим редко (снижает контеншн на noop)
  if (!rate_limit_notify) {
	// без лимита — полный fan-out
	notify_worker((base + 1) % W);
	notify_worker((base + 2) % W);
	notify_worker(rr_.fetch_add(1, std::memory_order_relaxed) % W);
  } else {
	// редкий fan-out + очень редкий мягкий broadcast
	uint32_t tick = submit_tick_.fetch_add(1, std::memory_order_relaxed) + 1;
	if ((tick & 31u) == 0u) {  // каждые 32 сабмита
	  notify_worker((base + 1) % W);
	  notify_worker((base + 2) % W);
	  notify_worker(rr_.fetch_add(1, std::memory_order_relaxed) % W);
	}
	if ((tick & 4095u) == 0u) {	 // очень редко — всех
	  notify_all_workers();
	}
  }
}

Handle Pool::combine(std::span<const Handle> hs, SubmitOptions opt) {
  if (hs.empty()) return Handle{};
  auto ctr = std::make_shared<Handle::Counter>();
  ctr->count.store(static_cast<int>(hs.size()), std::memory_order_relaxed);

  for (auto& h : hs) {
	auto dep = h.get();
	submit_impl(small_function<void()>{[this, dep = std::move(dep), c = ctr] {
				  while (dep->count.load(std::memory_order_acquire) > 0) {
					if (!tls_in_pool_ || !try_help_one(tls_id_))
					  std::this_thread::yield();
				  }
				  complete_counter(c.get());
				}},
				opt);
  }
  return Handle{std::move(ctr)};
}

void Pool::complete_counter(Handle::Counter* ctr) {
  if (!ctr) return;
  if (ctr->count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
	std::lock_guard<std::mutex> lk(ctr->mu);
	ctr->cv.notify_all();
  }
}

void Pool::wait(const Handle& h) {
  if (!h.valid()) return;
  auto c = h.ctr_;

  if (tls_in_pool_) {
	while (c->count.load(std::memory_order_acquire) > 0) {
	  if (!try_help_one(tls_id_)) std::this_thread::yield();
	}
  } else {
	std::unique_lock<std::mutex> lk(c->mu);
	c->cv.wait(lk,
			   [&] { return c->count.load(std::memory_order_acquire) == 0; });
  }
}

void Pool::wait_idle() {
  std::unique_lock lk(wait_mu_);
  wait_cv_.wait(lk, [&] {
	return stop_.load(std::memory_order_acquire) ||
		   (all_empty() && inflight_.load(std::memory_order_acquire) == 0);
  });
}

bool Pool::all_empty() const {
  for (auto const& c : centrals_)
	if (!c->hi.empty() || !c->lo.empty()) return false;
  for (auto const& w : workers_)
	if (!w->deq_hi.empty() || !w->deq_lo.empty()) return false;
  return true;
}

void Pool::notify_worker(uint32_t id) {
  auto& w = *workers_[id % workers_.size()];
  std::lock_guard lk(w.mu);
  w.cv.notify_one();
}

void Pool::notify_all_workers() {
  const uint32_t W = static_cast<uint32_t>(workers_.size());
  for (uint32_t i = 0; i < W; ++i) notify_worker(i);
}

uint32_t Pool::pick_queue() {
  return rr_.fetch_add(1, std::memory_order_relaxed) %
		 static_cast<uint32_t>(workers_.size());
}

bool Pool::try_help_one(uint32_t id) {
  constexpr uint32_t kStealBatch = 4;

  auto& me = *workers_[id];

  auto exec = [&](Task* t) {
	inflight_.fetch_add(1, std::memory_order_acq_rel);
	try {
	  if (t->fn) t->fn();
	} catch (...) {
	  // swallow as per pool policy
	}
	executed_.fetch_add(1, std::memory_order_relaxed);

	auto done = std::move(t->done);
	if (t->owned) delete t;
	if (done) complete_counter(done.get());

	if (inflight_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
	  std::lock_guard<std::mutex> lk(wait_mu_);
	  wait_cv_.notify_all();
	}
  };

  // локальные — самый дешёвый путь
  if (Task* t = nullptr; me.deq_hi.pop_bottom(t)) {
	exec(t);
	return true;
  }
  if (Task* t = nullptr; me.deq_lo.pop_bottom(t)) {
	exec(t);
	return true;
  }

  // батчево переложим из central в локальные деки
  auto& shard = *centrals_[id % centrals_.size()];

  auto drain_central_to_local = [&](auto& q, auto& local_deque,
									uint32_t limit) -> uint32_t {
	uint32_t taken = 0;
	for (; taken < limit; ++taken) {
	  if (auto v = q.pop()) {
		local_deque.push_bottom(*v);
	  } else {
		break;
	  }
	}
	return taken;
  };

  // HI priority batch
  if (drain_central_to_local(shard.hi, me.deq_hi, cfg_.central_batch) > 0) {
	if (Task* t = nullptr; me.deq_hi.pop_bottom(t)) {
	  exec(t);
	  return true;
	}
  }
  // LO priority batch
  if (drain_central_to_local(shard.lo, me.deq_lo, cfg_.central_batch) > 0) {
	if (Task* t = nullptr; me.deq_lo.pop_bottom(t)) {
	  exec(t);
	  return true;
	}
  }

  // воровство пачками у соседей
  auto steal_n_from = [](auto& victim_deque, Task* out[], uint32_t max_take) {
	uint32_t taken = 0;
	for (; taken < max_take; ++taken) {
	  Task* t = nullptr;
	  if (!victim_deque.steal(t)) break;
	  out[taken] = t;
	}
	return taken;
  };

  std::uniform_int_distribution<uint32_t> dist(
	  0, static_cast<uint32_t>(workers_.size() - 1));
  const uint32_t start = dist(me.rng);

  // HI
  for (uint32_t i = 0; i < workers_.size(); ++i) {
	uint32_t vic = (start + i) % workers_.size();
	if (vic == id) continue;

	Task* buf[kStealBatch];
	uint32_t n = steal_n_from(workers_[vic]->deq_hi, buf, kStealBatch);
	if (n > 0) {
	  stolen_.fetch_add(n, std::memory_order_relaxed);
	  exec(buf[0]);
	  for (uint32_t k = 1; k < n; ++k) me.deq_hi.push_bottom(buf[k]);
	  return true;
	}
  }

  // LO
  for (uint32_t i = 0; i < workers_.size(); ++i) {
	uint32_t vic = (start + i) % workers_.size();
	if (vic == id) continue;

	Task* buf[kStealBatch];
	uint32_t n = steal_n_from(workers_[vic]->deq_lo, buf, kStealBatch);
	if (n > 0) {
	  stolen_.fetch_add(n, std::memory_order_relaxed);
	  exec(buf[0]);
	  for (uint32_t k = 1; k < n; ++k) me.deq_lo.push_bottom(buf[k]);
	  return true;
	}
  }

  return false;
}

void Pool::worker_loop(uint32_t id) {
  constexpr uint32_t kStealBatch = 4;

  auto& me = *workers_[id];
  std::uniform_int_distribution<uint32_t> dist(
	  0, static_cast<uint32_t>(workers_.size() - 1));

  auto exec = [&](Task* t) {
	inflight_.fetch_add(1, std::memory_order_acq_rel);
	try {
	  if (t->fn) t->fn();
	} catch (...) {
	  // swallow as per pool policy
	}
	executed_.fetch_add(1, std::memory_order_relaxed);

	auto done = std::move(t->done);
	if (t->owned) delete t;
	if (done) complete_counter(done.get());

	if (inflight_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
	  std::lock_guard<std::mutex> lk(wait_mu_);
	  wait_cv_.notify_all();
	}
  };

  auto drain_central_to_local = [&](auto& q, auto& local_deque,
									uint32_t limit) -> uint32_t {
	uint32_t taken = 0;
	for (; taken < limit; ++taken) {
	  if (auto v = q.pop()) {
		local_deque.push_bottom(*v);
	  } else {
		break;
	  }
	}
	return taken;
  };

  while (!stop_.load(std::memory_order_acquire)) {
	// 1) локальные деки — самый быстрый путь
	if (Task* t = nullptr; me.deq_hi.pop_bottom(t)) {
	  exec(t);
	  me.backoff_us = cfg_.idle_us_min;
	  continue;
	}
	if (Task* t = nullptr; me.deq_lo.pop_bottom(t)) {
	  exec(t);
	  me.backoff_us = cfg_.idle_us_min;
	  continue;
	}

	// 2) central → локальные (batch)
	auto& shard = *centrals_[id % centrals_.size()];

	if (drain_central_to_local(shard.hi, me.deq_hi, cfg_.central_batch) > 0) {
	  me.backoff_us = cfg_.idle_us_min;
	  continue;
	}
	if (drain_central_to_local(shard.lo, me.deq_lo, cfg_.central_batch) > 0) {
	  me.backoff_us = cfg_.idle_us_min;
	  continue;
	}

	// 3) воровство пачками
	bool got = false;
	const uint32_t start = dist(me.rng);

	// HI
	auto steal_n_from = [](auto& victim_deque, Task* out[], uint32_t max_take) {
	  uint32_t taken = 0;
	  for (; taken < max_take; ++taken) {
		Task* t = nullptr;
		if (!victim_deque.steal(t)) break;
		out[taken] = t;
	  }
	  return taken;
	};

	for (uint32_t i = 0; i < workers_.size(); ++i) {
	  uint32_t vic = (start + i) % workers_.size();
	  if (vic == id) continue;
	  Task* buf[kStealBatch];
	  uint32_t n = steal_n_from(workers_[vic]->deq_hi, buf, kStealBatch);
	  if (n > 0) {
		stolen_.fetch_add(n, std::memory_order_relaxed);
		exec(buf[0]);
		for (uint32_t k = 1; k < n; ++k) me.deq_hi.push_bottom(buf[k]);
		got = true;
		break;
	  }
	}
	if (got) {
	  me.backoff_us = cfg_.idle_us_min;
	  continue;
	}

	// LO
	for (uint32_t i = 0; i < workers_.size(); ++i) {
	  uint32_t vic = (start + i) % workers_.size();
	  if (vic == id) continue;
	  Task* buf[kStealBatch];
	  uint32_t n = steal_n_from(workers_[vic]->deq_lo, buf, kStealBatch);
	  if (n > 0) {
		stolen_.fetch_add(n, std::memory_order_relaxed);
		exec(buf[0]);
		for (uint32_t k = 1; k < n; ++k) me.deq_lo.push_bottom(buf[k]);
		got = true;
		break;
	  }
	}
	if (got) {
	  me.backoff_us = cfg_.idle_us_min;
	  continue;
	}

	// 4) обслуживание домена HP/QSBR (дёшево)
	tp::detail::hazard_domain::instance().maybe_advance();

	// 5) backoff с коротким сном; просыпаемся если где-то появились задачи
	{
	  std::unique_lock lk(me.mu);
	  me.cv.wait_for(lk, std::chrono::microseconds(me.backoff_us), [&] {
		return stop_.load(std::memory_order_acquire) ||
			   !workers_[id]->deq_hi.empty() || !workers_[id]->deq_lo.empty() ||
			   !centrals_[id % centrals_.size()]->hi.empty() ||
			   !centrals_[id % centrals_.size()]->lo.empty();
	  });
	  // эксп. бэк-офф
	  uint32_t next = me.backoff_us ? (me.backoff_us * 2) : cfg_.idle_us_min;
	  me.backoff_us = std::min<uint32_t>(
		  cfg_.idle_us_max, std::max<uint32_t>(cfg_.idle_us_min, next));
	}
  }
}

}  // namespace tp
