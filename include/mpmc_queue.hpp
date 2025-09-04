#pragma once
/**
 * @file mpmc_queue.hpp
 * @brief Michael–Scott lock-free MPMC queue с QSBR-ре-клаймом (без hazard
 * pointers).
 *
 * Алгоритм классический: фиктивный dummy-узел, push линкует в хвост и помогает
 * двигать tail, pop читает head->next; после успешного CAS головы старый head
 * отправляется в retire().
 *
 * Требования к безопасности:
 *  - Любая операция с очередью выполняется внутри QSBR-секции (RAII).
 *  - Узлы освобождаются, когда все потоки прошли quiescent после их retire().
 */

#include <atomic>
#include <optional>
#include <utility>

#include "qsbr_domain.hpp"	

namespace dagflow::detail {

template <typename T>
class mpmc_queue {
  struct node {
	std::atomic<node*> next{nullptr};
	T data;
	node() = default;
	explicit node(T v) : next(nullptr), data(std::move(v)) {}
  };

 public:
  mpmc_queue() {
	node* d = new node();
	head_.store(d, std::memory_order_relaxed);
	tail_.store(d, std::memory_order_relaxed);
  }

  ~mpmc_queue() {
	node* p = head_.load(std::memory_order_relaxed);
	while (p) {
	  node* n = p->next.load(std::memory_order_relaxed);
	  delete p;
	  p = n;
	}
  }

  mpmc_queue(const mpmc_queue&) = delete;
  mpmc_queue& operator=(const mpmc_queue&) = delete;

  void push(T v) {
	auto& dom = qsbr_domain::instance();
	auto* tr = dom.acquire_thread_rec();
	qsbr_section qs(tr);  // объявим quiescent на выходе

	node* n = new node(std::move(v));
	for (;;) {
	  node* tail = tail_.load(std::memory_order_acquire);
	  node* next = tail->next.load(std::memory_order_acquire);
	  if (tail == tail_.load(std::memory_order_acquire)) {
		if (next == nullptr) {
		  if (tail->next.compare_exchange_weak(next, n,
											   std::memory_order_release,
											   std::memory_order_relaxed)) {
			(void)tail_.compare_exchange_strong(
				tail, n, std::memory_order_acq_rel, std::memory_order_relaxed);
			return;
		  }
		} else {
		  (void)tail_.compare_exchange_strong(
			  tail, next, std::memory_order_acq_rel, std::memory_order_relaxed);
		}
	  }
	}
  }

  std::optional<T> pop() {
	auto& dom = qsbr_domain::instance();
	auto* tr = dom.acquire_thread_rec();
	qsbr_section qs(tr);  // объявим quiescent на выходе

	for (;;) {
	  node* head = head_.load(std::memory_order_acquire);
	  node* tail = tail_.load(std::memory_order_acquire);
	  node* next = head->next.load(std::memory_order_acquire);

	  if (head == head_.load(std::memory_order_acquire)) {
		if (next == nullptr) return std::nullopt;  // пусто

		if (head == tail) {
		  (void)tail_.compare_exchange_strong(
			  tail, next, std::memory_order_acq_rel, std::memory_order_relaxed);
		  continue;
		}

		T val = std::move(next->data);
		if (head_.compare_exchange_strong(head, next, std::memory_order_acq_rel,
										  std::memory_order_relaxed)) {
		  dom.retire(tr, head, [](void* p) { delete static_cast<node*>(p); });
		  return val;
		}
	  }
	}
  }

  bool empty() const {
	node* head = head_.load(std::memory_order_acquire);
	return head->next.load(std::memory_order_acquire) == nullptr;
  }

 private:
  alignas(64) std::atomic<node*> head_{nullptr};
  alignas(64) std::atomic<node*> tail_{nullptr};
};

}  // namespace dagflow::detail
