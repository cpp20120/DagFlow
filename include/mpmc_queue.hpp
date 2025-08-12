#pragma once
/**
 * @file mpmc_queue.hpp
 * @brief Michael–Scott lock-free MPMC queue with Hazard Pointers and QSBR
 * reclamation.
 *
 * @details
 * This is a classic Michael–Scott queue with a dummy head node.
 * Producers allocate a node, enqueue it by linking at the tail and possibly
 * moving the tail. Consumers read `head->next`; if non-null they move the value
 * out of the next node and swing the head pointer forward. The old head is
 * retired via QSBR once the CAS succeeds.
 *
 * Safety and memory reclamation:
 *  - Two hazard pointer slots are used by pop(): one for `head` and one for
 * `next`.
 *  - The old `head` is retired via `retire_qsbr` to avoid concurrent
 * reclamation hazards.
 *
 * Progress:
 *  - The data structure is lock-free and supports multiple producers and
 * consumers.
 */

#include <atomic>
#include <optional>
#include <utility>

#include "hazard_ptr.hpp"

namespace tp::detail {
/**
 * @brief Michael–Scott lock-free MPMC queue with Hazard Pointers and QSBR
 * reclamation.
 * @tparam T
 */
template <typename T>
class mpmc_queue {
  /**
   * @brief Intrusive node holding the payload and the next pointer.
   *
   * The `next` pointer is atomic and participates in the Michael–Scott
   * protocol.
   */
  struct node {
	std::atomic<node*> next{nullptr};
	T data;
	node() = default;
	explicit node(T v) : next(nullptr), data(std::move(v)) {}
  };

 public:
  /**
   * @brief Construct an empty queue with a dummy head.
   */
  mpmc_queue() {
	node* d = new node();
	head_.store(d, std::memory_order_relaxed);
	tail_.store(d, std::memory_order_relaxed);
  }
  /**
   * @brief Destroy the queue by deleting all remaining nodes.
   *
   * This is safe only when no other threads are concurrently accessing the
   * queue.
   */
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
  /**
   * @brief Enqueue a value at the tail (multi-producer).
   * @param v Value to push; it is moved into the newly allocated node.
   *
   * The operation follows the standard MS-queue algorithm:
   *  - Read tail and tail->next under a hazard pointer.
   *  - If next is null, attempt to link the new node.
   *  - Otherwise, help advance the tail to keep the structure moving forward.
   */
  void push(T v) {
	auto& dom = hazard_domain::instance();
	auto* tr = dom.acquire_thread_rec();
	qsbr_section qs(tr);

	node* n = new node(std::move(v));
	for (;;) {
	  hp_guard ht(tr, 0);
	  node* tail =
		  static_cast<node*>(ht.set(tail_.load(std::memory_order_acquire)));
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
  /**
   * @brief Dequeue from the head (multi-consumer).
   * @return std::nullopt if the queue is currently empty; otherwise the value.
   *
   * The operation uses two hazard pointer slots to protect `head` and `next`
   * during the read and CAS sequence.
   */
  std::optional<T> pop() {
	auto& dom = hazard_domain::instance();
	auto* tr = dom.acquire_thread_rec();
	qsbr_section qs(tr);

	for (;;) {
	  hp_guard hh(tr, 0), hn(tr, 1);
	  node* head =
		  static_cast<node*>(hh.set(head_.load(std::memory_order_acquire)));
	  node* tail = tail_.load(std::memory_order_acquire);
	  node* next = static_cast<node*>(
		  hn.set(head->next.load(std::memory_order_acquire)));
	  if (head == head_.load(std::memory_order_acquire)) {
		if (next == nullptr) return std::nullopt;
		if (head == tail) {
		  (void)tail_.compare_exchange_strong(
			  tail, next, std::memory_order_acq_rel, std::memory_order_relaxed);
		  continue;
		}
		T val = std::move(next->data);
		if (head_.compare_exchange_strong(head, next, std::memory_order_acq_rel,
										  std::memory_order_relaxed)) {
		  dom.retire_qsbr(tr, head,
						  [](void* p) { delete static_cast<node*>(p); });
		  return val;
		}
	  }
	}
  }
  /**
   * @brief Snapshot emptiness check.
   * @return true if the queue is observed empty (head->next == nullptr).
   */
  bool empty() const {
	node* head = head_.load(std::memory_order_acquire);
	return head->next.load(std::memory_order_acquire) == nullptr;
  }

 private:
  /// Head pointer (points to dummy node or the first node). 64-byte aligned.
  alignas(64) std::atomic<node*> head_{nullptr};
  /// Tail pointer (points to last node). 64-byte aligned.
  alignas(64) std::atomic<node*> tail_{nullptr};
};

}  // namespace tp::detail
