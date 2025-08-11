#pragma once
#include <atomic>
#include <optional>
#include <utility>

#include "hazard_ptr.hpp"

namespace tp::detail {

/** \class mpmc_queue
 * \brief A lock-free MPMC queue using the Michael-Scott algorithm.
 *
 * This class provides a thread-safe queue that supports multiple producers and
 * consumers, using hazard pointers and optional QSBR for memory reclamation.
 *
 * \tparam T The type of elements stored in the queue.
 */
template <typename T>
class mpmc_queue {
  /** \struct node
   * \brief Represents a node in the queue.
   */
  struct node {
	std::atomic<node*> next{nullptr}; /**< \brief Pointer to the next node. */
	T data;							  /**< \brief Data stored in the node. */
	node() = default;				  // dummy
	/** \brief Constructs a node with data.
	 *
	 * \param v The data to store (moved into the node).
	 */
	explicit node(T v) : next(nullptr), data(std::move(v)) {}
  };

 public:
  /** \brief Constructs an empty queue with a dummy node. */
  mpmc_queue() {
	node* d = new node();
	head_.store(d, std::memory_order_relaxed);
	tail_.store(d, std::memory_order_relaxed);
  }
  /** \brief Destroys the queue, freeing all nodes. */
  ~mpmc_queue() {
	node* p = head_.load(std::memory_order_relaxed);
	while (p) {
	  node* n = p->next.load(std::memory_order_relaxed);
	  delete p;
	  p = n;
	}
  }
  /** \brief Deleted copy constructor to prevent copying. */
  mpmc_queue(const mpmc_queue&) = delete;
  /** \brief Deleted copy assignment operator to prevent copying. */
  mpmc_queue& operator=(const mpmc_queue&) = delete;
  /** \brief Pushes an element to the queue.
   *
   * \param v The element to push (moved into the queue).
   */
  void push(T v) {
	auto& dom = hazard_domain::instance();
	auto* tr = dom.acquire_thread_rec();

	detail::qsbr_section qs(tr);

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
  /** \brief Attempts to pop an element from the queue.
   *
   * \return An optional containing the popped element, or std::nullopt if the
   * queue is empty.
   */
  std::optional<T> pop() {
	auto& dom = hazard_domain::instance();
	auto* tr = dom.acquire_thread_rec();
	detail::qsbr_section qs(tr);

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
  /** \brief Checks if the queue is empty.
   *
   * \return True if the queue is empty, false otherwise.
   */
  bool empty() const {
	node* head = head_.load(std::memory_order_acquire);
	return head->next.load(std::memory_order_acquire) == nullptr;
  }

 private:
  alignas(64) std::atomic<node*> head_{
	  nullptr}; /**< \brief Head of the queue (atomic). */
  alignas(64) std::atomic<node*> tail_{
	  nullptr}; /**< \brief Tail of the queue (atomic). */
};

}  // namespace tp::detail
