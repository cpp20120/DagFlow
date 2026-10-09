
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <span>
#include <stdexcept>
#include <utility>

#include <dagflow/detail/fuzz_points.hpp>
#include <dagflow/detail/scheduler.hpp>
#include <dagflow/detail/parking_lot.hpp>
#include <dagflow/detail/idle_accounting.hpp>
#include <dagflow/detail/runtime_memory.hpp>
#include <dagflow/thread_pool.hpp>

#ifdef _WIN32
#include <windows.h>
#elif defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

#if defined(__ELF__) && (defined(__GNUC__) || defined(__clang__))
#ifdef DAGFLOW_STATIC
#define DAGFLOW_TLS_ATTR [[gnu::tls_model("initial-exec")]]
#else
#define DAGFLOW_TLS_ATTR [[gnu::tls_model("global-dynamic")]]
#endif
#else
#define DAGFLOW_TLS_ATTR
#endif

namespace dagflow {
namespace {
constexpr uint32_t wake_retry_mask = 63U;
constexpr uint32_t backoff_growth_factor = 2;
}  // namespace

thread_local uint32_t Pool::tls_id_ = UINT32_MAX;
DAGFLOW_TLS_ATTR thread_local Pool* Pool::tls_pool_ = nullptr;

static void pin_to_cpu(uint32_t idx) {
  // Placement is best-effort here: the scheduler's worker index is stable, but
  // the OS may expose fewer/irregular CPUs. Failure is intentionally ignored;
  // placement must never prevent pool construction or task progress.
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

// Construct immutable topology before workers can publish anything. Parking
// shares Scheduler membership, and accounting must outlive every worker task.
// The catch path is a constructor transaction only for thread startup: objects
// already started are stopped and joined before the allocation error escapes.
Pool::Pool(const Config& cfg) : cfg_(cfg) {
  const uint32_t worker_count =
      cfg_.threads ? cfg_.threads : DAGFLOW_MIN_WORKER_COUNT;
  scheduler_ = detail::make_owned<detail::Scheduler>(
      worker_count, cfg_.shards, cfg_.central_batch, cfg_.worker_shards);
  parking_ = detail::make_owned<detail::ParkingLot>(*scheduler_);
  accounting_ = detail::make_owned<detail::IdleAccounting>(worker_count);
  threads_.reserve(worker_count);
  try {
    for (uint32_t worker_id = 0; worker_id < worker_count; ++worker_id) {
      threads_.emplace_back([this, worker_id] {
        tls_id_ = worker_id;
        tls_pool_ = this;
        if (cfg_.pin_threads) pin_to_cpu(worker_id);
        worker_loop(worker_id);
        tls_pool_ = nullptr;
        tls_id_ = UINT32_MAX;
      });
    }
  } catch (...) {
    // A partially constructed vector of joinable threads must also be joined.
    stop_.store(true, std::memory_order_release);
    parking_->wake_all();
    for (auto& thread : threads_) thread.join();
    throw;
  }
}

// Destruction uses the same graceful protocol as explicit shutdown. Owning Pool
// storage from one of its own tasks is invalid: that task cannot join itself.
// shutdown diagnoses explicit misuse; throwing through this noexcept destructor
// terminates. Other external callers must be joined before destroying storage.
Pool::~Pool() {
  shutdown();
}

// The high-bit transition is the external admission linearization point.
// An already admitted publisher keeps its low-bit reservation even if it is
// currently blocked on a full ingress. close never waits, so workers may use it.
void Pool::close() noexcept {
  admission_.fetch_or(admission_closed_bit, std::memory_order_acq_rel);
}

// Closure is permanent, but says nothing about remaining work or joined threads.
bool Pool::closed() const noexcept {
  return (admission_.load(std::memory_order_acquire) & admission_closed_bit) != 0;
}

// Only external publishers reserve here. A worker has an outstanding accounting
// publication until AFTER callable destruction and completion propagation, so
// all of its child publications are covered without another shared RMW.
Pool::PublicationGuard::PublicationGuard(Pool& pool)
    : pool_(tls_pool_ == &pool ? nullptr : &pool) {
  if (pool_) pool_->enter_publication();
}

// After this release, shutdown may finish its publication barrier. Do not move
// the guard's end before dispatch/wake: accounting alone does not protect a
// publisher that has not yet made its accepted packet visible to workers.
Pool::PublicationGuard::~PublicationGuard() {
  if (pool_) pool_->leave_publication();
}

// Reserve only against an open state. CAS couples the check and reservation;
// a separate increment after reading an open flag would race the idle barrier.
void Pool::enter_publication() {
  auto state = admission_.load(std::memory_order_relaxed);
  for (;;) {
    if (state & admission_closed_bit)
      throw std::logic_error("Pool: external task admission is closed");
    if (state == admission_closed_bit - 1)
      throw std::overflow_error("Pool: too many concurrent publishers");
    if (admission_.compare_exchange_weak(state, state + 1,
                                        std::memory_order_acquire,
                                        std::memory_order_relaxed))
      return;
  }
}

// Publish completion of dispatch, including its wake, before dropping admission.
void Pool::leave_publication() noexcept {
  const auto previous = admission_.fetch_sub(1, std::memory_order_release);
  // Notify only the final publisher after close. Ordinary submits do not enter
  // the atomic-wait notification machinery or touch shutdown_mutex_.
  if (previous == admission_closed_bit + 1) admission_.notify_all();
}

void Pool::shutdown() {
  // Check before taking the mutex: an external shutdown may already be waiting
  // for this worker, so letting the worker lock it would deadlock both callers.
  if (tls_pool_ == this)
    throw std::logic_error("Pool::shutdown must be called outside the pool");
  std::lock_guard lock(shutdown_mutex_);
  close();
  auto state = admission_.load(std::memory_order_acquire);
  while (state != admission_closed_bit) {
    admission_.wait(state, std::memory_order_acquire);
    state = admission_.load(std::memory_order_acquire);
  }
  // Keep workers alive while publishers retry full queues and while parents
  // spawn descendants. Only after both barriers can stop mean "no work left".
  // Captures and final completion payloads are destroyed before lane retirement.
  accounting_->wait();
  stop_.store(true, std::memory_order_release);
  parking_->wake_all();
  for (auto& worker_thread : threads_)
    if (worker_thread.joinable()) worker_thread.join();
}

// Adapt the C callback to the same credit/packet path as typed submit. The
// callback argument is borrowed by the callable and must remain valid until
// the returned handle is ready; no special C-ABI completion path exists.
Handle Pool::submit(Fn function, void* arg, SubmitOptions opt) {
  return submit_impl(
      [=](detail::CompletionCredit&) { function(arg); }, std::move(opt));
}

void Pool::enqueue_prepared(detail::OwnedScheduledTask owned,
                            const SubmitOptions& opt) {
  PublicationGuard publication(*this);
  // The concrete callable already lives inline in the variable-sized packet.
  // Accounting is published only after construction succeeds and before queue
  // publication. If producer registration throws, the erased owner destroys
  // the exact TaskModel type and retires its unaccepted completion credit.
  detail::runtime_count(detail::RuntimeEvent::packets);
  detail::runtime_count(tls_pool_ == this ? detail::RuntimeEvent::worker_submit
                                          : detail::RuntimeEvent::external_submit);
  if (tls_pool_ == this)
    accounting_->publish_worker(tls_id_);
  else
    accounting_->publish_external();
  // Successful publication revokes all producer access to packet state.
  dispatch(owned.release(), opt);
}

void Pool::dispatch(Task* task, const SubmitOptions& opt) {
  // Dispatch is the routing linearization point. A worker prefers its own
  // Chase-Lev queue, while external callers use a shard ingress and may yield
  // under backpressure. Workers never wait for queue capacity: an intrusive
  // shared overflow queue preserves progress without recursive execution or
  // hiding a child from a nested helper. Every accepted pointer is wake-visible
  // before this function returns; the caller never accesses it afterward.
  if (tls_pool_ == this) {
    const auto home = scheduler_->home_shard(tls_id_);
    const auto target =
        opt.affinity ? scheduler_->select_shard(opt.affinity) : home;
    const bool ingress = opt.mode == SubmissionMode::Enqueue || target != home;
    const auto shard = ingress && !opt.affinity
        ? scheduler_->select_shard(std::nullopt) : target;
    const bool accepted =
        ingress ? scheduler_->try_submit_external(shard, task)
                : scheduler_->try_submit_local(tls_id_, task);
    if (!accepted) {
      // No allocation or execution after the accounting commit. A private
      // immediate slot would hide the incoming child from helping/thieves.
      scheduler_->submit_overflow(shard, task);
    }
    parking_->wake_one(shard);
    return;
  }

  const auto shard = scheduler_->select_shard(opt.affinity);
  uint32_t retries = 0;
  while (!scheduler_->try_submit_external(shard, task)) {
    // DAGFLOW_FUZZ_POINT(external_retry);
    detail::runtime_count(detail::RuntimeEvent::external_retry);
    if ((retries++ & wake_retry_mask) == 0) parking_->wake_one(shard);
    std::this_thread::yield();
  }
  parking_->wake_one(shard);
}

void Pool::enqueue_batch_prepared(
    std::span<detail::OwnedScheduledTask> pending,
    const SubmitOptions& opt) {
  PublicationGuard publication(*this);
  // One admission for the whole prepared group. Later groups can be rejected
  // by close; callers already owe the accepted prefix its borrowed-data lifetime.
  // The caller has fully constructed the <=64 packet group before entering
  // this function. Every entry is a concrete TaskModel allocation owned by the
  // span until queue publication transfers custody.
  detail::runtime_count(detail::RuntimeEvent::external_batch);
  for (std::size_t i = 0; i < pending.size(); ++i) {
    detail::runtime_count(detail::RuntimeEvent::packets);
    detail::runtime_count(detail::RuntimeEvent::external_submit);
    detail::runtime_count(detail::RuntimeEvent::batch_tasks);
  }

  const auto shard = scheduler_->select_shard(opt.affinity);
  bool published = false;
  try {
    for (auto& owned : pending) {
      // Publish accounting before queue ownership is transferred. Registration
      // can still fail while `owned` protects the packet; after release, the
      // accepted packet belongs exclusively to the scheduler/executor path.
      accounting_->publish_external();
      auto* task = owned.release();
      uint32_t retries = 0;
      while (!scheduler_->try_submit_external(shard, task)) {
        // DAGFLOW_FUZZ_POINT(external_retry);
        detail::runtime_count(detail::RuntimeEvent::external_retry);
        if ((retries++ & wake_retry_mask) == 0) parking_->wake_one(shard);
        std::this_thread::yield();
      }
      published = true;
    }
  } catch (...) {
    // Accepted prefix packets already own their accounting publications. The
    // remaining erased owners destroy their concrete models during unwinding.
    if (published) parking_->wake_one(shard);
    throw;
  }
  parking_->wake_one(shard);
}

Handle Pool::combine(std::span<const Handle> handles, SubmitOptions opt) {
  // Build a completion-only fan-in. Each valid source receives one dependent
  // credit; the registration credit is the sentinel that keeps the result
  // state alive while edges are installed. `opt` is intentionally ignored:
  // completion edges schedule no executable work and must not enter a queue.
  (void)opt;  // Completion edges do not schedule executable work.
  if (handles.empty()) return {};
  auto registration = detail::CompletionCredit::create();
  auto result = registration.handle();
  for (const auto& handle : handles) {
    if (handle.valid()) handle.state_->add_dependent(registration.fork());
  }
  // The registration sentinel also retires if vector allocation throws.
  return result;
}

void detail::CompletionState::retain() noexcept {
  // References keep the state allocation alive for Handle/credit observers;
  // they do not mean executable work is outstanding. Credits are counted
  // separately and are the only input to terminal propagation.
  references.fetch_add(1, std::memory_order_relaxed);
}

void detail::CompletionState::release() noexcept {
  // A reference decrement may destroy the state, but only after the final
  // credit has already made it ready and cleared payload/dependent storage.
  if (references.fetch_sub(1, std::memory_order_acq_rel) == 1)
    ObjectDeleter<CompletionState>{}(this);
}

// Destruction is reachable only after terminal propagation. These assertions
// document the state machine and catch accidental reference/credit reordering.
detail::CompletionState::~CompletionState() {
  assert(credits.load(std::memory_order_relaxed) == 0);
  assert(ready.load(std::memory_order_relaxed));
  assert(!payload && dependents.empty());
}

void detail::CompletionState::set_error(std::exception_ptr failure) noexcept {
  // First failure wins under the same mutex that protects dependent-list
  // mutation. Error publication is independent from readiness publication.
  std::lock_guard lock(mutex);
  if (!error) {
    error = std::move(failure);
    has_error.store(static_cast<bool>(error), std::memory_order_release);
  }
}

std::exception_ptr detail::CompletionState::get_error() noexcept {
  if (!has_error.load(std::memory_order_acquire)) return {};
  // Copy the exception under the mutex; callers must not retain a reference to
  // mutable state after unlocking.
  std::lock_guard lock(mutex);
  return error;
}

void detail::CompletionState::add_dependent(CompletionCredit dependent) {
  // Only combine takes this lock. Claiming the gate before appending obliges
  // the final credit to take it too; closure sends late edges down the ready
  // path. A source without registrations never locks on ordinary completion.
  std::exception_ptr failure;
  {
    std::lock_guard lock(mutex);
    auto gate = Dependents::unused;
    dependent_gate.compare_exchange_strong(gate, Dependents::registered,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire);
    if (gate != Dependents::closed) {
      dependents.push_back(std::move(dependent));
      return;
    }
    failure = error;
  }
  if (failure) dependent.fail(failure);
  // Destruction consumes the edge against an already-completed source.
}

void detail::CompletionState::retire(CompletionState* state) noexcept {
  // Consume exactly one credit. Non-final credits only drop their reference;
  // the final credit owns a non-recursive terminal worklist so deep completion
  // graphs cannot overflow the executor or C++ call stack. Each current state
  // stays alive through payload destruction, ready notification and dependent
  // propagation, then releases its final worklist reference.
  const auto previous = state->credits.fetch_sub(1, std::memory_order_acq_rel);
  assert(previous != 0);
  if (previous != 1) {
    state->release();
    return;
  }
  // The final credit keeps storage alive through payload destruction, waiter
  // notification and dependent propagation even if all observers disappear.
  CompletionState* pending = state;
  while (pending) {
    auto* current = pending;
    pending = current->next_terminal;
    current->next_terminal = nullptr;
    if (current->destroy_payload) {
      auto* payload = std::exchange(current->payload, nullptr);
      auto destroy = std::exchange(current->destroy_payload, nullptr);
      destroy(payload);
    }
    std::vector<CompletionCredit, RuntimeAllocator<CompletionCredit>> edges;
    auto failure = current->get_error();
    // User payload is gone and the final credit sees every prior retirement.
    // Publish readiness before closing registration: a late registrant that
    // sees closed can immediately propagate the now immutable error. The final
    // credit's storage reference keeps this state alive through edge delivery.
    current->ready.store(true, std::memory_order_release);
    if (current->dependent_gate.exchange(Dependents::closed, std::memory_order_acq_rel)
        == Dependents::registered) {
      std::lock_guard lock(current->mutex);
      edges.swap(current->dependents);
    }
    current->ready.notify_all();
    for (auto& edge : edges) {
      auto* dependent = std::exchange(edge.state_, nullptr);
      if (failure) dependent->set_error(failure);
      const auto before = dependent->credits.fetch_sub(1, std::memory_order_acq_rel);
      assert(before != 0);
      if (before == 1) {
        // Transfer this edge's storage reference to the terminal worklist.
        dependent->next_terminal = pending;
        pending = dependent;
      } else {
        dependent->release();
      }
    }
    current->release();  // Final access; may destroy current.
  }
}

void detail::CompletionState::wait() {
  // Readiness is monotonic: atomic wait cannot miss a completed operation and
  // requires no mutex shared with the normal final-credit path.
  ready.wait(false, std::memory_order_acquire);
}

detail::CompletionCredit detail::CompletionCredit::create() {
  // Empty completion state still has one sentinel credit. The caller must
  // eventually finish it; that final transition publishes ready exactly once.
  return create_erased(nullptr, nullptr);
}

detail::CompletionCredit detail::CompletionCredit::create_erased(
    void* payload, void (*destroy)(void*) noexcept) {
  // Payload ownership transfers to the new state and is destroyed by the final
  // credit before ready is published. `destroy` is noexcept by contract.
  auto owner = make_owned<CompletionState>();
  runtime_count(RuntimeEvent::completion_created);
  auto* state = owner.get();
  state->payload = payload;
  state->destroy_payload = destroy;
  return CompletionCredit{owner.release()};
}

detail::CompletionCredit detail::CompletionCredit::fork() const noexcept {
  // Fork is valid only while the source has a live credit. Retain first, then
  // increment credits; the source keeps the allocation alive if propagation
  // races an observer release.
  assert(state_ && state_->credits.load(std::memory_order_relaxed) != 0);
  state_->retain();
  state_->credits.fetch_add(1, std::memory_order_relaxed);
  return CompletionCredit{state_};
}

// A Handle observes the same state but does not add a work credit. Its copy
// lifetime is protected by CompletionState::references in handle.hpp.
Handle detail::CompletionCredit::handle() const noexcept { return Handle{state_}; }

void detail::CompletionCredit::fail(std::exception_ptr error) const noexcept {
  // Failure is advisory state on the shared completion object. It does not
  // consume a credit; the normal retirement path still determines readiness.
  if (state_) state_->set_error(std::move(error));
}

void detail::CompletionCredit::finish() noexcept {
  // Exchange makes finish idempotent for a moved-from credit. Only the caller
  // that owns the exchanged pointer can perform the terminal decrement.
  if (auto* state = std::exchange(state_, nullptr)) CompletionState::retire(state);
}

void Pool::wait(const Handle& handle) {
  // External waits block on atomic readiness. A worker wait must help because a
  // single worker can otherwise be waiting for work that only it can execute.
  // Copying the observation protects the state if task code resets its Handle.
  if (!handle.valid()) return;
  // Retain an observation while helping: user code may replace its own Handle.
  auto observation = handle;
  if (tls_pool_ == this) {
    while (!observation.ready()) {
      detail::runtime_count(detail::RuntimeEvent::help_attempt);
      if (!try_help_one(tls_id_)) std::this_thread::yield();
      else detail::runtime_count(detail::RuntimeEvent::help_success);
    }
  } else {
    observation.state_->wait();
  }
}

void Pool::wait_idle() {
  // Idle accounting observes physical task epilogues, including descendants
  // and packet destruction. It is intentionally external-only: a worker must
  // use cooperative wait(handle), otherwise it could wait on itself.
  if (tls_pool_ == this)
    throw std::logic_error("Pool::wait_idle must be called outside the pool");
  accounting_->wait();
}
// Ordering below is part of the lifetime protocol. Do not reorder.
void Pool::execute_task(uint32_t id, Task* task) {
  // DAGFLOW_FUZZ_POINT(before_execute);
  detail::runtime_count(detail::RuntimeEvent::executed);
#if defined(DAGFLOW_RUNTIME_DIAGNOSTICS)
  if (task->allocating_thread != UINT64_MAX &&
      task->allocating_thread != detail::runtime_diagnostic_thread())
    detail::runtime_count(detail::RuntimeEvent::cross_thread_free);
#endif
  // Lifetime ordering is fixed: invoke -> capture exception -> move credit ->
  // destroy callable/packet -> finish completion -> retire executor lane.
  // Retiring earlier would let wait_idle() return while packet state is still
  // being touched; destroying after retire would create a use-after-idle.
  const auto* ops = task->ops;
  try {
    ops->invoke(task);
  } catch (...) {
    // Detached tasks have no error observer. Handled tasks preserve the first
    // exception, including execution caused by queue overflow.
    task->done.fail(std::current_exception());
  }

  auto done = std::move(task->done);
  // `destroy` releases the packet before readiness: owning TaskModels destroy
  // their callable/allocation, graph slots release their exclusive borrow.
  // In both cases the executor must never access the base pointer afterward.
  ops->destroy(task);
  done.finish();
  // The handle may now be ready while pool quiescence still waits for retirement.
  // DAGFLOW_FUZZ_POINT(before_retire);
  accounting_->retire(id);
}

bool Pool::try_help_one(uint32_t id, bool parking_announced) {
  // One acquisition attempt. Cancel our parking announcement before notifying
  // others, or the wake could select this worker instead of another waiter.
  bool recruit;
  if (auto* task = scheduler_->try_acquire(id, recruit)) {
    if (parking_announced) parking_->cancel(id);
    // Drain/steal temporarily removes tasks from a queue and republishes them
    // locally. A waiter's final scan can miss that transfer, so publication
    // still needs wake_one's fence/registration handshake before user code.
    // A successful ingress poll or steal also relays recruitment when it
    // returns just one task: its source may be empty while another queue still
    // has work. Stopping that relay can leave sleepers stranded until timeout.
    // Ordinary local pop only consumes work covered by the earlier publication
    // and relay. It adds no new publication requiring another fenced wake.
    // No accounting transfer occurs when work changes executor.
    if (recruit)
      parking_->wake_one(scheduler_->home_shard(id));
    execute_task(id, task);
    return true;
  }
  return false;
}

void Pool::worker_loop(uint32_t id) {
  // Worker state machine: acquire -> execute, or announce idle -> final acquire
  // -> park with bounded backoff. The final acquire pairs with publisher
  // fences, so parking cannot miss a task published during announcement.
  uint32_t backoff_us = cfg_.idle_us_min;
  while (!stop_.load(std::memory_order_acquire)) {
    if (try_help_one(id)) {
      backoff_us = cfg_.idle_us_min;
      continue;
    }
    accounting_->worker_idle();
    const auto epoch = parking_->prepare(id);
    // Publication either meets the announced idle bit and changes the epoch,
    // or precedes this final acquisition scan. Keep this handshake intact.
    if (try_help_one(id, true)) {
      backoff_us = cfg_.idle_us_min;
      continue;
    }
    parking_->wait(id, epoch, backoff_us, stop_);
    const auto next = std::max<uint64_t>(
        cfg_.idle_us_min,
        uint64_t{backoff_us} * backoff_growth_factor);
    backoff_us = static_cast<uint32_t>(std::min<uint64_t>(cfg_.idle_us_max, next));
  }
}

}  // namespace dagflow
