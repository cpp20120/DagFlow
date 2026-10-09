#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <span>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>


#include <dagflow/config.hpp>
#include <dagflow/handle.hpp>
#include <dagflow/detail/runtime_diagnostics.hpp>


namespace dagflow {

namespace detail {
class Scheduler;
class ParkingLot;
class IdleAccounting;
}  // namespace detail

enum class Priority : uint8_t { High = 0, Normal = 1 };

/// Spawn prefers worker-local execution; Enqueue uses the shared ingress.
enum class SubmissionMode : uint8_t { Spawn, Enqueue };

struct Config {
  uint32_t threads = std::thread::hardware_concurrency();
  uint32_t shards = 0;
  uint32_t central_batch = DAGFLOW_DEFAULT_CENTRAL_BATCH;
  uint32_t idle_us_min = DAGFLOW_DEFAULT_IDLE_US_MIN;
  uint32_t idle_us_max = DAGFLOW_DEFAULT_IDLE_US_MAX;
  bool pin_threads = true;
  /// Empty selects balanced contiguous worker groups. Otherwise one valid
  /// shard ID per worker; shards=0 still means one shard per worker.
  /// Storage uses the runtime backend. Copy another allocator's vector with
  /// assign(begin, end); initializer-list assignment is supported directly.
  std::vector<uint32_t, detail::RuntimeAllocator<uint32_t>> worker_shards;
};

struct SubmitOptions {
  /// Worker hint mapped through its home shard. Out-of-range hints wrap by
  /// worker count; placement is not a guarantee of execution on that worker.
  std::optional<uint32_t> affinity;
  Priority priority = Priority::Normal;
  SubmissionMode mode = SubmissionMode::Spawn;
};

namespace detail {
struct ScheduledTask;

struct ScheduledTaskOps {
  void (*invoke)(ScheduledTask*);
  void (*destroy)(ScheduledTask*) noexcept;
};

/// Common scheduler-visible prefix. The concrete callable lives inline in a
/// ScheduledTaskModel<Callable> allocation; queues move only this base pointer.
struct ScheduledTask {
  const ScheduledTaskOps* ops{};
  // Used only while owned by a shard's overflow queue, under its mutex.
  // Keeping the link in the packet makes overflow publication allocation-free.
  ScheduledTask* overflow_next{};
  Priority prio{Priority::Normal};
  CompletionCredit done;
#if defined(DAGFLOW_RUNTIME_DIAGNOSTICS)
  uint64_t allocating_thread{runtime_diagnostic_thread()};
#endif

  ScheduledTask() = default;
  ScheduledTask(const ScheduledTaskOps* task_ops, Priority priority,
                CompletionCredit completion) noexcept
      : ops(task_ops), prio(priority), done(std::move(completion)) {}
};

struct ScheduledTaskDeleter {
  void operator()(ScheduledTask* task) const noexcept {
    if (task) task->ops->destroy(task);
  }
};

using OwnedScheduledTask =
    std::unique_ptr<ScheduledTask, ScheduledTaskDeleter>;

template <class Callable>
struct ScheduledTaskModel final : ScheduledTask {
  static_assert(std::is_nothrow_destructible_v<Callable>,
                "task callable destruction must not throw");

  template <class Function>
  ScheduledTaskModel(Function&& function, Priority priority,
                     CompletionCredit completion)
      : ScheduledTask(&operations, priority, std::move(completion)),
        callable(std::forward<Function>(function)) {}

  static void invoke(ScheduledTask* task) {
    auto* self = static_cast<ScheduledTaskModel*>(task);
    std::invoke(self->callable, self->done);
  }

  static void destroy(ScheduledTask* task) noexcept {
    auto* self = static_cast<ScheduledTaskModel*>(task);
    std::destroy_at(self);
    deallocate_bytes(self, alignof(ScheduledTaskModel));
  }

  static constexpr ScheduledTaskOps operations{&invoke, &destroy};
  [[no_unique_address]] Callable callable;
};

template <class Callable>
[[nodiscard]] OwnedScheduledTask make_scheduled_task(
    Callable&& callable, Priority priority, CompletionCredit completion) {
  using Model = ScheduledTaskModel<std::decay_t<Callable>>;
  void* storage = allocate_bytes(sizeof(Model), alignof(Model));
  runtime_count(RuntimeEvent::packet_allocations);
  runtime_count(RuntimeEvent::packet_allocated_bytes, sizeof(Model));
  try {
    return OwnedScheduledTask{std::construct_at(
        static_cast<Model*>(storage), std::forward<Callable>(callable), priority,
        std::move(completion))};
  } catch (...) {
    deallocate_bytes(storage, alignof(Model));
    throw;
  }
}
}  // namespace detail

/// Raw function signature for low-level submission.
using Fn = void (*)(void*);

/// Work-stealing executor. Destruction drains accepted work and joins workers.
/// Graphs/scopes and concurrent callers must finish using the pool before its
/// storage is destroyed. Borrowed task data must survive the drain.
class Pool {
 public:
  explicit Pool(const Config& cfg = {});

  ~Pool();

  Pool(const Pool&) = delete;
  Pool& operator=(const Pool&) = delete;

  Handle submit(Fn function, void* arg = nullptr, SubmitOptions opt = {});

  /// External callers wait on queue saturation; workers spill without invoking
  /// the callable in submit(). Closed external admission throws logic_error.
  template <class F>
  Handle submit(F f, SubmitOptions opt = {}) {
    return submit_impl(
        [fn = std::move(f)](detail::CompletionCredit&) mutable { fn(); },
        std::move(opt));
  }

  /// Submit without a per-task completion handle. Track lifetime at the caller.
  /// Uncaught task exceptions are discarded; capture/report errors explicitly.
  template <class F>
  void submit_detached(F f, SubmitOptions opt = {}) {
    enqueue([fn = std::move(f)](detail::CompletionCredit&) mutable { fn(); },
            opt, {});
  }

  /// Consume callable objects by move; no per-task completion state is created.
  ///
  /// Protocol (the comments here are part of the contract): for an external
  /// caller each <=64-job group is fully constructed, chooses one shard, then
  /// publishes each packet in this order: producer-lane publication -> MPMC
  /// queue release -> one parking wake. A worker can therefore observe the
  /// accounting publication before it observes/executes the packet, and the
  /// final wake is allowed to race with queue draining. Retirement happens in
  /// the ordinary execute_task epilogue after callable/packet destruction.
  ///
  /// A group is not a transaction. Earlier groups may run before a later group
  /// throws, and a publication/registration failure may leave an accepted
  /// prefix. Callers must keep the span's borrowed data alive until accepted
  /// tasks finish, including when the call throws; input elements may be moved
  /// from even when their task was not accepted. `wait_idle()` is the lifetime
  /// barrier for that accepted work, not a barrier for future submissions.
  ///
  /// Calls from this pool's worker deliberately use scalar enqueue: shared
  /// overflow remains visible to cooperative helping. We do not
  /// hide a TLS producer buffer (it changes visibility/backpressure), add a
  /// batch completion credit (detached work has no result to wait on), or claim
  /// multiple MPMC slots with one CAS (ring_mpmc has per-slot ownership and a
  /// stalled producer must not block publication of unrelated slots).
  /// Empty spans do nothing. No task remains buffered after successful return.
  template <class F, std::size_t Extent>
    requires (!std::is_const_v<F> && std::is_invocable_r_v<void, F&>)
  void submit_batch_detached(std::span<F, Extent> tasks, SubmitOptions opt = {}) {
    if (tls_pool_ == this) {
      for (auto& task : tasks)
        enqueue([fn = std::move(task)](detail::CompletionCredit&) mutable { fn(); },
                opt, {});
      return;
    }

    constexpr std::size_t batch_limit = 64;
    for (std::size_t base = 0; base < tasks.size();) {
      const auto size = std::min(batch_limit, tasks.size() - base);
      detail::OwnedScheduledTask pending[batch_limit];
      for (std::size_t i = 0; i < size; ++i) {
        pending[i] = detail::make_scheduled_task(
            [fn = std::move(tasks[base + i])](detail::CompletionCredit&) mutable {
              fn();
            },
            opt.priority, {});
      }
      enqueue_batch_prepared(std::span{pending, size}, opt);
      base += size;
    }
  }

  /// Process a forward range in approximately 16K-element chunks.
  template <class It, class F>
  Handle for_each(It begin, It end, F f, SubmitOptions opt = {}) {
    const auto n = static_cast<std::size_t>(std::distance(begin, end));
    if (n == 0) return Handle{};
    const std::size_t target = DAGFLOW_DEFAULT_RANGE_CHUNK;
    std::size_t chunks = (n + target - 1) / target;
    if (chunks == 0) chunks = 1;

    auto callable_owner = detail::make_owned<F>(std::move(f));
    F* callable = callable_owner.get();  // Borrow protected by every task credit.
    auto publication = detail::CompletionCredit::create(std::move(callable_owner));
    auto result = publication.handle();
    try {
      for (std::size_t c = 0; c < chunks; ++c) {
        const std::size_t lo = c * n / chunks;
        const std::size_t hi = (c + 1) * n / chunks;
        auto base = std::next(begin, static_cast<std::iter_difference_t<It>>(lo));
        enqueue([base, count = hi - lo, callable](detail::CompletionCredit&) mutable {
          auto it = base;
          for (std::size_t i = 0; i < count; ++i, ++it) (*callable)(*it);
        }, opt, publication.fork());
      }
    } catch (...) {
      publication.finish();
      // Drain all borrowed range users before a publication error escapes.
      wait(result);
      throw;
    }
    return result;  // Sentinel retires; the last credit destroys the callable.
  }
  /// Asynchronously process an lvalue range or a borrowed temporary (e.g. span).
  /// No elements are owned/copied: keep the backing data and any iterator-owning
  /// view alive and unmodified until completion, including during unwinding.
  /// A single callable is shared across tasks; concurrent calls must be safe.
  template <std::ranges::forward_range R, class F>
    requires std::ranges::borrowed_range<R>
  Handle for_each(R&& range, F f, SubmitOptions opt = {}) {
    auto begin = std::ranges::begin(range);
    // Materialize an iterator end for sentinel-based ranges. Common ranges
    // simply copy end; a non-common forward range may need a traversal.
    auto end = std::ranges::next(begin, std::ranges::end(range));
    return for_each(begin, end, std::move(f), opt);
  }

  /// Number of worker threads.
  [[nodiscard]] uint32_t thread_count() const noexcept {
    return cfg_.threads ? cfg_.threads : DAGFLOW_MIN_WORKER_COUNT;
  }

  /// Recursively split a random-access range, sharing one callable instance.
  template <class It, class F>
  Handle for_each_ws(It begin, It end, F f, SubmitOptions opt = {},
                     std::size_t min_grain_hint = DAGFLOW_DEFAULT_RANGE_CHUNK) {
    static_assert(std::random_access_iterator<It>,
                  "for_each_ws requires random-access iterators");

    const auto n = static_cast<std::size_t>(end - begin);
    if (n == 0) return Handle{};

    const uint32_t T = this->thread_count();
    std::size_t min_grain = std::max<std::size_t>(n / (T * 8u), min_grain_hint);
    min_grain = std::clamp<std::size_t>(min_grain, 1, n);

    struct Range {
      std::size_t lo, hi;
    };

    struct ProcState {
      Pool* pool;
      It begin;
      F func;
      SubmitOptions opt;
      std::size_t min_grain;
    };
    auto owner = detail::make_owned<ProcState>(
        ProcState{this, begin, std::move(f), opt, min_grain});
    ProcState* state = owner.get();
    auto publication = detail::CompletionCredit::create(std::move(owner));
    auto result = publication.handle();

    struct Exec {
      ProcState* state;  // Borrow only while the passed credit is live.
      void operator()(Range range, detail::CompletionCredit& credit) const {
        while (range.hi - range.lo > state->min_grain &&
               range.hi - range.lo - state->min_grain > state->min_grain) {
          const std::size_t mid = range.lo + ((range.hi - range.lo) >> 1);
          const Range upper{mid, range.hi};
          range.hi = mid;
          Exec continuation{state};
          state->pool->enqueue(
              [continuation, upper](detail::CompletionCredit& child) {
                continuation(upper, child);
              }, state->opt, credit.fork());
        }
        auto it = std::next(state->begin, static_cast<std::iter_difference_t<It>>(range.lo));
        for (std::size_t i = range.lo; i < range.hi; ++i, ++it) state->func(*it);
      }
    };

    try {
      const std::size_t tiles = n / min_grain >= 4 && T > 1 ? T : 1;
      for (std::size_t t = 0; t < tiles; ++t) {
        const Range range{n * t / tiles, n * (t + 1) / tiles};
        auto local_options = state->opt;
        if (tiles > 1) local_options.affinity = static_cast<uint32_t>(t);
        enqueue([exec = Exec{state}, range](detail::CompletionCredit& credit) {
          exec(range, credit);
        }, local_options, publication.fork());
      }
    } catch (...) {
      publication.finish();
      wait(result);
      throw;
    }
    return result;
  }

  /// Range overload with the same borrowed-lifetime/shared-callable contract
  /// as for_each(). Owning temporaries are rejected at compile time.
  template <std::ranges::random_access_range R, class F>
    requires std::ranges::borrowed_range<R>
  Handle for_each_ws(R&& range, F f, SubmitOptions opt = {},
                     std::size_t min_grain_hint = DAGFLOW_DEFAULT_RANGE_CHUNK) {
    auto begin = std::ranges::begin(range);
    auto end = std::ranges::next(begin, std::ranges::end(range));
    return for_each_ws(begin, end, std::move(f), opt, min_grain_hint);
  }

  Handle combine(std::span<const Handle> handles, SubmitOptions opt = {});
  /// Convenience overload from initializer_list.
  Handle combine(std::initializer_list<Handle> hs, SubmitOptions opt = {}) {
    return combine(std::span(hs.begin(), hs.size()),
                   std::move(opt));
  }

  /// Workers help execute queued tasks while waiting; external callers block.
  /// Completion-only: use h.rethrow_if_failed() after waiting to observe
  /// errors.
  void wait(const Handle& h);

  /// Wait cooperatively when called from a worker, then rethrow task failure.
  /// An empty handle is a completed no-op, just as for wait().
  void wait_and_rethrow(const Handle& h) {
    wait(h);
    h.rethrow_if_failed();
  }

  /// Wait for queued and executing tasks, including their children, to finish.
  /// External callers only; concurrent submissions may begin after return.
  void wait_idle();

  /// Close external task admission without waiting. Idempotent; worker-safe.
  /// Already admitted publishers finish, and this pool's executing workers may
  /// still submit descendants. A different pool's worker is an external caller.
  /// Admission is per packet / prepared batch group, not per graph or range.
  void close() noexcept;

  /// True once external admission closes; does not imply drained/joined.
  [[nodiscard]] bool closed() const noexcept;

  /// Close, finish admitted publications, drain all tasks/descendants and join.
  /// Idempotent, including concurrent calls. Throws logic_error on own worker.
  /// Does not cancel tasks or rethrow their errors; Handles retain results.
  /// Concurrent external callers still need joining before Pool destruction.
  void shutdown();

 private:
  friend class TaskGraph;
  friend class TaskScope;
  using Task = detail::ScheduledTask;

  // External publishers reserve admission before touching task accounting.
  // Workers already hold an outstanding task until after descendant publication
  // and capture destruction, so they need no shared admission RMW.
  class PublicationGuard {
   public:
    explicit PublicationGuard(Pool& pool);
    ~PublicationGuard();
    PublicationGuard(const PublicationGuard&) = delete;
    PublicationGuard& operator=(const PublicationGuard&) = delete;
   private:
    Pool* pool_;
  };
  void enter_publication();
  void leave_publication() noexcept;

  template <class Callable>
  Handle submit_impl(Callable&& job, SubmitOptions opt) {
    auto credit = detail::CompletionCredit::create();
    auto handle = credit.handle();
    enqueue(std::forward<Callable>(job), opt, std::move(credit));
    return handle;
  }

  /// Consumes completion on both success and failure. The concrete callable is
  /// constructed directly inside one variable-sized task allocation; scheduler
  /// queues erase only the resulting ScheduledTask* pointer.
  template <class Callable>
  void enqueue(Callable&& job, SubmitOptions opt,
               detail::CompletionCredit completion) {
    auto task = detail::make_scheduled_task(
        std::forward<Callable>(job), opt.priority, std::move(completion));
    enqueue_prepared(std::move(task), opt);
  }

  void enqueue_prepared(detail::OwnedScheduledTask task,
                        const SubmitOptions& opt);
  void enqueue_batch_prepared(
      std::span<detail::OwnedScheduledTask> tasks,
      const SubmitOptions& opt);

  void dispatch(Task* task, const SubmitOptions& opt);

  void worker_loop(uint32_t id);

  bool try_help_one(uint32_t id, bool parking_announced = false);

  /// Execute and recycle a task on a worker of its owning pool.
  void execute_task(uint32_t id, Task* task);


  /// TLS: current worker id (UINT32_MAX if not in pool thread).
  static thread_local uint32_t tls_id_;

  /// TLS: owning pool, or nullptr on an external thread.
  static thread_local Pool* tls_pool_;

  /// Configuration.
  Config cfg_;

  /// Worker threads.
  std::vector<std::thread, detail::RuntimeAllocator<std::thread>> threads_;

  /// Queue placement, batching, and work stealing, independent of threads.
  detail::OwnedObject<detail::Scheduler> scheduler_;
  /// Shares scheduler membership; destroyed before scheduler storage.
  detail::OwnedObject<detail::ParkingLot> parking_;
  detail::OwnedObject<detail::IdleAccounting> accounting_;

  // High bit closes admission, low bits count external publishers, including
  // those blocked on ingress capacity. Do not replace this with a bool check:
  // shutdown could otherwise observe idle before a checked submit publishes.
  static constexpr uint64_t admission_closed_bit = uint64_t{1} << 63;
  alignas(DAGFLOW_CACHE_LINE_SIZE) std::atomic<uint64_t> admission_{0};
  std::mutex shutdown_mutex_;
  alignas(DAGFLOW_CACHE_LINE_SIZE) std::atomic<bool> stop_{false};

};

}  // namespace dagflow
