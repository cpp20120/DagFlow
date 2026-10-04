# Worker-lane TLS cache — отклонённый эксперимент

Статус: удалён из рабочего C++ кода. Дата фиксации: 2026-09-30.

Эксперимент кешировал указатель на worker accounting lane в TLS, чтобы убрать
загрузки `pool → accounting → workers[index]` на каждой публикации и retirement.
Общего выигрыша не подтвердилось: в длинной серии single-worker local улучшился
примерно на 2%, а four-worker local замедлился примерно на 3–5%. Поэтому выбран
вариант без этого кеша. Более короткая серия давала оптимистичные 7,5%, которые
длинная серия не подтвердила; исходные результаты сохранены ниже.

Single-writer accounting lanes, release stores, idle snapshot и TLS-кеш
регистраций **внешних producers** остаются в реализации. Откат касается только
дополнительного TLS-указателя worker на свой lane. Cross-pool тест с `wait_idle()`
для обоих pools оставлен как полезная проверка корректности.

## Фрагмент реализации эксперимента

Это архивный код, а не действующая реализация. Lane продолжал принадлежать
`IdleAccounting`; TLS хранил только заимствованный указатель.

```cpp
// Borrowed only by its single writer; storage remains owned by IdleAccounting.
class alignas(CACHE_LINE_SIZE) AccountingLane {
 public:
  void publish() {
    const auto previous = published.load(std::memory_order_relaxed);
    if (previous == std::numeric_limits<uint64_t>::max()) publication_exhausted();
    published.store(previous + 1, std::memory_order_release);
  }
  void retire() noexcept {
    const auto previous = retired.load(std::memory_order_relaxed);
    if (previous == std::numeric_limits<uint64_t>::max()) std::terminate();
    retired.store(previous + 1, std::memory_order_release);
  }

 private:
  friend class IdleAccounting;
  [[noreturn]] static void publication_exhausted();
  std::atomic<uint64_t> published{0}, retired{0};
};
```

В `Pool` указатель устанавливался перед `worker_loop`, очищался после выхода,
использовался для same-pool publication и для retirement после `done.finish()`.
Другая pool по-прежнему использовала external producer path. Полный patch ниже
также сохраняет изменения объявления метода `execute_task` и его вызовов.

## Полный patch эксперимента

Направление patch: восстановленная версия без worker-кеша → архивный эксперимент.
Он сохраняет все изменения четырёх production-файлов этого эксперимента; это
материал для повторной проверки, а не рекомендация включить оптимизацию обратно.

```diff
--- a/src/idle_accounting.hpp
+++ b/src/idle_accounting.hpp
@@ -11,39 +11,47 @@
 #include "runtime_memory.hpp"
 
 namespace dagflow::detail {
+// Borrowed only by its single writer; storage remains owned by IdleAccounting.
+class alignas(CACHE_LINE_SIZE) AccountingLane {
+ public:
+  void publish() {
+    const auto previous = published.load(std::memory_order_relaxed);
+    if (previous == std::numeric_limits<uint64_t>::max()) publication_exhausted();
+    published.store(previous + 1, std::memory_order_release);
+  }
+  void retire() noexcept {
+    const auto previous = retired.load(std::memory_order_relaxed);
+    if (previous == std::numeric_limits<uint64_t>::max()) std::terminate();
+    retired.store(previous + 1, std::memory_order_release);
+  }
+
+ private:
+  friend class IdleAccounting;
+  [[noreturn]] static void publication_exhausted();
+  std::atomic<uint64_t> published{0}, retired{0};
+};
+
 // Each lane has exactly one writer. Observers read only while waiting for idle.
 // Registration/storage lifetime belongs to the pool, never to producer TLS.
 class IdleAccounting {
  public:
   explicit IdleAccounting(uint32_t workers);
   ~IdleAccounting();
-  void publish_worker(uint32_t worker) { publish(workers_[worker]); }
+  AccountingLane& worker_lane(uint32_t worker) noexcept { return workers_[worker]; }
+  void publish_worker(uint32_t worker) { worker_lane(worker).publish(); }
   void publish_external();
-  void retire(uint32_t worker) noexcept {
-    auto& retired = workers_[worker].retired;
-    const auto previous = retired.load(std::memory_order_relaxed);
-    if (previous == std::numeric_limits<uint64_t>::max()) std::terminate();
-    retired.store(previous + 1, std::memory_order_release);
-  }
+  void retire(uint32_t worker) noexcept { worker_lane(worker).retire(); }
   void worker_idle();
   void wait();
 
  private:
-  struct alignas(CACHE_LINE_SIZE) Lane {
-    std::atomic<uint64_t> published{0}, retired{0};
-  };
+  using Lane = AccountingLane;
   struct Producer {
     Lane lane;
     uint64_t id;
     OwnedObject<Producer> next;
     explicit Producer(uint64_t producer) : id(producer) {}
   };
-  [[noreturn]] static void publication_exhausted();
-  static void publish(Lane& lane) {
-    const auto previous = lane.published.load(std::memory_order_relaxed);
-    if (previous == std::numeric_limits<uint64_t>::max()) publication_exhausted();
-    lane.published.store(previous + 1, std::memory_order_release);
-  }
   Lane& producer_lane();
   bool idle_locked() const noexcept;
 
--- a/src/idle_accounting.cpp
+++ b/src/idle_accounting.cpp
@@ -33,7 +33,7 @@
     producers_ = std::move(next);
   }
 }
-void IdleAccounting::publication_exhausted() {
+void AccountingLane::publication_exhausted() {
   throw std::overflow_error("pool publication counter exhausted");
 }
 IdleAccounting::Lane& IdleAccounting::producer_lane() {
@@ -57,7 +57,7 @@
   entry = {identity_, &producers_->lane};
   return *entry.lane;
 }
-void IdleAccounting::publish_external() { publish(producer_lane()); }
+void IdleAccounting::publish_external() { producer_lane().publish(); }
 bool IdleAccounting::idle_locked() const noexcept {
   // Registry is fixed under mutex_. Retirements are monotonic: equal totals
   // in both collections mean every observed retirement lane stayed stable.
--- a/include/thread_pool.hpp
+++ b/include/thread_pool.hpp
@@ -24,6 +24,7 @@
 class Scheduler;
 class ParkingLot;
 class IdleAccounting;
+class AccountingLane;
 struct ScheduledTask;
 }  // namespace detail
 
@@ -220,7 +221,7 @@
   bool try_help_one(uint32_t id, bool parking_announced = false);
 
   /// Execute and recycle a task on its owning pool worker (also on overflow).
-  void execute_task(uint32_t id, Task* t);
+  void execute_task(Task* t);
 
 
   /// TLS: current worker id (UINT32_MAX if not in pool thread).
@@ -228,6 +229,9 @@
 
   /// TLS: owning pool, or nullptr on an external thread.
   static thread_local Pool* tls_pool_;
+
+  /// Borrowed worker lane, valid until this worker exits and the pool joins it.
+  static thread_local detail::AccountingLane* tls_accounting_lane_;
 
   /// Configuration.
   Config cfg_;
--- a/src/thread_pool.cpp
+++ b/src/thread_pool.cpp
@@ -35,6 +35,7 @@
 
 thread_local uint32_t Pool::tls_id_ = UINT32_MAX;
 DAGFLOW_TLS_ATTR thread_local Pool* Pool::tls_pool_ = nullptr;
+DAGFLOW_TLS_ATTR thread_local detail::AccountingLane* Pool::tls_accounting_lane_ = nullptr;
 
 static void pin_to_cpu(uint32_t idx) {
 #if defined(_WIN32)
@@ -61,9 +62,11 @@
     for (uint32_t i = 0; i < n; ++i) {
       threads_.emplace_back([this, i] {
         tls_id_ = i;
+        tls_accounting_lane_ = &accounting_->worker_lane(i);
         tls_pool_ = this;
         if (cfg_.pin_threads) pin_to_cpu(i);
         worker_loop(i);
+        tls_accounting_lane_ = nullptr;
         tls_pool_ = nullptr;
         tls_id_ = UINT32_MAX;
       });
@@ -105,7 +108,7 @@
   task->prio = opt.priority;
   task->done = std::move(completion);
   if (tls_pool_ == this)
-    accounting_->publish_worker(tls_id_);
+    tls_accounting_lane_->publish();
   else
     accounting_->publish_external();
   // Successful publication revokes all access to task, including its credit.
@@ -124,7 +127,7 @@
     if (!accepted) {
       // Workers cannot block for queue space. Publish or execute the incoming
       // task before helping tasks that might themselves depend on it.
-      execute_task(tls_id_, t);
+      execute_task(t);
       return;
     }
     parking_->wake_one(shard);
@@ -290,7 +293,7 @@
   accounting_->wait();
 }
 
-void Pool::execute_task(uint32_t id, Task* t) {
+void Pool::execute_task(Task* t) {
   try {
     if (t->fn) t->fn(t->done);
   } catch (...) {
@@ -304,7 +307,7 @@
   detail::ObjectDeleter<Task>{}(t);
   done.finish();
 
-  accounting_->retire(id);
+  tls_accounting_lane_->retire();
 }
 
 bool Pool::try_help_one(uint32_t id, bool parking_announced) {
@@ -313,7 +316,7 @@
     // Batches are visible before execution, including during nested waits.
     if (scheduler_->has_local_work(id))
       parking_->wake_one(scheduler_->home_shard(id));
-    execute_task(id, task);
+    execute_task(task);
     return true;
   }
   return false;
```

## Замеры и проверки эксперимента


The experimental worker borrowed its `AccountingLane*` once at thread startup and cached
it in TLS. Same-pool publication and retirement use that pointer directly.
Worker exit clears the pointer before the pool joins and destroys accounting
storage. Cross-pool submission still takes the destination's external producer
path. The cross-pool test now drains both pools to verify that each accounting
balance is correct. Counter ordering, overflow checks, registration and idle
observation are unchanged; no new per-task allocation or metadata was added.

In the static Release assembly, retirement changed from loading
`Pool::accounting_`, loading the worker array and scaling the worker index to one
TLS pointer load followed by the existing counter load/store. The unused worker
ID argument was removed from `execute_task`, and the compiler saves fewer
registers there.

The initial three-trial/21-repeat matrix showed single-worker zero-body local
saturation about 7.5% faster, but other results were mixed, including slower
four-worker handles and local saturation. A focused series checked six points
with seven alternating trials, ten warmups and 101 repetitions per invocation.
It restricted each process to workers+1 distinct allowed physical cores via
`taskset`; this is a CPU-set restriction, not fixed per-thread placement or a
frequency lock. Both binaries execute 4,096 zero-body task units per run.

| Workers | Scenario | Before median (µs) | After median (µs) | Ratio of medians | Median paired ratio |
|---:|---|---:|---:|---:|---:|
| 1 | `idle_burst` | 349.802 | 366.917 | 1.049 | 1.011 |
| 1 | `local_saturated` | 162.063 | 158.943 | 0.981 | 0.979 |
| 4 | `external_detached` | 1841.041 | 1814.466 | 0.986 | 1.012 |
| 4 | `external_handles` | 3368.119 | 3404.632 | 1.011 | 0.989 |
| 4 | `local_saturated` | 497.758 | 514.668 | 1.034 | 1.053 |
| 4 | `scope_recursive` | 692.406 | 743.973 | 1.074 | 0.973 |

Ratios below 1 mean less time. The longer series supports only a small local
single-worker improvement (about 2%), not the initial 7.5% estimate. Four-worker
local saturation was slower in all seven pairs (about 3–5% by these aggregates).
Other points contain large outliers; the difference between aggregate methods
is visible above. Raw results retain every run and no outlier was removed.
A general throughput improvement is **not established**, and the earlier
single-worker regression is not demonstrated to be fully resolved.

These measurements cover static Release only. Shared-library builds have a
different TLS access model; they passed correctness tests but their throughput
was not measured here. Final checks: system 18/18 (plus the changed cross-pool
test rerun), mimalloc 18/18, tbbmalloc 17/17, ASan/UBSan 19/19 with leak detection
disabled, and nine targeted TSan tests.

Local artifacts are in `out/benchmarks/lane-cache/`: `before`, `after`,
`compare.py`, `paired.jsonl`, `summary.json`, `focused.py`, `focused.jsonl`,
`focused-summary.json`, exact focused commands, binary/source hashes, before/
after assembly and test logs. The full initial matrix is preserved alongside
the focused results. The ownership rule was: the TLS pointer only borrows storage, and joining
workers precedes lane destruction.
