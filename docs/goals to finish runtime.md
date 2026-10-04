Статус сверён с кодом 2026-09-30. `[x]` — выполнено; `[ ]` — осталось или выполнено частично (см. пояснение). Проверки сохранения инвариантов относятся к текущей реализации и должны повторяться при её изменении.

Memory/topology закрыты по архитектуре и проверкам корректности. [Замеры](benchmarks/memory-topology.md) смешанные: tiny external submit и idle burst регрессируют; общая performance-оптимизация не завершена. [Wakeup follow-up](benchmarks/wakeup-profile.md) убрал no-idle RMW/epoch fallback и сократил регрессию, но не устранил её полностью.

- [x] **`TaskScope` — добить semantics**
    - [x] `spawn → outstanding → completion`.
    - [x] Root/sentinel credit.
    - [x] Child получает credit **до publication**.
    - [x] Child может spawn'ить descendants, пока держит credit.
    - [x] Отдельно `close admission` и `completed`.
    - [x] `cancel` не меняет lifetime protocol, только execution policy.
    - [x] First exception wins → request cancel → обычное retirement credits.
    - [x] Последний `1 → 0` публикует completion ровно один раз.
    - [x] После потенциально последнего `credit.finish()` больше **ни одного dereference scope state**.
    - [x] `TaskScope::~TaskScope()` = close + wait/join + только потом destroy.
    - [x] Тесты: nested spawn, concurrent spawn/close, cancellation, exception, last-credit race, destruction.

- [x] **Унифицировать completion**
    - [x] `CompletionCredit/State` оставить общей базой для `Handle`, `TaskGraph`, `TaskScope`.
    - [x] Не плодить второй termination protocol.
    - [x] Проверить happens-before `ready/error/payload destruction`.
    - [x] Сохранить правило final credit = storage-liveness до конца terminal propagation. Текущий механизм уже именно так устроен.

- [x] **Memory layer** — size classes принадлежат backend, отдельного allocator layer нет.
    - [x] `small_function` → SBO + spill через `runtime_memory`.
    - [x] `small_vector` → `runtime_memory`.
    - [x] Task packets → нативные size classes backend; mimalloc использует small-object path, system сохраняет точный new/delete.
    - [x] Completion states → тот же нативный small-object path; проверены alignment, remote free и профиль аллокации.
    - [x] Graph definitions/state → contiguous/bulk allocation.
    - [x] Chase–Lev **не трогать**: bounded fixed storage уже убирает reclamation.
    - [x] Не возвращать QSBR/HP туда, где lifetime можно поднять до owner/pool.

- [x] **Compiled graph execution** — [протокол и замеры](experiments/graph-execution.md).
    - [x] Builder отдельно от compiled CSR; nodes/edges contiguous, roots подготовлены на seal().
    - [x] RunState использует spans; NodeState[] переиспользуется между запусками.
    - [x] RunState allocation и initial-driver packets переиспользуются; отдельный CompletionState сохраняет независимость старых Handle. [Аллокации и tradeoff](experiments/graph-allocations.md).
    - [x] Options нормализуются на seal(); индексы/счётчики topology — uint32_t, tokens — size_t.
    - [x] Один invocation выполняется без token cursor; одна lane не требует атомарного lane join; один predecessor не требует arrival RMW.
    - [x] Для нескольких lanes/predecessors сохранены bounded CAS и acquire/release join.
    - [x] На точной границе бюджета не публикуется заведомо пустое продолжение.
    - [x] Проверены cancellation/reuse, duplicate edges, seal/publication failures и cooperative helping.
    - [ ] AoS/SoA, padding и reuse дополнительных lanes/token-continuations — отдельные эксперименты после профиля.

- [x] **Task packet representation**
    - [x] Один компактный `ScheduledTask`.
    - [x] На текущей Linux/Clang 64-bit сборке без diagnostics: `ScheduledTask` prefix = 32 B, align = 8; размер owning packet зависит от callable, graph initial packet = 48 B. Overflow link добавляет 8 B.
    - [x] Callable + completion + priority без лишних pointer chasing.
    - [x] `submit_detached` уже не создаёт отдельный completion state; packet общий. Обычный submit создаёт completion state и task отдельно. Дальнейшая специализация — только после профиля.

- [x] **Scheduler topology** — [контракт и layout](scheduler-topology.md).
    - [x] `Local[]` contiguous через runtime_memory.
    - [x] `Shard[]` contiguous через runtime_memory.
    - [x] У worker фиксированный `home_shard`; автоматическая balanced-карта или `Config::worker_shards`.
    - [x] Shard = injection/locality/parking domain; ParkingLot использует ту же membership-карту.
    - [x] External submit → выбранный shard, wakeup того же домена.
    - [x] Central MPMC drain пачкой → один task execute, остальные опубликованы в local Chase–Lev до исполнения.
    - [x] Periodic external probe budget, чтобы injector не starvation'ился.
    - [x] Steal сначала внутри home shard, потом remote; пустые домены поддерживаются.
    - [x] Affinity → worker hint → явный home shard. Hint вне диапазона нормализуется по worker count.
    - [x] `mt19937` заменён на owner-only SplitMix64 state (8 B); сравнение через benchmark suite.

- [x] **Overflow path** — [протокол](pool-lifecycle.md).
    - [x] Recursive `execute_task()` на saturation убран.
    - [x] Общая intrusive overflow FIFO на shard/priority, без новой аллокации при публикации; доступна helping и другим workers.
    - [x] Worker никогда не блокируется на queue capacity.
    - [x] External producer может получать настоящий backpressure.

- [x] **Parking / wakeup** — выполнено вместе с shard domains.
    - [x] Выделен отдельный `ParkingLot`.
    - [x] Worker регистрируется idle в своём shard.
    - [x] `wake_one` выбирает idle bit в домене; до 64 workers — один probe с cached domain mask, далее fallback по compact bitmap words. Worst-case O(1) не обещается.
    - [x] Lost-wakeup invariant описан и проверен race-тестами; повторная регистрация не стирается запоздавшим notifier.
    - [x] Сохранён CV/backoff; замена на atomic_wait/hybrid остаётся необязательным экспериментом по профилю.
    - [x] Сохранены wake_epoch + idle announcement + final scan и mutex handshake; paired SC fences позволяют убрать no-idle RMW/epoch fallback.

- [x] **Pool-wide accounting** — [контракт](idle-accounting.md).
    - [x] `outstanding_` заменён на single-writer published/retired lanes; shared add/sub на task убран.
    - [x] `wait_idle()` выполняет retirement/publication/retirement scan под registry mutex; зарегистрированные producers могут завершить свой поток раньше pool.
    - [x] На task остаётся owner release store; wake ожидающих перенесён в no-work path worker. Регистрация producer — отдельный cold path.

- [x] **Wait/help semantics**
    - [x] Сохранить worker-side cooperative `wait()` → helping. Сейчас worker при wait исполняет другую работу.
    - [x] Проверить nested waits.
    - [x] Проверить wait на scope/graph/handle внутри task.
    - [x] Для текущего scheduler проверены nested/cooperative waits, включая один worker; при redesign повторить проверки.

- [x] **Shutdown** — [контракт и проверки](pool-lifecycle.md).
    - [x] `close external admission → finish admitted publications → drain → stop/join → destroy scheduler`.
    - [x] Destructor использует graceful shutdown; own-worker shutdown запрещён.
    - [x] Queued tasks и descendants дренируются; Handle сохраняет результат. Pool не закрывает внешний sentinel TaskScope; scopes/graphs и concurrent callers обязаны закончить доступ до уничтожения Pool.
    - [x] Constructor partial-failure path оставить корректным.

- [ ] **CPU placement**
    - [ ] Вынести `pin_to_cpu` в topology/placement layer.
    - [ ] Реальные allowed CPUs.
    - [ ] Linux cpuset.
    - [ ] Windows processor groups.
    - [ ] Потом возможный NUMA/LLC mapping → shards.

- [ ] **После этого — freeze semantics**
    - [ ] Tag/commit: **semantic complete, perf unfinished**.
    - [ ] Никаких новых dynamic-DAG/market/blocking-compensation фич до benchmark pass.

- [x] **Benchmark harness** — [suite и методика](benchmarks/runtime-suite.md). Готовность harness не означает завершение performance study.
    - [x] local saturated;
    - [x] steal-heavy (при одном worker явно skipped);
    - [x] external submit, с handles и detached;
    - [x] idle→burst wake;
    - [x] nested spawn с cooperative waits;
    - [x] TaskScope recursive spawn;
    - [x] fan-out/fan-in;
    - [x] deep DAG;
    - [x] uneven task durations;
    - [x] graph reuse и отдельный build/run сценарий;
    - [x] task work sweep от почти нуля до µs: одна калибровка, одинаковые iterations между сборками;
    - [x] scaling по worker count: runner матрицы; проверены 1/2 workers, широкую серию проводить отдельно;
    - [x] throughput + p50/p99/p999: отдельные throughput/latency проходы, sample counts и сырые run durations в JSONL/CSV;
    - [x] perf/PMU: отдельные perf stat запуски, raw counters и статусы ошибок; smoke со счётчиками прошёл;
    - [x] Отдельно `Release`, `+LTO`, `+PGO`, `+LTO+PGO`: сборка, training/merge и короткая матрица из 384 точек проверены. Это проверка suite, не рейтинг производительности.
    - [x] Явный detached batch submit: API, backpressure/exception tests и paired scalar/batch benchmark; остаётся opt-in из-за смешанных результатов на nontrivial payload.

- [ ] **И только потом API polish**
    - [ ] STL/TBB vocabulary: часть naming уже приведена, общий API-проход не завершён.
    - [ ] concepts вместо разбросанных traits checks.
    - [ ] CPO только на настоящих customization boundaries.
    - [x] policies как values, если compile-time specialization не нужен.
    - [ ] `using value_type/size_type/...`, ranges/span interoperability: aliases и spans уже используются, общий проход не завершён.
    - [x] Не тащить `allocator_traits`-цирк, если allocator принадлежит runtime.
