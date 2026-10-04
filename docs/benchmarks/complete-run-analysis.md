# Разбор complete-run от 2026-10-02

Главный кандидат для оптимизации — взаимодействие подачи мелких задач,
поиска работы и park/wake при недостаточно заполненных очередях. Широкие
графы с полезной работой около 1 мкс на узел масштабируются существенно лучше.
Измерения подтверждают проблемы отдельных сценариев, но без профиля CPU не
устанавливают долю времени каждого участка кода.

## Данные и границы выводов

- [Общий manifest](../../out/benchmarks/complete-run/manifest.json): system allocator,
  Clang 22.1.8, workers=1/2/4/8, наследуемая CPU affinity.
- [Runtime results](../../out/benchmarks/complete-run/runtime/results.jsonl):
  448 строк, 440 успешных и 8 ожидаемых пропусков stealing на одном worker.
  По 9 измерений после 2 прогревочных повторений в одном процессе на точку.
  Хеши всех файлов из runtime source manifest совпали с текущим деревом.
- [Stress summary](../../out/benchmarks/complete-run/stress/summary.json):
  35 конфигураций, O3 и O3+Full LTO, по 3 независимых процесса. Времена ниже —
  медиана медиан процессов. Диагностика запускалась отдельно от timing.
- [API comparison](../../out/benchmarks/complete-run/public-api/tbb/comparison.md):
  5 повторений, 8 workers/concurrency. DagFlow запускает 8 worker threads;
  oneTBB может использовать приложение и до 7 scheduler workers.
- Все четыре этапа верхнего runner завершились с кодом 0.
- Perf выключен, свежих flamegraphs и PMU-счётчиков нет. В manifest записан
  governor powersave; потоки не закреплены за отдельными физическими ядрами.
  Это ограничения контроля эксперимента, а не доказательство причины просадок.
  Небольшие отличия, в частности 6% на тяжёлых independent tasks, пока не стоит
  превращать в диагноз рантайма.

## 1. Поиск работы и park/wake — первый приоритет

Пустая полезная нагрузка, O3, 4096 задач:

| Stress case | 1 worker, мкс | 8 workers, мкс | Изменение времени |
|---|---:|---:|---:|
| local-overflow | 198.37 | 3087.84 | ×15.57 |
| hot-shard-skew | 782.01 | 3410.94 | ×4.36 |
| nested-helping | 900.29 | 2724.59 | ×3.03 |
| mixed-chaos | 480.41 | 1200.49 | ×2.50 |

Для local-overflow (cases 24/26) медиана по процессам их медианного числа
добровольных переключений контекста на прогон: **3 → 2014**. Суммарное
system CPU time по потокам при 8 workers — около 8.24 мс при wall time 3.09 мс.
Это данные обычных timing binaries, а не диагностических сборок.

В отдельной O3-диагностике case 26 на логическую задачу приходится:

- 28.28 steal probes, из них 26.75 пустых;
- 1.355 успешных переносов stealing: один packet может перемещаться повторно;
- 1.815 wake calls, 0.774 wake signals и 0.637 park calls;
- около одного cross-thread free.

Счётчики указывают на интенсивное взаимодействие потоков при малом количестве
готовой работы. Они не являются процентами CPU time; инструментирование само
влияет на расписание. Однако переключения контекста в timing подтверждают,
что эффект не ограничивается диагностической сборкой.

Участки для разбора:

- `src/scheduler.cpp`, `Scheduler::try_acquire` и `steal`: сканирование shards,
  очередей обоих приоритетов и victims; перенос до четырёх packets за steal.
- `src/thread_pool.cpp`, `worker_loop`: после неудачного acquire — объявление
  idle, повторный полный acquire и переход к parking.
- `src/thread_pool.cpp`, `dispatch` / `try_help_one`: wake при публикации и
  relay после shared acquisition/stealing.
- `src/parking_lot.cpp`, `wake_one` / `signal` / `wait`: fenced idle-probe,
  общие idle bits, handshake через mutex и condition_variable.

Гипотеза для следующего отдельного эксперимента: политика поиска/парковки и
привлечения workers слишком дорога для такой скорости поступления мелких задач.
Проверять ограниченный поиск и адаптивное ожидание надо с сохранением final-scan
и wake-relay протокола: удаление wake/fence без нового доказательства прогресса
может потерять пробуждение.

## 2. Scalar submit и освобождение packets

API noop: DagFlow **457.30 мс**, oneTBB **69.88 мс** на миллион задач —
DagFlow медленнее примерно в **6.54 раза**. Его диагностическая сборка
регистрирует миллион packet allocations и 45.78 MiB запрошенных байтов.
Это сумма запросов, не пиковая память и не измерение стоимости allocator.

Stress с 8 workers / 8 shards / одним producer:

| Submit batch | Время O3, мкс | Пустые steal probes/task, diagnostics |
|---|---:|---:|
| scalar | 3424.04 | 32.20 |
| 16 | 1355.14 | 4.80 |
| 64 | 1105.46 | 2.75 |

Batch=64 ускоряет этот случай в 3.10 раза. При 4 producers и 8 shards:
scalar 479.63 мкс → batch=64 287.90 мкс, ускорение 1.67 раза.

Scalar путь включает packet allocation, admission CAS и release decrement,
выбор shard, accounting, ingress publication и wake. В API без affinity
`Scheduler::select_shard` также делает общий `fetch_add`; стрессовые планы
с affinity могут обходить этот конкретный шаг. Внешние задачи освобождают
workers, поэтому стоимость межпоточного освобождения тоже заслуживает профиля.

Batch амортизирует admission/routing/wake и меняет размещение задач. По этим
данным нельзя приписать весь выигрыш одному fence или allocator. Сравнения
system/mimalloc/tbbmalloc на этой матрице нет. Следующий кандидат после
park/wake — стоимость packet allocation/free и scalar publication.

## 3. TaskScope: общая точка синхронизации на каждом spawn

Runtime Release, 4096 пустых payloads:

| Сценарий | 1 worker, мкс | 8 workers, мкс |
|---|---:|---:|
| scope_recursive | 239.4 | 753.5 |
| nested_spawn | 319.4 | 97.3 |

В `src/task_scope.cpp:45`, `TaskScope::reserve`, mutex `state.admission`
берётся и для дочернего `Context::spawn`, после чего выполняется fork общего
completion credit. Это конкретный кандидат на сериализацию рекурсивной
подачи. Эти два benchmark имеют разные контракты и структуру ожиданий;
отношение времён не измеряет отдельно цену mutex.

При payload около 1 мкс scope уже масштабируется: 4249 → 769 мкс, но
nested_spawn на 8 workers занимает 659 мкс. Для оптимизации reserve требуется
сохранить контракты close/cancel и принятия дочерней работы.

## 4. Графы: отделить построение от выполнения

Runtime Release, payload около 1 мкс, 4096 payloads:

| Сценарий | 1 worker, мкс | 8 workers, мкс | Ускорение |
|---|---:|---:|---:|
| graph_reuse | 4202.1 | 606.5 | ×6.93 |
| fanout_fanin | 4259.4 | 655.7 | ×6.50 |
| graph_tokens_parallel | 4035.0 | 589.6 | ×6.84 |
| nested_spawn | 4393.9 | 658.6 | ×6.67 |

На пустых узлах graph_reuse ухудшается с 240.5 до 1390.9 мкс: общая цена
публикаций, completion и передачи работы важна даже без per-node packet malloc.
В `TaskGraph::run` независимые roots публикуются последовательно через
`publish_initial` и общий completion credit.

Последовательный deep_dag ожидаемо не получает параллельного ускорения.
Но пустая цепочка дорожает с 89.9 до 264.6 мкс при 1 → 8 workers.
В `TaskGraph::execute` budget уменьшается за выполнение и переход к successor;
при текущем execution_budget=64 обычная цепочка передаёт управление примерно
каждые 32 узла. Это кандидат для проверки цены handoff и миграции цепочки,
а не основание просто убрать ограничение fairness.

API chain включает build/seal/run/destruction: 298.0 мкс против 129.9 мкс TBB.
DagFlow делает 31 runtime allocation, запрашивает суммарно 741.38 KiB,
при этом packet allocations равны нулю. API workflow: 55.4 против 37.5 мкс,
103 runtime allocations на граф из 50 узлов. Эти результаты требуют
отдельного рассмотрения builder/storage, а не только scheduler.

`graph_tokens_parallel` дополнительно содержит benchmark-owned atomic ticket
для выбора выходного слота. Пустые tokens нельзя считать чистым замером
token scheduler; при анализе этой ветки следует устранить или отдельно учесть
влияние ticket.

## Что эта матрица не проверила

- Во всех 70 stress summary rows `overflow_push=overflow_acquire=0`.
  Case local-overflow с 4096 задачами переполняет local deque на одном worker,
  но остаток помещается в ingress. Mutex shared overflow queue не нагружен.
  Для его проверки нужен отдельный 1-worker случай с большим N, например
  65536, с проверкой ненулевых overflow counters.
- Нет отдельного точного enqueue-to-dequeue timestamp: runtime latency означает
  submit-to-start либо graph-run-to-start и включает другие стадии.
- Stress latency выключена, кроме автоматически инструментированного idle-burst.
  Раздельных latency-распределений для приоритетов и измерения starvation нет.
- Нет прикладной нагрузки или NUMA-эксперимента. Увеличение workers само по себе
  их не заменяет.

Приоритет дальнейшего разбора: **park/wake и поиск работы → scalar publication
и packet lifetime → TaskScope admission → графовый builder и handoff**.
Переписывать MPMC/Chase–Lev целиком по этим данным оснований пока нет.
Новые измерения при подготовке этого разбора не запускались.
