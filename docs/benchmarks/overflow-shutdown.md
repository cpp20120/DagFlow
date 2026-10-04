# Overflow/shutdown: стоимость контракта

Изменение закрывает recursive worker overflow и stop-only destruction.
[Контракт и проверка lifetime](../pool-lifecycle.md). Это исправление поведения,
не обещание общего ускорения.

## Методика

Clang 22.1.8, Release `-O3 -g -DNDEBUG`, static library, mimalloc, **без LTO**.
16 cases, 7 пар на case, случайный порядок пар и baseline/candidate внутри пары,
31 measured repetitions + 10 warmups на process. `iterations=0`, одинаковый
runtime_suite и checksum у обоих вариантов. Процесс ограничен workers+1
разными physical CPUs; отдельные threads не привязаны. Частоты не фиксировались.
Во время timings сборки и sanitizer tests не запускались.

Baseline — снимок перед этой задачей: recursive inline overflow, деструктор без
admission/drain. Final — shared overflow с batch-drain и отдельной сменой
предпочтения источника на полный оборот periodic shard probe; graceful shutdown.
Pool construction и shutdown **не входят** в timings, измеряется стоимость
новых hot paths. Это exploratory comparison, не доказательство выигрыша на
всех CPUs/loads; LTO, 8+ workers и несколько external producers здесь не покрыты.

Время — медиана process p50, микросекунды. Δ — медиана **парных** отношений
candidate/baseline минус 1, поэтому она может отличаться от отношения двух
отдельных медиан. Плюс означает замедление.

| Scenario | Workers | Tasks | Batch | Before, us | Final, us | Δ paired |
|---|---:|---:|---:|---:|---:|---:|
| external_detached | 1 | 4096 | 0 | 310.4 | 394.0 | +26.1% |
| external_detached | 1 | 4096 | 16 | 190.5 | 192.4 | +0.7% |
| external_handles | 1 | 4096 | 0 | 434.6 | 476.8 | +7.6% |
| local_saturated | 1 | 4096 | 0 | 146.6 | 158.6 | +7.7% |
| local_saturated | 1 | 65536 | 0 | 1753.5 | 2545.2 | +45.2% |
| nested_spawn | 1 | 4096 | 0 | 320.5 | 325.7 | +1.2% |
| deep_dag | 1 | 4096 | 0 | 49.6 | 49.3 | -0.5% |
| graph_reuse | 1 | 4096 | 0 | 256.6 | 238.1 | -7.2% |
| external_detached | 4 | 4096 | 0 | 823.4 | 882.5 | +5.6% |
| external_detached | 4 | 4096 | 16 | 289.3 | 296.1 | +1.5% |
| external_handles | 4 | 4096 | 0 | 1030.9 | 1040.5 | +0.8% |
| local_saturated | 4 | 4096 | 0 | 452.4 | 441.6 | -2.4% |
| local_saturated | 4 | 65536 | 0 | 7206.0 | 6871.1 | -4.8% |
| nested_spawn | 4 | 4096 | 0 | 111.9 | 114.1 | +0.0% |
| deep_dag | 4 | 4096 | 0 | 113.6 | 108.7 | -4.1% |
| graph_reuse | 4 | 4096 | 0 | 978.1 | 912.3 | -6.9% |

## Что оплачиваем

На одном worker scalar external detached вырос примерно на **26%**: admission
теперь требует CAS reserve и release fetch_sub. Для группы из 16 стоимость
амортизируется, observed delta около +1%. Worker publication admission-счётчик
не трогает. Дальнейшая оптимизация должна сохранить race-free close/publication
barrier; простая проверка `closed()` вместо reservation его ломает.

Полное однопоточное saturation стало примерно на **45%** дороже старого inline
исполнения: теперь все children публикуются, не исполняются рекурсивно прямо при
submit. Новый путь сохраняет больше одновременно живых packets и выполняет
queue/wake/acquisition операции для overflow. Это не изолированный замер mutex.
Первый вариант с pop под отдельным lock на task показывал +79%; batch-drain
снизил потерю. На четырёх workers saturation в этом прогоне не ухудшился.

Packet prefix вырос на 8 B (32 B total), InitialTask тоже на 8 B (48 B total,
192 KiB / 4096 nodes, без diagnostics). Queue-node allocations не добавлены.
Вместо рекурсии queue может удерживать неограниченное количество worker tasks;
это устраняет stack overflow от submit, но не заменяет application backpressure.

Небольшие отрицательные Δ в таблице не считаются доказанным ускорением:
частоты/OS noise и изменившийся layout не изолированы. Главные стабильные потери
здесь — scalar external admission и однопоточный overflow.

## Артефакты

- `out/experiments/overflow-shutdown/sources/{baseline,candidate,batched-overflow,final}` — снимки исходников.
- `out/experiments/overflow-shutdown/build/` — соответствующие binaries/builds.
- [Final summary](../../out/experiments/overflow-shutdown/comparison-final/summary.json),
  [raw runs](../../out/experiments/overflow-shutdown/comparison-final/runs.jsonl),
  [manifest и hashes](../../out/experiments/overflow-shutdown/comparison-final/manifest.json).
- `comparison/` — первый scalar overflow pop; `comparison-batched/` — до отдельного periodic preference.
- `comparison-final/runner.py` и `measure.py` — воспроизведение измерений.
- `packet-layout.txt` — проверенные Clang layouts; `fairness-before.txt` — новый
  regression test падает на промежуточной версии выбора источника.

Все runtime sources/headers текущей реализации совпадают с `sources/final`.
Release 29/29; ASan/UBSan/LSan 29/29; TSan 7/7; diagnostics main harness passed.
