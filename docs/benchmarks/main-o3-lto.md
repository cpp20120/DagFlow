# Профиль bench/stress_harness.cpp: O3 и Full LTO

Замер 2026-09-30: AMD Ryzen 7 6800H (8 ядер / 16 потоков), Clang 22.1.8,
Linux, perf 7.2.7. Измерен **пользовательский `bench/stress_harness.cpp`** с шестью
нагрузками, на 1 и 4 workers. `bench/runtime_suite.cpp` здесь не используется.

[Открыть таблицу и все flamegraph/call graph](../../out/profiles/main-o3-lto/index.html) ·
[CSV со всеми счётчиками](../../out/profiles/main-o3-lto/summary.csv) ·
[Manifest и хеши исходников/бинарников](../../out/profiles/main-o3-lto/manifest.json).

## Условия

- Основные сборки: `-O3 -g -DNDEBUG`, static DagFlow + dynamic mimalloc,
  одинаковый lld; `DAGFLOW_LTO_MODE=none` против `full` (`-flto=full`).
  PGO и native CPU flags выключены. Это Full LTO, не ThinLTO.
- Зафиксирован снимок исходников в `source/`. В `main.cpp` добавлен только
  выбор `--scenario` / `--workers`; тела нагрузок, размеры, 2 warmup + 7 timed
  repetitions не менялись. Без аргументов исполняются все нагрузки на 1 и 4 workers.
- Одинаковая affinity `0–15`, `pin_threads=false`. Частота CPU и внешний load
  не фиксировались; worker/producers могут мигрировать между ядрами/SMT.
- Время: пять пар независимых процессов для каждой комбинации, порядок
  сценариев и сборок перемешан. Это 120 запусков **без perf**.
- Счётчики: три отдельных `perf stat` запуска на комбинацию, 72 процесса.
  Все семь событий доступны, running time 100%: multiplexing отсутствовал.
- Стеки: 24 отдельных Release/DWARF профиля и 24 диагностических FP-профиля.
  Эти запуски не входят в таблицу времени и счётчиков.

Время внутри `main` исключает создание Pool и `warm_pool`. `perf stat` и
`perf record` охватывают **весь отдельный процесс**: startup, прогрев 8192 задач,
два warmup и семь измеряемых повторов (либо 64 burst для idle-burst), cleanup.
Время создания/join внешних producer threads входит в сами contention/chaos
нагрузки. Счётчики нельзя делить на задачи одного timed repetition.

## Время

Здесь медиана пяти process-median. Изменение — медиана пяти парных
`(LTO / O3 - 1) × 100%`, а не отношение двух агрегированных медиан.
Поэтому процент не обязательно совпадает с отношением времён в соседних колонках.
Минус означает меньшее время. Диапазон — наблюдавшиеся изменения пяти пар,
не доверительный интервал.

| Нагрузка | Workers | O3, мс | O3 + LTO, мс | Парное изменение | Диапазон пар |
| --- | ---: | ---: | ---: | ---: | ---: |
| external-contention | 1 | 8.9660 | 8.5720 | -4.2% | -36.6% … +0.0% |
| hot-shard-skew | 1 | 27.5510 | 27.4050 | -0.7% | -2.7% … +8.3% |
| local-overflow | 1 | 3.3330 | 2.8460 | -13.7% | -17.9% … -8.4% |
| nested-helping | 1 | 6.2170 | 5.5300 | -6.1% | -11.1% … -1.1% |
| mixed-chaos | 1 | 17.1640 | 16.7220 | -1.2% | -12.8% … +0.3% |
| idle-burst | 1 | 0.0367 | 0.0434 | +19.7% | -35.7% … +30.3% |
| external-contention | 4 | 4.6030 | 4.4760 | +1.9% | -26.5% … +3.4% |
| hot-shard-skew | 4 | 4.8860 | 5.5080 | -2.3% | -7.8% … +12.7% |
| local-overflow | 4 | 17.5720 | 14.0740 | -18.9% | -50.4% … -17.6% |
| nested-helping | 4 | 3.8020 | 2.5960 | -2.0% | -31.7% … -1.3% |
| mixed-chaos | 4 | 5.5390 | 5.5540 | +4.1% | -4.9% … +11.7% |
| idle-burst | 4 | 0.0531 | 0.0504 | -5.0% | -24.3% … +5.0% |

`idle-burst` — p50 времени от начала публикации 64 задач до `wait_idle()`;
это **не latency старта одной задачи**. Sleep 10 мс не входит в интервал.
Столбцы выше для него также в миллисекундах; отдельно p50/p99 в микросекундах:

| Workers | O3 p50, мкс | LTO p50, мкс | O3 p99, мкс | LTO p99, мкс |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 36.75 | 43.38 | 140.83 | 204.93 |
| 4 | 53.08 | 50.43 | 239.44 | 237.69 |

## Все запрошенные счётчики

Медианы трёх **whole-process** запусков. Hardware events запрошены с `:u`;
perf также пометил software event как `page-faults:u`. M = 1,000,000 событий.
Generic `cache-references` / `cache-misses` здесь не обозначают специально L1;
интерпретация соответствует PMU данного CPU.

| Нагрузка | W | Сборка | Cycles, M | Instructions, M | Branches, M | Branch misses, M | Cache refs, M | Cache misses, M | Page faults |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| external-contention | 1 | o3 | 770.720 | 650.161 | 133.221 | 0.656 | 18.592 | 6.083 | 233 |
| external-contention | 1 | o3-lto | 764.013 | 547.348 | 104.721 | 0.494 | 18.682 | 6.090 | 235 |
| hot-shard-skew | 1 | o3 | 635.839 | 804.498 | 94.137 | 0.306 | 7.088 | 0.697 | 227 |
| hot-shard-skew | 1 | o3-lto | 623.200 | 756.529 | 80.806 | 0.286 | 7.142 | 0.675 | 226 |
| local-overflow | 1 | o3 | 68.737 | 213.628 | 41.399 | 0.059 | 1.553 | 0.152 | 226 |
| local-overflow | 1 | o3-lto | 64.115 | 178.286 | 32.348 | 0.059 | 1.658 | 0.143 | 227 |
| nested-helping | 1 | o3 | 133.526 | 283.281 | 49.016 | 0.054 | 2.028 | 0.243 | 228 |
| nested-helping | 1 | o3-lto | 126.327 | 252.332 | 40.886 | 0.052 | 1.980 | 0.254 | 226 |
| mixed-chaos | 1 | o3 | 547.721 | 705.405 | 113.379 | 0.549 | 13.060 | 3.202 | 234 |
| mixed-chaos | 1 | o3-lto | 532.502 | 633.563 | 93.220 | 0.510 | 13.084 | 3.116 | 235 |
| idle-burst | 1 | o3 | 16.345 | 13.948 | 2.472 | 0.106 | 1.393 | 0.587 | 226 |
| idle-burst | 1 | o3-lto | 15.852 | 12.856 | 2.182 | 0.100 | 1.334 | 0.530 | 226 |
| external-contention | 4 | o3 | 877.948 | 623.025 | 125.451 | 0.568 | 20.339 | 6.240 | 321 |
| external-contention | 4 | o3-lto | 840.472 | 530.354 | 99.500 | 0.518 | 20.429 | 6.270 | 319 |
| hot-shard-skew | 4 | o3 | 717.240 | 806.673 | 93.704 | 0.275 | 8.134 | 1.918 | 234 |
| hot-shard-skew | 4 | o3-lto | 701.755 | 758.413 | 80.389 | 0.278 | 8.123 | 1.808 | 234 |
| local-overflow | 4 | o3 | 773.584 | 444.977 | 79.038 | 1.064 | 19.060 | 8.376 | 235 |
| local-overflow | 4 | o3-lto | 744.173 | 395.676 | 67.264 | 0.937 | 18.490 | 8.277 | 234 |
| nested-helping | 4 | o3 | 227.142 | 278.700 | 46.540 | 0.194 | 4.662 | 1.638 | 237 |
| nested-helping | 4 | o3-lto | 232.489 | 252.843 | 39.148 | 0.242 | 5.213 | 1.850 | 236 |
| mixed-chaos | 4 | o3 | 733.807 | 714.786 | 113.983 | 0.639 | 15.743 | 4.549 | 485 |
| mixed-chaos | 4 | o3-lto | 705.482 | 645.229 | 94.675 | 0.605 | 15.371 | 4.574 | 476 |
| idle-burst | 4 | o3 | 52.411 | 34.865 | 5.685 | 0.322 | 4.772 | 1.677 | 234 |
| idle-burst | 4 | o3-lto | 46.711 | 33.141 | 5.298 | 0.286 | 4.424 | 1.391 | 235 |

IPC, branch-miss rate, cache-miss rate и диапазоны времени также сохранены в
[summary.csv](../../out/profiles/main-o3-lto/summary.csv). Они вычислены по парным значениям внутри
каждого stat-процесса, затем взята медиана.

## Что видно

1. **Local overflow — самый устойчивый выигрыш LTO**: −13.7% на одном worker и
   −18.9% на четырёх, все пять пар в обоих случаях отрицательные. Instructions
   уменьшились примерно на 16.5% / 11.1%. Но четыре worker всё ещё заметно медленнее
   одного в этой нагрузке из очень коротких задач.
2. **Nested helping тоже быстрее во всех пяти парах**: медиана изменений −6.1%
   на одном worker и −2.0% на четырёх. Разброс четырёх workers большой;
   нельзя выдавать отношение агрегированных времён за стабильное ускорение.
3. **Нет общего большого выигрыша для contention, chaos и idle**. Парные
   изменения меняют знак. LTO уменьшает число инструкций, но меньший instruction
   count сам по себе не гарантирует пропорционального сокращения времени.
4. **Page faults почти не отличаются** между сборками: сотни за весь процесс.
   В этой паре O3/LTO нет эффекта allocator routing из предыдущего эксперимента.

По Release sampling и диагностическим FP-стекам видны кандидаты для следующего
разбора, а не доказанные причины всех различий времени:

- `external-contention`: publication через MPMC, CAS/load и shard selection;
  после LTO часть этих операций оказывается внутри `Pool::enqueue`. Сравнение
  процентов только по имени функции было бы некорректно из-за inlining.
- `hot-shard-skew`: большая часть sampled work — сам `work()` (примерно 67–80%
  в FP-профилях). Здесь есть существенная полезная вычислительная нагрузка.
- `local-overflow`, 4 workers: `Scheduler::steal` занимает около 31–32% FP-samples,
  плюс заметны mimalloc/free и атомики. В основной stat-сборке IPC около 0.58 / 0.53.
  Это повод разбирать стоимость stealing и передачи task packets между потоками;
  конкретную bouncing cache line эти счётчики не определяют.
- `mixed-chaos`: в FP-профилях видны `work`, publication и wake/accounting paths;
  это смесь механизмов, а не изолированная цена одного wake.

## Flamegraph и call graph

Для каждой строки в [HTML-индексе](../../out/profiles/main-o3-lto/index.html) есть:

- Release flamegraph / call graph / hotspots **точного бинарника**, использованного
  в основной таблице: `profiles/<build>-<workers>-<scenario>/`.
- FP flamegraph / call graph диагностической сборки из того же снимка:
  `profiles-fp/<build>-<workers>-<scenario>/`.
- Исходные `perf.data` и JSON дерева рядом с SVG.

Release использует DWARF со stack dump 16 KiB. Некоторые нагрузки имеют много
samples без восстановленного callchain; leaf symbol сохраняется, но считать
такие профили полными цепочками нельзя. Диагностическая сборка добавляет
`-fno-omit-frame-pointer -mno-omit-leaf-frame-pointer` и использует `--call-graph fp`.
В ней 615–1660 samples на случай, **нет samples с полностью отсутствующим
callchain**, и perf сообщает ноль lost samples. Это не гарантия полноты каждого
стека: системные библиотеки могут не сохранять frame pointers, хвостовые вызовы
и inlining меняют видимую цепочку. Её время/счётчики не подставлены в основную таблицу.

Ширина SVG равна сумме `sample.period` события `cycles:u`, а не числу вызовов
или wall time. События записаны с фиксированным периодом, выбранным из stat cycles
для примерно 1500 samples. SVG работают офлайн, поддерживают zoom/search.
Неизвестные адреса и отсутствующие стеки сохранены явно. Это существенно для
idle-burst: значительная часть leaf weight остаётся unresolved, профиль также
включает startup/warmup. По нему нельзя точно разложить задержку пробуждения.
CPU flamegraph вообще не показывает время, когда поток не исполнялся на CPU.

Первая пробная frequency-based запись сохранена в `profiles-frequency/`, но не
использована для выводов: короткие случаи давали всего десятки samples.
[Sampling coverage Release](../../out/profiles/main-o3-lto/sampling-summary.json) и
[FP](../../out/profiles/main-o3-lto/sampling-summary-fp.json) содержат число samples и отсутствующих callchains.

## Повторить

```sh
python3 tools/profiling/profile_main.py --output out/profiles/main-o3-lto-repeat
python3 tools/profiling/profile_main_stacks.py out/profiles/main-o3-lto-repeat
python3 tools/profiling/render_main_profile.py out/profiles/main-o3-lto-repeat
```

Нужны Linux perf с Python/DWARF support и доступом к userspace PMU,
Clang/lld, CMake/Ninja и mimalloc. Используется affinity вызывающего процесса.
Выходной каталог должен быть новым; `--resume` продолжает основной сбор по
сохранённым успешным командам после проверки snapshot/binary hashes.

[Команды основных измерений](../../out/profiles/main-o3-lto/commands.json),
[команды FP](../../out/profiles/main-o3-lto/fp-commands.json),
[сырые времена](../../out/profiles/main-o3-lto/timings.jsonl),
[сырые счётчики](../../out/profiles/main-o3-lto/counters.jsonl), build logs и compile_commands.json
сохранены в каталоге замера. `main.original.cpp` — main до добавления CLI.
`source/` — точный снимок измеренных исходников. Нагрузки используют volatile sink,
но не проверяют полезный результат как correctness test; успешный exit не заменяет тесты runtime.

Каталог `out/` исключён из Git. Сохраните его вместе с бинарниками, если нужны
повторная символизация `perf.data` и эти же исходники после будущих изменений.
