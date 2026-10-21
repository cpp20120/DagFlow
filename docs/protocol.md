# Протокол DagFlow: единый реестр контрактов runtime

Этот файл — основное место для действующих контрактов DagFlow: требований к
вызывающему коду, внутренних инвариантов, порядка публикации и завершения,
гарантий при ошибках и известных ограничений. Сверен с исходниками текущего
рабочего дерева 2026-10-19, включая изменения worker wait и detached errors.
Реестр описывает реализацию, а не проект будущего runtime

Тематические документы оставлены для объяснений, примеров и измерений;
изменения контрактов следует сначала отражать здесь. Каталоги
`dagflow-lifetime-invariants-current*.md` исторические: их allocator, completion,
scope и shutdown модели нельзя применять к текущему коду.

Каждая строка содержит стабильный ID, сам контракт, обеспечивающую его
реализацию и проверку. Ссылки на тесты означают существующие сценарии, а не
заявление об их запуске или доказательство всех межпоточных расписаний.
**Предусловие** означает обязанность вызывающего кода; **пробел** — отсутствие
проверки или защиты, а не обещание runtime. `detail/` — внутренняя реализация,
даже когда соответствующие заголовки устанавливаются вместе с библиотекой.

## Навигация

- [Модель владения и завершения](#model)
- [Pool: создание, admission, shutdown](#pool)
- [Packet: публикация, custody, cleanup](#packet)
- [Completion, Handle, combine](#completion)
- [Ожидание и progress](#wait)
- [Scheduler и очереди](#scheduler)
- [Parking и уведомления](#parking)
- [Physical idle accounting](#idle)
- [TaskGraph: definition, run, lanes, tokens](#graph)
- [TaskScope](#scope)
- [GraphScope и identity](#graph-scope)
- [Range algorithms и detached batch](#ranges)
- [Память, callable wrappers, контейнеры](#memory)
- [Порядок памяти и точки принятия решений](#ordering)
- [Непроверенные границы и ограничения](#limits)
- [Как проверять и обновлять реестр](#validation)

<a id="model"></a>
## Модель владения и завершения

| Роль | Смысл | Чего не даёт |
| --- | --- | --- |
| Storage ownership | Право уничтожить объект и вернуть его allocation backend. Обычно `OwnedObject` / `OwnedArray`; graph initial packet принадлежит массиву графа. | Само по себе не разрешает публиковать или повторно исполнять packet. |
| Packet custody | Исключительное право передавать или исполнять конкретный packet: producer → queue → transfer → executor. | Queue pointer не становится вторым владельцем allocation. |
| Liveness credit | Move-only обязательство `CompletionCredit`; живой credit разрешает `fork()` и препятствует terminal completion. | Не означает, что только один packet или token представляет операцию. |
| Observation | `Handle` держит storage reference на completion и позволяет ждать результат. | Не держит graph definition, range, scope или Pool; не разрешает создавать credits. |

Порядок обычной задачи:

```text
construct packet → reserve admission → count publication → publish queue
→ acquire → invoke / record error → move credit out → destroy packet/captures
→ finish credit [last: destroy operation payload → ready → deliver dependents]
→ physical retirement
```

Для graph-owned initial packet `destroy` означает окончание borrow на slot,
а не уничтожение элемента массива. После release executor не читает slot.
`ready()` графа разрешает повторное использование его storage, хотя pool epilogue
ещё может завершаться. `wait_idle()` ждёт этот более поздний physical retirement.

| ID | Контракт | Реализация | Проверка |
| --- | --- | --- | --- |
| OWN-01 | Storage, custody, credits и observation учитываются независимо. Удаление всех Handles не отменяет работу; сохранение Handle не продлевает жизнь пользовательских captures после completion. | [handle.hpp][handle], `CompletionCredit`, `CompletionState`; [execute_task][pool-cpp] | [ownership_tests.cpp][ownership]: `credit_transfer_and_payload`, `dropped_observers_and_rollback` |
| OWN-02 | Borrow на state/range/definition допустим только пока живо защищающее obligation. После последнего credit task-side код, включая destructor capture, больше не обращается к этому state. | [TaskGraph::execute][graph-cpp], [TaskScope::publish][scope-hpp], [Pool::for_each][pool-hpp] | [ownership][ownership], [reentrant_capture_cleanup_test.cpp][reentry], [graph_cancel_continuation_reuse_race_test.cpp][graph-race] |
| OWN-03 | Пользовательские references/pointers не становятся owned captures. Их storage и требуемая синхронизация принадлежат вызывающему коду и должны пережить все принятые обращения, включая rollback и shutdown. | [Pool API][pool-hpp], [TaskScope API][scope-hpp], [GraphScope API][gs] | [range_publication_rollback_lifetime_test.cpp][range-rollback], [graph_scope_unwind_cleanup_test.cpp][gs-unwind]; произвольные dangling captures runtime не диагностирует |
| OWN-04 | Completion boundary задаётся API: scalar submit покрывает свой packet/captures, но не автоматически новые Pool submissions из него; graph — только собственные lane drivers; range — свои chunks/splits; TaskScope — принятые scope tasks/descendants. Независимые submissions требуют своих Handles/combine либо physical idle/shutdown. | [submit_impl, range helpers][pool-hpp], [TaskGraph::publish][graph-cpp], [TaskScope::publish][scope-hpp] | [ownership][ownership], [pool queues][pool-queue]: `completion_edges`; boundary определяется передаваемыми credits |

<a id="pool"></a>
## Pool: создание, admission, shutdown

| ID | Контракт | Реализация | Проверка |
| --- | --- | --- | --- |
| POOL-01 | Scheduler topology, parking и accounting полностью создаются до workers и живут до их join. Ошибка запуска потока останавливает и join'ит уже запущенные потоки до раскрутки members. | [Pool::Pool, ~Pool][pool-cpp] | [runtime_failure_tests.cpp][failure]: `pool_construction_failures`; сбой самого OS thread-start отдельно не инъецируется |
| POOL-02 | `threads=0` даёт минимум одного worker; `shards=0` — по shard на worker. Пустой `worker_shards` выбирает balanced contiguous mapping, непустой должен иметь один допустимый shard ID на worker; иначе `invalid_argument`. `central_batch=0` нормализуется минимум к одному. | [Config][pool-hpp], [Scheduler::Scheduler][sched-cpp] | [scheduler_topology_tests.cpp][topology]: `membership_and_stealing`, `pool_domains` |
| POOL-03 | Topology неизменна после создания. `pin_threads` — best effort OS placement; неудача pin не отменяет pool construction и не меняет identity worker. | [Pool::Pool, pin_to_cpu][pool-cpp], [Scheduler][sched-cpp] | [topology][topology]; отказ OS affinity отдельно не инъецируется |
| POOL-04 | Внешний publisher атомарно резервирует открытый admission до accounting. Проверку closed нельзя отделять от резервирования. Успешная reservation сохраняется до dispatch и wake, даже если `close()` уже произошёл или ingress полон. | [PublicationGuard, enter_publication, leave_publication][pool-cpp] | [pool_lifecycle_tests.cpp][lifecycle]: `close_races_producers`, `shutdown_during_backpressure` |
| POOL-05 | `close()` необратим, idempotent и не ждёт. Он закрывает только внешний вход; `closed()` не означает idle/join. Поздняя внешняя публикация бросает `logic_error`; callable к этому моменту уже мог быть сконструирован/перемещён. | [Pool::close, enqueue_prepared][pool-cpp] | [lifecycle][lifecycle]: `close_and_descendants`, `close_between_batch_groups` |
| POOL-06 | Задача своего Pool может публиковать descendants после close, в том числе из capture/payload cleanup; её physical retirement покрывает публикацию детей. Worker другого Pool — внешний publisher для целевого Pool. | [PublicationGuard, dispatch, execute_task][pool-cpp] | [lifecycle][lifecycle]: `close_and_descendants`, `cross_pool_is_external`; [idle_accounting_tests.cpp][idle-test]: `destructor_publication` |
| POOL-07 | Admission единица — packet либо подготовленная внешняя batch-группа ≤64. Range/graph/scope целиком не резервируются как транзакция; close может разделить их на принятый prefix и отказ. | [enqueue_batch_prepared][pool-cpp], [range helpers][pool-hpp] | [lifecycle][lifecycle]: `close_between_batch_groups`, `graph_and_scope_after_close`; [range rollback][range-rollback] |
| POOL-08 | `shutdown()` и destructor выполняют close → ожидание publishers → physical idle → stop → wake all → join. Stop до drain запрещён: workers ещё нужны backpressure и descendants. Shutdown не отменяет задачи и не перебрасывает их ошибки. | [Pool::shutdown, ~Pool][pool-cpp] | [lifecycle][lifecycle]: `destructor_drains`, `shutdown_during_backpressure`, `shutdown_waits_for_capture_destruction` |
| POOL-09 | Повторные/конкурентные внешние shutdown сериализованы mutex. Собственный worker получает `logic_error` до этого mutex; уничтожение Pool на собственном worker недопустимо и завершает процесс через noexcept destructor. | [Pool::shutdown][pool-cpp] | [lifecycle][lifecycle]: `close_and_descendants`; self-destruction — предусловие |
| POOL-10 | Внешние callers должны закончить доступ до уничтожения Pool storage; Pool живёт до уничтожения связанных graph/scope. Shutdown не закрывает произвольные внешние credits, например sentinel открытого TaskScope. Пользователь обязан завершить callback: timeout/cancellation принудительно его не прерывают. | [Pool API][pool-hpp], [TaskScope::State][scope-cpp] | [lifecycle][lifecycle], [task_scope_tests.cpp][scope-test]; lifetime/deadlock пользователя автоматически не проверяются |

<a id="packet"></a>
## Packet: публикация, custody, cleanup

| ID | Контракт | Реализация | Проверка |
| --- | --- | --- | --- |
| PKT-01 | Packet полностью сконструирован до accounting/queue publication: ops, priority, callable, credit. Concrete `ScheduledTaskModel<F>` owns callable; lvalue копируется, rvalue перемещается. Сбой конструктора освобождает allocation и непереданные obligations. | [make_scheduled_task, ScheduledTaskModel][pool-hpp] | [ownership][ownership]: `lvalue_callable_ownership`, `spilled_callable_ownership`; [failure][failure] |
| PKT-02 | Каждый живой packet имеет одного custody holder. До успешного push за него отвечает publisher; после успешного push publisher больше не читает packet. Неуспешный push сохраняет custody. Acquired pointer принадлежит одному executor/transfer. Старые bits в ring slot не являются ownership. | [dispatch][pool-cpp], [queues][deque], [ring_mpmc][ring] | [queue_tests.cpp][queue-test], [pool_queue_tests.cpp][pool-queue], [queues_fuzz.cpp][queues-fuzz] |
| PKT-03 | До accounting commit возможен обычный rollback RAII. После commit dispatch не должен терять packet или добавлять throwing allocation. Worker overflow использует link самого packet; внешний publisher сохраняет packet при retry. | [enqueue_prepared, dispatch][pool-cpp], [OverflowQueue][sched-cpp] | [accounting_allocation_tests.cpp][account-alloc], [lifecycle][lifecycle]: `overflow_does_not_recurse`, `shutdown_during_backpressure` |
| PKT-04 | Epilogue строго: invoke → сохранить error → вынести credit → destroy callable/packet (или release graph slot) → finish credit → retire accounting. После destroy/release pointer не читается. | [Pool::execute_task][pool-cpp], [InitialTask::release][graph-cpp] | [ownership][ownership], [reentry][reentry], [exception_payload_last_release_test.cpp][exception-release] |
| PKT-05 | Target/capture destructors и erased destroy callbacks не бросают. Cleanup может повторно войти в runtime, поэтому credit должен покрывать весь cleanup, а не только возврат callable. Собственный исходный callable/argument temporary caller'а не входит в completion задачи. | [ScheduledTaskModel][pool-hpp], [ScopedTask][scope-hpp], [CompletionState::retire][pool-cpp] | [reentry][reentry], [cancelled_capture_reentry_test.cpp][cancel-reentry], compile-time `static_assert` |
| PKT-06 | Ошибка tracked task попадает в его completion. Detached error при ненулевом `detached_error_handler` передаётся ему на исполняющем worker до cleanup; при null отбрасывается. Handler обязан быть noexcept и thread-safe. | [Config][pool-hpp], [execute_task][pool-cpp] | [worker_wait_tests.cpp][worker-wait]: `detached_reporting`; [batch_submit_tests.cpp][batch-test]: `partial_failure_and_task_exceptions` |

<a id="completion"></a>
## Completion, Handle, combine

| ID | Контракт | Реализация | Проверка |
| --- | --- | --- | --- |
| CMP-01 | Каждый credit гасится ровно один раз. Move передаёт obligation; `finish()` очищает pointer и безопасен для пустого/moved-from credit. `fork()` допустим только из живого credit; terminal zero нельзя открыть снова. | [CompletionCredit][handle], [fork, finish, retire][pool-cpp] | [ownership][ownership]: `credit_transfer_and_payload`; [completion_last_credit_race_test.cpp][last-credit], [credits_fuzz.cpp][credits-fuzz] |
| CMP-02 | `references` держит allocation, `credits` держит незавершённость. Каждый live credit несёт storage reference; Handles добавляют только references. Финальный reference может исчезнуть лишь после ready, удаления payload/dependents/waiters. | [CompletionState][handle], [retain, release, ~CompletionState][pool-cpp] | [ownership][ownership]: `dropped_observers_and_rollback`; [last credit race][last-credit]; destructor assertions |
| CMP-03 | Последний credit уничтожает operation payload до release-store `ready=true`. Ready монотонен; acquire-наблюдение видит предшествующую работу и cleanup. Ready не является доказательством живости payload и не ждёт всех pool epilogues. | [CompletionState::retire][pool-cpp], [Handle::ready][handle] | [ownership][ownership]: `range_payload_released_before_result`; [exception release][exception-release] |
| CMP-04 | Финальный credit сохраняет storage reference во время payload destruction, notifications и доставки dependent credits, даже если исчезли все Handles. Terminal propagation использует итеративный worklist; глубокая цепочка combine не вызывает рекурсивный retire на каждое ребро. | [CompletionState::retire][pool-cpp] | [pool queues][pool-queue]: `completion_edges`; [completion_reentrant_registration_test.cpp][registration-reentry], [joins_fuzz.cpp][joins-fuzz] |
| CMP-05 | Регистрация dependent и terminal closure согласованы gate + mutex. Ранний dependent забирается finisher'ом; поздний немедленно получает уже сохранённую ошибку и гасит credit. Ни потеря, ни двойное завершение ребра недопустимы. Gate закрывается после ready-store. | [add_dependent, retire][pool-cpp] | [ownership][ownership]: `dependent_registration_races_completion`; [registration reentry][registration-reentry] |
| CMP-06 | Первая записанная ошибка сохраняется под mutex; `fail()` не гасит credit. Ошибка распространяется в dependents до погашения их credit. При конкурентных разных источниках выбор первой ошибки не детерминирован порядком входного массива. | [set_error, get_error, retire][pool-cpp] | [multi_source_completion_failure_race_test.cpp][multi-error], [ownership][ownership] |
| CMP-07 | `combine` создаёт только completion edges, не executable tasks, не использует `SubmitOptions` и не требует открытого admission. Registration sentinel защищает всю установку edges. Empty input возвращает invalid-ready Handle; invalid sources пропускаются, повторные valid sources дают отдельные obligations. | [Pool::combine][pool-cpp] | [pool queues][pool-queue]: `completion_edges`; [joins fuzz][joins-fuzz] |
| CMP-08 | При allocation failure посередине combine уже зарегистрированные edges сохраняют свои obligations, локальный sentinel и непереданные credits закрываются RAII. Сбой не отменяет sources и не ждёт их; промежуточный result не возвращается. | [combine, add_dependent][pool-cpp] | [combine_partial_registration_oom_race_test.cpp][combine-oom] |
| CMP-09 | Пустой Handle invalid, но ready; wait и rethrow для него no-op. Copy/move сохраняют observation semantics. Готовый Handle может пережить граф, scope и Pool и сохраняет результат. `rethrow_if_failed()` не ждёт: для окончательного результата сначала дождаться ready. | [Handle][handle], [Pool::wait_and_rethrow][pool-hpp] | [api_convenience_tests.cpp][api-test]: `wait_and_errors`; [pool queues][pool-queue]: `handles_outlive_pool`; [ownership][ownership]: `graph_repeated_wide_runs_and_old_results` |
| CMP-10 | Разные Handles могут наблюдать один state параллельно. Один и тот же изменяемый Handle/credit нельзя несинхронно move/reset из разных потоков. `Pool::wait` копирует Handle перед helping, защищая state от реентерабельной замены исходного Handle на том же потоке. | [Handle][handle], [Pool::wait_impl][pool-cpp] | [worker_wait_tests.cpp][worker-wait]: `shared_completion`; межпоточная mutation wrapper — предусловие C++ |

<a id="wait"></a>
## Ожидание и progress

| ID | Контракт | Реализация | Проверка |
| --- | --- | --- | --- |
| WAIT-01 | `Pool::wait` completion-only: не перебрасывает task failure. Внешний caller ждёт atomic ready; worker этого Pool с policy Help (по умолчанию) исполняет доступные задачи, затем паркуется. `wait_and_rethrow` отдельно читает результат после wait. | [wait_impl][pool-cpp], [wait_and_rethrow][pool-hpp] | [api-test][api-test]: `wait_and_errors`; [pool queues][pool-queue]: `worker_overflow`, `stolen_batch_dependencies` |
| WAIT-02 | Helping — точка реентерабельного вызова пользовательского кода на native stack. Нельзя держать ресурс, без которого helped work не завершится, или полагаться на отсутствие вложенных callbacks. `worker_wait_depth()` — диагностика, не recursion limit. | [wait_impl, tls_wait_depth_][pool-cpp] | [worker-wait][worker-wait]: `depth_and_rejection`; [nested_helping_context_restore_test.cpp][nested] |
| WAIT-03 | `WorkerWaitPolicy::Reject` проверяется по Pool текущего worker, даже при вызове wait другого Pool; unfinished wait бросает `logic_error`. Ready/empty handles разрешены. Range publication rollback вызывает `wait_impl(..., false)`, потому что обязан завершить принятые borrowed accesses. | [wait_impl][pool-cpp], [for_each, for_each_ws][pool-hpp] | [worker-wait][worker-wait]: `depth_and_rejection`; [range rollback][range-rollback] |
| WAIT-04 | Cooperative helping действует только если `tls_pool_ == this`. Worker, вызывающий wait другого Pool при Help, блокируется на ready и не помогает своему Pool. Циклические ожидания, блокирующий I/O, mutex/CV и сторонние futures не компенсируются дополнительными workers. | [Pool::wait_impl, worker_loop][pool-cpp] | [pool queues][pool-queue]: `cross_pool_submission`; отсутствие циклов и достаточный progress — предусловия, общей deadlock detection нет |
| WAIT-05 | После исчерпания work каждый suspended wait frame регистрирует отдельную stack запись в CompletionState. Registration/removal/notification делят mutex/gate; копия Handle защищает state. Finisher не может обратиться к уничтоженной записи/ParkingLot. | [CompletionWaiter, wait_impl, retire][pool-cpp] | [worker-wait][worker-wait]: `shared_completion`; [wake_protocol_fuzz.cpp][wake-fuzz]: worker-wait/nested-worker-wait cases |
| WAIT-06 | Worker регистрируется до prepare/final scan; ready до prepare ловится повторной проверкой, ready после проверки сигналит epoch. Новая работа использует обычный queue wake. Вложенное helping не перезаписывает регистрацию внешнего frame. | [Pool::wait_impl][pool-cpp], [ParkingLot::wake_worker][parking-cpp] | [wake protocol fuzz][wake-fuzz], [nested][nested] |
| WAIT-07 | Собственный worker не вызывает `wait_idle`/shutdown: иначе ждал бы собственный physical retirement. Scoped/graph destruction с незавершённой работой на worker Reject также недопустим: отказ обязательного wait может привести к terminate в destructor. | [wait_idle, shutdown][pool-cpp], [~TaskGraph][graph-cpp], [~TaskScope][scope-cpp] | [lifecycle][lifecycle], [worker-wait][worker-wait]; использование destructors при Reject — предусловие |

<a id="scheduler"></a>
## Scheduler и очереди

| ID | Контракт | Реализация | Проверка |
| --- | --- | --- | --- |
| SCH-01 | Affinity — worker hint, нормализованный modulo worker count и отображённый в home shard. Это не гарантия потока исполнения, порядка или эксклюзивного доступа. Shards могут быть пустыми; remote workers всё равно обслуживают их shared work. | [Scheduler::select_shard, try_acquire][sched-cpp] | [topology][topology]: `membership_and_stealing`, `pool_domains`; [lifecycle][lifecycle]: `overflow_visible_to_other_worker` |
| SCH-02 | Собственный worker с Spawn предпочитает local deque, затем home ingress. Enqueue или другой целевой shard направляет в ingress; Enqueue без affinity выбирает shard round-robin. При заполнении worker публикует shared overflow, не исполняет callable внутри submit и не ждёт capacity. | [Pool::dispatch][pool-cpp], [try_submit_local][sched-cpp] | [pool queues][pool-queue]: `worker_overflow`; [lifecycle][lifecycle]: `overflow_does_not_recurse` |
| SCH-03 | Внешний caller публикует только через bounded ingress и ждёт места с retry/wake/backoff, сохраняя custody и admission. Он не помогает исполнением задач. После успешного submit задача не остаётся в скрытом producer buffer; возврат не означает completion. | [dispatch, backpressure_pause][pool-cpp] | [pool queues][pool-queue]: `external_backpressure`; [batch tests][batch-test]: `backpressure` |
| SCH-04 | Overflow — FIFO links под mutex, отдельно на shard/priority; atomic nonempty только probe. После accounting commit публикация не аллоцирует. Queue не lock-free и не ограничивает общее число задач/память. Links отсоединяются до передачи packet, после push к нему не обращаются. | [OverflowQueue::push/pop][sched-cpp] | [lifecycle][lifecycle], [topology][topology]: `overflow_batch_visible` |
| SCH-05 | Drain/steal сначала делают все siblings доступными в local deque, затем возвращают первый packet для user code. Private batch нельзя скрывать от nested helper/thief. Batch ограничен доступной owner capacity; невозможный local push означает нарушение инварианта и terminate. | [drain, steal, OverflowQueue::pop][sched-cpp] | [pool queues][pool-queue]: `stolen_batch_dependencies`; [topology][topology]: `central_batch_visible`, `overflow_batch_visible` |
| SCH-06 | Shared acquisition/steal, даже одного packet, перед user code передаёт wake дальше: final scan другого worker мог пропустить transfer. Собственная parking registration отменяется до relay. Обычный local pop не создаёт нового relay. | [try_acquire][sched-cpp], [Pool::try_help_one][pool-cpp] | [topology][topology]: `acquisition_publication`, `transferred_batch_wakes_waiter`, `batch_recruits_sleeping_workers`; [wake fuzz][wake-fuzz] |
| SCH-07 | Периодический shared probe обслуживает ingress/overflow при непрерывном local work. Shard cursor ротируется; source preference меняется за полный оборот shards и независима от drain preference. Priority предпочтительна внутри tier, но не задаёт глобальный порядок или bounded latency. | [try_acquire, poll_external, pop_shared][sched-cpp] | [scheduler_fairness_tests.cpp][fairness], [topology][topology]: `shared_sources_receive_service` |
| QUE-01 | Chase–Lev имеет одного owner для push/pop/free_capacity; остальные только steal. Индексы и успешная arbitration определяют membership. Неуспешный steal может означать contention; output при неуспехе не меняется. | [chase_lev_deque][deque] | [queue tests][queue-test]: `deque_boundaries`, `deque_last_element`, `deque_wraparound_contention` |
| QUE-02 | Last-item owner/thief race решает CAS `top`. Bottom stores release; speculative slots атомарны; thief не разыменовывает pointer до успешного steal. Fence и повторное наблюдение bottom в arbitration нельзя убирать на основании обычного stress pass. | [try_pop, try_steal][deque] | [queue tests][queue-test], [queues fuzz][queues-fuzz]; memory-order review обязателен |
| QUE-03 | Capacity обеих bounded очередей — степень двух ≥2; собственное storage операций не аллоцируется, ожидания места нет; пользовательские операции generic T могут аллоцировать. Chase–Lev требует trivially-copyable T, lock-free atomic T/ptrdiff_t и capacity ≤PTRDIFF_MAX. | [deque][deque], [ring][ring] | Compile-time assertions; [queue_allocation_tests.cpp][queue-alloc] |
| QUE-04 | Ring MPMC резервирует отдельный slot CAS, затем публикует value release-store sequence; consumer читает sequence acquire и release-публикует reuse. После reservation операция обязана закончить протокол: move assignment noexcept, copy overload только для noexcept copy assignment. | [ring_mpmc::push_impl, try_pop][ring] | [queue tests][queue-test]: `ring_contention`, `ring_boundaries`; [container_tests.cpp][container-test]: `queue_move_ownership`; static assertions |
| QUE-05 | Stalled ring producer может блокировать доступ к head; `try_pop=false` не обязательно означает отсутствие принятых packets. `empty()` любой очереди — snapshot, не completion. Уничтожение очередей разрешено только после прекращения всех accesses. | [ring][ring], [deque][deque], [Pool::shutdown][pool-cpp] | [queue tests][queue-test]: `ring_delayed_publication`; [idle tests][idle-test]: `moving_work_cannot_look_idle` |

<a id="parking"></a>
## Parking и уведомления

| ID | Контракт | Реализация | Проверка |
| --- | --- | --- | --- |
| PARK-01 | ParkingLot использует неизменный worker order scheduler. Каждому worker соответствует ровно один bitmap bit; domain masks корректны и для shared boundary words, sparse/empty domains, interleaved mapping. Scheduler storage живёт дольше ParkingLot. | [ParkingLot::ParkingLot][parking-cpp], [Pool members][pool-hpp] | [topology][topology]: `parking_membership`, `packed_domain_boundaries`; [topology_allocation_tests.cpp][topology-alloc] |
| PARK-02 | Worker: snapshot epoch → объявить idle bit → SC fence → final acquisition. Publisher: publish queue → SC fence → idle probe. Парные fences не позволяют одновременно пропустить work и registration. Удаление fence у no-idle fast path нарушает протокол. | [prepare, wake_one][parking-cpp], [worker_loop][pool-cpp] | [topology][topology]: `parking_publication_race`, `queue_publication_race`; [wake fuzz][wake-fuzz] |
| PARK-03 | Notifier CAS'ом claims одну registration и меняет epoch. Уже claimed bit нельзя очищать повторно: owner мог зарегистрировать следующее ожидание. Ответственность за сигнал переходит notifier'у, выигравшему claim. | [wake_word, wake_one, signal][parking-cpp] | [parking_epoch_reuse_test.cpp][epoch-reuse], [wake fuzz][wake-fuzz] |
| PARK-04 | Перед CV wait worker ставит sleeping под mutex и проверяет stop/epoch. SC epoch increment + sleeping probe дают выбор: worker видит epoch либо notifier видит sleeping и делает mutex handshake + notify. Notification не должна попасть в щель между predicate check и sleep. | [ParkingLot::wait, signal][parking-cpp] | [epoch reuse][epoch-reuse], [wake fuzz][wake-fuzz], [parking_fuzz.cpp][parking-fuzz] |
| PARK-05 | Успешное final acquisition отменяет idle bit до user code/relay; выход из wait, timeout и stop также снимают registration. Spurious/timed wakes допустимы: worker снова ищет work. Wake не является разрешением читать packet без queue acquisition. | [cancel, wait][parking-cpp], [try_help_one][pool-cpp] | [topology][topology], [parking fuzz][parking-fuzz] |
| PARK-06 | `wake_one` предпочитает home domain, но при пустом/занятом domain может выбрать remote idle worker. No-idle не требует representative epoch RMW. `wake_all` сигналит всех, включая stop; completion wake адресует конкретного зарегистрированного worker. | [wake_one, wake_all, wake_worker][parking-cpp] | [topology][topology]: `parking_membership`, `packed_domain_boundaries`; [worker-wait][worker-wait] |

<a id="idle"></a>
## Physical idle accounting

| ID | Контракт | Реализация | Проверка |
| --- | --- | --- | --- |
| IDLE-01 | Каждый worker пишет только свои published/retired; внешний producer пишет только свой published. Publication и retirement монотонны. Steal/transfer не переносят accounting между lanes; retirement принадлежит исполнителю. | [IdleAccounting][idle-hpp], [enqueue_prepared, execute_task][pool-cpp] | [idle_accounting_tests.cpp][idle-test]: `pending_descendant`, `moving_work_cannot_look_idle`; [accounting_fuzz.cpp][account-fuzz] |
| IDLE-02 | Publication после успешной подготовки/регистрации и до queue visibility; retirement после callable/packet cleanup и completion propagation. Публикация потомка из body, destructor или payload cleanup предшествует retirement родителя. | [publish, retire][idle-hpp], [execute_task][pool-cpp] | [idle tests][idle-test]: `destructor_publication`; [reentry][reentry]; [lifecycle][lifecycle]: `shutdown_waits_for_capture_destruction` |
| IDLE-03 | Registry и producer lanes принадлежат Pool, не TLS/thread lifetime. TLS cache содержит только borrow с уникальным pool identity; stale pointer при повторном использовании адреса не разыменовывается. Producer exit и eviction не удаляют accumulated counts; список уничтожается итеративно. | [producer_lane, unique_id, ~IdleAccounting][idle-cpp] | [pool_address_reuse_tls_test.cpp][address-reuse], [idle tests][idle-test]: `reused_address_and_cache_eviction`; [accounting allocations][account-alloc] |
| IDLE-04 | При scan registry mutex запрещает добавление невидимого producer. Acquire scans идут retired-before → published → retired-after; idle принимается только при равенстве всех трёх totals. Нельзя сначала читать publications: старый total может совпасть с новыми retirements. | [IdleAccounting::idle_locked][idle-cpp] | [idle tests][idle-test]: `moving_work_cannot_look_idle`, `registration_and_multiple_waiters`; [accounting fuzz][account-fuzz] |
| IDLE-05 | Totals суммируются с carry в два 64-bit слова; per-lane counters не wrap. Publication exhaustion бросает до queue commit, retirement exhaustion завершает процесс. Pool/producer IDs не переиспользуются; zero identity вызывает terminate. | [Total, unique_id][idle-cpp], [publish, retire][idle-hpp] | Обычные счётчики: [idle tests][idle-test]; [boundary PBT](../tests/detail_accounting_boundary_property_tests.cpp) проверяет publication/retirement exhaustion через сбалансированную историю около UINT64_MAX; исчерпание identity отдельно не покрыто |
| IDLE-06 | Waiter объявляется под registry/CV mutex, выполняет SC fence, проверяет idle и ждёт. Worker на no-work пути делает парный fence, проверяет waiter count, делает mutex handshake и notify. Нет per-task notification counter; spurious notification требует нового scan. | [IdleAccounting::wait, worker_idle][idle-cpp], [worker_loop][pool-cpp] | [idle tests][idle-test]: `retire_wait_race`, `registration_and_multiple_waiters`; [accounting fuzz][account-fuzz] |
| IDLE-07 | `wait_idle()` наблюдает физическое завершение, не закрывает admission и не ждёт ещё не принятых submissions. Concurrent submit может продлить wait или начать новую работу после наблюдённого idle. Оно не заменяет scope.close и не означает отсутствие живых Handles. | [Pool::wait_idle][pool-cpp], [IdleAccounting::wait][idle-cpp] | [idle tests][idle-test], [lifecycle][lifecycle]; конкурентное будущее admission не является частью барьера |

<a id="graph"></a>
## TaskGraph: definition, run, lanes, tokens

| ID | Контракт | Реализация | Проверка |
| --- | --- | --- | --- |
| GRAPH-01 | Граф допускает последовательные повторные runs, максимум один active run. Build/mutate/run сериализуются caller'ом; наличие `ensure_not_running` не превращает API в потокобезопасный builder. Mutation/reset/clear/run при active result бросают `logic_error`; seal возвращает false. | [TaskGraph API][graph-hpp], [ensure_not_running, seal][graph-cpp] | [graph_tests.cpp][graph-test]: `graph_explicit_cancel_and_retry`, `graph_capacity_and_reuse` |
| GRAPH-02 | Builder и compiled CSR принадлежат graph. Seal проверяет acyclicity, строит roots/edges/options; false на cycle, run непустого циклического graph бросает. Empty run возвращает invalid-ready Handle. До всех throwing allocations callable transfer не начинается, поэтому failed seal сохраняет callable state. | [TaskGraph::seal, run][graph-cpp] | [graph tests][graph-test]: `graph_tokens_and_errors`, `graph_recompile_and_callable_ownership`; [failure][failure]: `graph_seal_failures` |
| GRAPH-03 | Node/edge counts ограничены UINT32_MAX до narrowing; UINT32_MAX — no-node sentinel. NodeId входы size_t проверяются bounds checks. Edge registration увеличивает predecessor count только после успешного добавления edge. Duplicate edges считаются отдельными arrivals. | [emplace_indexed][graph-hpp], [add_edge, seal][graph-cpp] | [graph tests][graph-test]: `graph_compiled_options_and_indices`; огромные allocation/count limits отдельно не исчерпываются |
| GRAPH-04 | Граф owns переиспользуемые NodeState[], InitialTask[] и allocation RunState. RunState хранит spans/raw borrows, packets держат borrow под live credit. Новый run получает новый CompletionState; old Handles сохраняют старые результаты. RunState реконструируется только после ready предыдущего run. | [run, reset][graph-cpp], [RunState][graph-hpp] | [ownership][ownership]: `graph_repeated_wide_runs_and_old_results`; [failure][failure]: `graph_reuses_runtime_storage` |
| GRAPH-05 | Reset сохраняет topology/callables/runtime allocations и сбрасывает last run state. Clear удаляет nodes/callables/compiled/run/runtime storage; builder vector capacity может сохраняться. Mutation инвалидирует compiled snapshot; roots перестраиваются. | [clear, reset, seal][graph-cpp] | [graph tests][graph-test]: `graph_recompile_and_callable_ownership`, `graph_cancel_before_activation_and_reseal` |
| GRAPH-06 | Startup sentinel жив до окончания всех root publications. Любой activator/spawner сам держит credit; terminal completion невозможен между публикациями roots/children. После завершения sentinel путь run не обращается к state. | [TaskGraph::run, activate, publish_initial][graph-cpp] | [ownership][ownership], [graph_cancel_continuation_reuse_race_test.cpp][graph-race], [graph_fuzz.cpp][graph-fuzz] |
| GRAPH-07 | При activation token/lane state инициализируется до публикации lanes; это обязательно и после отменённого run, не достигшего node. Multi-predecessor arrivals сбрасываются до root publication. Последняя lane при activation остаётся у caller, пока остальные уже могут исполняться. | [run, activate][graph-cpp] | [graph tests][graph-test]: `graph_cancel_before_activation_and_reseal`, `graph_lane_boundaries_and_visibility` |
| GRAPH-08 | Token — индекс вызова, не отдельный packet/owned object. `set_tokens(0)` означает один вызов. CAS cursor выдаёт каждый admitted token не более одного раза и не переполняется даже при tokens=SIZE_MAX. Одновызовный node обходит cursor, поскольку имеет одну lane. | [set_tokens, execute][graph-cpp] | [graph tests][graph-test]: `graph_lane_boundaries_and_visibility`, `graph_explicit_cancel_and_retry`, `graph_compiled_options_and_indices`; SIZE_MAX — отменяемый boundary сценарий, не исполнение всех tokens |
| GRAPH-09 | Lane count ограничен tokens, capacity, concurrency и pool workers; `concurrency<=0` снимает только node-specific bound. Даже zero-admitted/cancelled node имеет служебный путь retirement. Один persistent callable node может вызываться одновременно; его состояние должно допускать выбранную concurrency. | [seal, activate, execute][graph-cpp] | [graph tests][graph-test]: `graph_barrier_and_cancellation`, `graph_lane_boundaries_and_visibility`; thread safety пользовательского callable — предусловие |
| GRAPH-10 | Block исполняет все tokens ограниченным числом lanes и не блокирует worker на capacity. Drop допускает первые min(tokens, capacity); zero-admitted node всё равно освобождает successors. Fail записывает error/cancel при initial_tokens>capacity. Block capacity=0 отвергается уже при emplace и проверяется на seal; invalid priority/overflow отвергаются на seal. | [seal, activate][graph-cpp] | [graph tests][graph-test]: `graph_capacity_and_reuse`, `graph_compiled_options_and_indices` |
| GRAPH-11 | Только последняя lane освобождает outgoing edges. Для multi-lane нужен acq_rel join, даже если cancel изменил фактическое число активированных lanes. Для multi-predecessor successor только последний acq_rel arrival активирует node и собирает все predecessor writes. Single-lane/single-edge путь имеет одного владельца. | [TaskGraph::execute][graph-cpp] | [graph tests][graph-test]: `graph_dependencies`, `graph_barrier_and_cancellation`, `graph_lane_boundaries_and_visibility` |
| GRAPH-12 | Bypass переносит текущую execution lane/credit в одного ready successor только с теми же priority и affinity; остальные successors публикуются. Бюджет token/transition шагов ограничивает непрерывный bypass. Handoff публикует continuation под новым credit до release текущего; handoff не равен lane completion. | [execute, publish][graph-cpp] | [graph tests][graph-test]: `graph_budget_allows_priority_progress`; [graph race][graph-race] |
| GRAPH-13 | InitialTask slot используется максимум один раз на node за run. Его erased release заканчивает borrow, не уничтожает array element. Additional lanes и same-node continuations используют отдельные packets: старый packet ещё может быть в epilogue. После release/final credit запрещены обращения к slot/run/definition. | [InitialTask::release, publish_initial, publish][graph-cpp] | [ownership][ownership]: `graph_definition_can_die_after_ready`; [graph race][graph-race]; [lifecycle][lifecycle]: `graph_and_scope_after_close` |
| GRAPH-14 | Allocation/admission failure в publication гасит непереданный child credit, записывает first error/cancel и завершает зарезервированную lane под caller credit. Принятые lanes дренируются; pending count не остаётся висеть. Failures до создания/публикации run могут бросить синхронно; publication failures представлены результатом run. | [run, publish, publish_initial, record_error][graph-cpp] | [failure][failure]: `graph_continuation_failure`, startup failure scenarios; [graph race][graph-race] |
| GRAPH-15 | Cancel кооперативен: уже выполняющийся callable заканчивает, непринятые к исполнению invocations пропускаются; credits всё равно гасятся. Cancel сам по себе не error. First error выбирается call_once и передаётся completion, затем отменяет run. `cancel`/`last_error` требуют стабильного run storage относительно run/reset/clear/destruction. | [cancel, last_error, record_error, execute][graph-cpp] | [graph tests][graph-test]: `graph_explicit_cancel_and_retry`; [graph race][graph-race] |
| GRAPH-16 | Ready означает окончание всех runtime borrows на graph/run, поэтому clear/reset/destruction допустимы без wait_idle. Destructor ждёт active run, но не запускает ещё не запущенный TaskGraph. Node callables живут до clear/destruction/replacement storage и сохраняют mutable state между runs. Нельзя уничтожать граф из его собственного active run. | [~TaskGraph, clear, reset][graph-cpp] | [ownership][ownership]: `graph_definition_can_die_after_ready`; [graph tests][graph-test]: `graph_cooperative_destruction_and_chain`; self-destruction не диагностируется как общий цикл |
| GRAPH-17 | Emplace сначала строит callable, затем builder node и pending callable entry; если pending storage бросает, добавленный node удаляется. Existing graph/callables сохраняются, но переданный rvalue caller'а уже может быть moved-from. Этим не обещается транзакционность составных GraphScope операций. | [TaskGraph::emplace_indexed][graph-hpp] | [failure][failure]: `graph_builder_failures`; [gs-unwind][gs-unwind]: `throwing_chunk_copy` |

<a id="scope"></a>
## TaskScope

| Состояние | Внешний spawn | Context::spawn | Что удерживает completion |
| --- | --- | --- | --- |
| Open | Разрешён | Разрешён | Root sentinel, publishers, executors |
| Closed | Отклонён | Разрешён без cancel | Принятые publishers/executors/descendants |
| Cancelled/failed | Отклонён | Отклонён после наблюдения cancel | Дренирование принятой работы и cleanup |
| Complete | Отклонён | Valid Context больше нет | Только observation references, ready навсегда |

| ID | Контракт | Реализация | Проверка |
| --- | --- | --- | --- |
| SCOPE-01 | TaskScope — one-shot dynamic group: spawn/submit публикуют сразу, reset/rerun нет. True означает принятие, но cancel может пропустить body. False возвращается до runtime construction/move callable; вычисление аргументов caller'ом уже произошло. | [TaskScope::spawn, Context::spawn][scope-hpp] | [scope tests][scope-test]: `empty_and_closed`, `cancellation_skips_queued`; [failure][failure]: `scope_publication_failures` |
| SCOPE-02 | Внешний CAS reservation защищает доступ к root. Close атомарно закрывает вход и гасит sentinel сам при нуле reservations либо оставляет это последнему publisher. Close не ждёт. После fork publisher имеет собственный credit до конца construction/publication cleanup. | [reserve, close_admission][scope-cpp], [publish][scope-hpp] | [scope tests][scope-test]: `submit_close_race`, `publisher_keeps_completion_pending`, `simultaneous_publish_close_cancel` |
| SCOPE-03 | Child admission — наблюдение cancel и fork живого parent credit; root не нужен. Close разрешает descendants, cancel закрывает оба входа. Уже принятый publisher может закончить publication после cancel; callback дополнительно проверяет cancel перед invocation. | [reserve, request_cancel][scope-cpp], [invoke][scope-hpp] | [scope tests][scope-test]: `descendants_after_close`, `child_reservations_survive_close_and_cancel` |
| SCOPE-04 | State отдельно принадлежит scope owner. Publisher и executor имеют разные credits: executor держит state через callback и capture cleanup, publisher — через rollback enqueue parameters. Context noncopyable/nonmovable, жив только внутри callback; сохранять его или передавать независимо работающему коду запрещено. | [State][scope-cpp], [Context, ScopedTask, publish][scope-hpp] | [scope tests][scope-test]: `publisher_keeps_completion_pending`; [cancelled capture reentry][cancel-reentry]; сохранение Context — предусловие |
| SCOPE-05 | Callback принимает Context& или ничего; возвращение child work через ctx.spawn включает его в группу. Ручной Pool::submit из callback не включён в scope completion. Caller-owned captures/temporaries остаются обязанностью caller. | [invoke, Context::spawn][scope-hpp] | [scope tests][scope-test]: `descendants_after_close`, `destructor_drains`; принадлежность ручных submissions — по API |
| SCOPE-06 | Первая callback/construction/publication error выбирается под error_mutex, запрашивает cancel и попадает в общий completion. Publication failure также перебрасывается submitter'у; пойманное им исключение не стирает scope error. last_error не публикует ошибку до запроса cancel. | [record_error][scope-cpp], [publish, invoke][scope-hpp] | [scope tests][scope-test]: `failure_and_first_error`; [failure][failure]: `scope_publication_failures`; [scope_races_fuzz.cpp][scope-races] |
| SCOPE-07 | Close возвращает observer и не ждёт. Wait закрывает и ждёт без rethrow, join после wait перебрасывает first error; повторные вызовы разрешены. Completion observer при открытом sentinel сам по себе не завершится. Cancel без error — успешное completion. | [close, wait, join, completion][scope-cpp] | [scope tests][scope-test]: `empty_and_closed`, `failure_and_first_error` |
| SCOPE-08 | Destructor закрывает и дожидается всей принятой работы/cleanup, в том числе при unwinding; он не отменяет автоматически и не перебрасывает task error. После final ready task-side state accesses закончились; residual pool/completion epilogues используют собственное storage. | [~TaskScope][scope-cpp], [ScopedTask::~ScopedTask][scope-hpp] | [scope tests][scope-test]: `destructor_drains`; [cancelled capture reentry][cancel-reentry], [reentry][reentry] |
| SCOPE-09 | ActiveFrame покрывает callback, callable construction/destruction и nested helping. Wait/join своего scope или active ancestor на том же потоке бросает logic_error; self-destruction terminate. Прямой Pool::wait(scope.completion()) обходит эту проверку и не разрешён из contributing work. | [ActiveFrame, active_here, wait, ~TaskScope][scope-cpp], [publish, ScopedTask][scope-hpp] | [scope tests][scope-test]: `cooperative_join_and_self_join`, `destructor_self_join`; [nested][nested], [cancel-reentry][cancel-reentry] |
| SCOPE-10 | Внешние spawn/close/cancel/wait/join/observations могут гоняться при живом scope object, но обязаны закончиться до его destruction. Callback должен использовать Context вместо сохранённого owner, который уже может разрушаться. Нет общей cross-thread/cross-scope cycle detection. | [TaskScope API][scope-hpp], [scope implementation][scope-cpp] | [scope tests][scope-test]: `simultaneous_publish_close_cancel`; [scope races fuzz][scope-races]; destruction races — предусловие |

<a id="graph-scope"></a>
## GraphScope и identity

| ID | Контракт | Реализация | Проверка |
| --- | --- | --- | --- |
| GS-01 | GraphScope — reusable DAG builder; emplace/submit/then/when_all строят graph и не исполняют сразу. Run возвращает тот же Handle, пока wait не потребил его; после wait явный run может повторить graph. Repeated wait/destructor завершённый неизменённый graph не перезапускают. | [GraphScope::run, wait][gs] | [graph tests][graph-test]: `scope_building`, `scope_repeated_parallel_for` |
| GS-02 | Wait/destructor запускают pending dirty graph при необходимости и ждут active run. Wait сохраняет last_error без rethrow task failure; run_and_wait перебрасывает его. Structural/run startup errors всё ещё могут бросить из wait; destructor ловит исключения. Clear дожидается active run и удаляет pending definition, не исполняя не начатую работу. | [GraphScope::wait, clear, ~GraphScope][gs] | [graph tests][graph-test]: `graph_tokens_and_errors`, `scope_repeated_parallel_for`; [graph_scope_unwind_cleanup_test.cpp][gs-unwind] |
| GS-03 | JobHandle/NodeId — только index, без ownership, graph identity или generation. Valid JobHandle проверяет лишь npos. Caller использует IDs только своего графа и до clear; foreign/stale ID с допустимым числом может обратиться к другому node. Builder composition не обещает rollback всего then/when_all при ошибке edge. | [JobHandle, then, when_all][gs], [TaskGraph::add_edge][graph-cpp] | Bounds: [graph tests][graph-test]: `graph_compiled_options_and_indices`; identity/generation и transactional composition — защиты нет |
| GS-04 | GraphScope::parallel_for хранит отдельную копию callable на chunk и persistent iterators. Callable copy-constructible, chunk-state сохраняется между runs; backing data и iterator-owning view живут до удаления node/уничтожения scope. Empty range создаёт no-op node, пригодный для dependency. | [parallel_for, parallel_for_after][gs] | [graph tests][graph-test]: `scope_repeated_parallel_for`; [api-test][api-test]: `graph_ranges`; [gs-unwind][gs-unwind]: `throwing_chunk_copy` |

<a id="ranges"></a>
## Range algorithms и detached batch

| ID | Контракт | Реализация | Проверка |
| --- | --- | --- | --- |
| RANGE-01 | Pool for_each требует корректный multipass forward range, for_each_ws — random-access, не обязательно contiguous. Range overloads принимают lvalue/borrowed range, не owning temporary. Iterator overload не снимает обязанности сохранить container/view/iterators. Нельзя инвалидировать range до completion. | [Pool range overloads][pool-hpp] | [api-test][api-test]: compile-time constraints, `range_algorithms`, `linear_forward_traversal`; [graph tests][graph-test]: `pool_algorithms` |
| RANGE-02 | Pool range operation owns один callable/ProcState в completion payload; tasks разделяют его под credits. Callable должен допускать concurrent invocation; runtime не копирует элементы и не сериализует пользовательские side effects. Пустой range возвращает invalid-ready Handle. | [for_each, for_each_ws][pool-hpp] | [ownership][ownership]: `range_payload_released_before_result`; [api-test][api-test]: `range_algorithms` |
| RANGE-03 | Sentinel покрывает initial chunk/tile publication; recursive split forks credit до child publication и до освобождения parent. Никакой child не обращается к ProcState после final credit. Worker split failure попадает в completion через execute_task; уже опубликованные siblings дренируются. | [for_each_ws::Exec][pool-hpp], [execute_task][pool-cpp] | [pool queues][pool-queue]: `algorithm_exceptions`; [ranges_fuzz.cpp][ranges-fuzz] |
| RANGE-04 | Synchronous initial publication failure закрывает sentinel и ждёт все accepted range users перед rethrow, включая worker Reject. Это защищает borrows при unwinding. Task-body exception останавливает текущий body, но не обещает all-elements execution/rollback; callers читают Handle error. | [for_each/for_each_ws catch][pool-hpp] | [range_publication_rollback_lifetime_test.cpp][range-rollback], [pool queues][pool-queue]: `partial_range_submission`, `algorithm_exceptions` |
| BATCH-01 | Detached batch move-consumes span elements, создаёт независимые tasks без per-task или aggregate Handle. Empty span no-op. Успех публикует всё без скрытого buffer; не гарантирует execution order, simultaneous visibility или completion. Errors следуют PKT-06. | [submit_batch_detached][pool-hpp] | [batch tests][batch-test]: `move_only_and_descendants`, `partial_failure_and_task_exceptions` |
| BATCH-02 | Внешняя группа ≤64 полностью подготавливается до reservation/publication, выбирает один shard; accounting и queue push выполняются для каждого packet, wake — после группы. При saturation wake происходит и перед retry; при частичном failure принятый prefix также будится. | [enqueue_batch_prepared][pool-cpp] | [batch tests][batch-test]: `progress_and_idle`, `backpressure`, `concurrent_producers`; [failure][failure]: `batch_publication_failures` |
| BATCH-03 | Batch даёт basic exception guarantee: ранние группы/prefix могут уже работать, непринятые элементы тоже могут быть moved-from. Автоматического join на failure нет; borrowed data caller сохраняет до завершения accepted tasks. Close может отвергнуть следующую подготовленную группу. | [submit_batch_detached][pool-hpp], [enqueue_batch_prepared][pool-cpp] | [batch tests][batch-test]: `partial_failure_and_task_exceptions`; [lifecycle][lifecycle]: `close_between_batch_groups` |
| BATCH-04 | Собственный worker использует scalar enqueue для каждого элемента, сохраняя shared overflow и visibility для helping. Worker другого Pool идёт внешним batch путём. Retirement всегда обычный packet epilogue, не отдельный batch accounting. | [submit_batch_detached][pool-hpp], [execute_task][pool-cpp] | [batch tests][batch-test]: `worker_overflow_and_cross_pool`, `move_only_and_descendants` |

<a id="memory"></a>
## Память, callable wrappers, контейнеры

| ID | Контракт | Реализация | Проверка |
| --- | --- | --- | --- |
| MEM-01 | DagFlow использует выбранный process-wide backend system/mimalloc/tbbmalloc, без собственного slab/free-list/remote-return. Allocation и deallocation парны по backend и alignment, free допустим на другом потоке/после Pool; backend обязан поддерживать over-alignment. | [runtime_memory.hpp][memory-hpp], [runtime_memory.cpp][memory-cpp] | [runtime_memory_tests.cpp][memory-test]: `small_class_alignment_and_remote_free`; [stl_allocation_tests.cpp][stl-test] |
| MEM-02 | Alignment allocate_bytes — ненулевая поддерживаемая степень двух. Zero-byte allocation даёт минимальный aligned block. Constructor failure в make_owned/array освобождает storage; array owner уничтожает все построенные элементы. Scalar default-initialized array elements должны быть записаны до чтения. | [allocate_bytes][memory-cpp], [make_owned, make_owned_array][memory-hpp] | [memory tests][memory-test]: `array_storage`, `zero_size_storage`; [topology allocation][topology-alloc] |
| MEM-03 | Array size проверяется относительно PTRDIFF_MAX/sizeof(T); RuntimeAllocator относительно SIZE_MAX/sizeof(T) до умножения. RuntimeAllocator stateless/equal, move assignment передаёт allocator, не хранит Pool pointer. Пользовательские STL allocations не становятся автоматически runtime-owned. | [RuntimeAllocator, make_owned_array][memory-hpp] | [stl tests][stl-test], [memory tests][memory-test] |
| FN-01 | small_function/inplace_function move-only; target должен быть invocable и noexcept-destructible. Inline допустим лишь при fitting size/alignment и noexcept move. small_function spill использует runtime backend; inplace_function неподходящий target отвергает compile-time. Wrapper move noexcept, heap move переносит pointer. | [basic_function][function-hpp], [inplace_function.hpp][inplace] | [function_tests.cpp][function-test]: `storage_boundaries`, `throwing_move_pointer_steal`, compile-time assertions |
| FN-02 | Empty invocation — нарушение предусловия, не bad_function_call. Reset сначала снимает ops, затем уничтожает target. Emplace строит replacement до commit: при allocation/construction failure старый target сохранён. | [basic_function::operator(), reset, emplace][function-hpp] | [function tests][function-test]: `inline_lifetime`, `constructor_failure`, `allocation_failure`; empty invocation не исполняется как valid scenario |
| VEC-01 | small_vector move-only; inline N>0, heap aligned как max(alignof(T), cache line). Только [0,size) содержит живые objects; raw slots не читаются как constructed T. Clear уничтожает elements и сохраняет capacity; reserve растит точно до request, automatic growth удваивает с limit check. | [small_vector][vector-hpp] | [container_tests.cpp][container-test]: `vector_storage`, `vector_alignment`, `vector_inline_reuse`; [vector_allocation_tests.cpp][vector-test] |
| VEC-02 | Growth сначала конструирует appended element, поскольку args могут ссылаться на старый storage. Relocation использует move_if_noexcept, очищает построенный prefix при failure. Throwing move-only T имеет basic guarantee: исходные элементы могут быть moved-from. Heap move переносит buffer, inline move переносит elements. | [emplace_back, relocate_to, move_from][vector-hpp] | [vector tests][vector-test]: `relocation_failure`, `throwing_move_only`, `allocation_policy`; [container tests][container-test]: `vector_exception_lifetime` |
| VEC-03 | operator[] требует index<size; front/back/pop_back требуют nonempty; at бросает out_of_range. Reallocation инвалидирует pointers/iterators; конкурентная mutation не поддерживается. Destructors T обязаны не бросать на noexcept cleanup путях. | [small_vector accessors, release_storage][vector-hpp] | [container tests][container-test], [vector tests][vector-test]; invalid unchecked access — предусловие |

<a id="ordering"></a>
## Порядок памяти и точки принятия решений

Эта таблица связывает протоколы выше; одинаковый счётчик не может заменить
несколько разных барьеров. Acquire/release относится к указанным операциям,
а не к произвольной несинхронной mutation пользовательских объектов.

| Переход | Обязательная связь | Реестр |
| --- | --- | --- |
| External accept / close | Reservation CAS и closed bit одного atomic; release последнего publisher наблюдается acquire shutdown. Guard покрывает dispatch **и wake**. | POOL-04…08 |
| Packet publication / acquisition | Полная construction предшествует queue release; consumer acquire/CAS получает право чтения. Transfer требует новой публикации и wake relay. | PKT-01…04, QUE-02…04, SCH-05…06 |
| Cleanup / logical completion | Callable/packet release → credit acq_rel retirement → payload destruction → ready release → observer acquire. Последний reference живёт через доставку dependents. | CMP-01…06 |
| Node / successor | Последняя multi-lane acq_rel retirement собирает node writes; последняя multi-predecessor acq_rel arrival собирает predecessor writes до successor publication/bypass. | GRAPH-11…12 |
| Scope admission / final state access | Root reservation защищает fork; publisher/executor credits защищают state до окончания cleanup; closed/cancel bits не заменяют эти credits. | SCOPE-02…08 |
| Publisher / parking waiter | Queue publication → SC fence → idle probe; idle announcement → SC fence → final acquisition; epoch/sleeping SC и mutex закрывают переход в CV. | PARK-02…05 |
| Completion / parked helper | Stack registration под completion mutex/gate; ready-store до адресных wakes; removal под тем же mutex до уничтожения записи. | WAIT-05…06 |
| Task / physical idle | Publication release до queue; retirement release после completion propagation; acquire retired/published/retired scans под фиксированным registry. | IDLE-01…05 |
| Last work / idle waiter sleep | Waiter announcement + SC fence и no-work worker fence + probe, затем общий mutex/CV handshake. | IDLE-06 |

<a id="limits"></a>
## Непроверенные границы и ограничения

Эти требования тоже часть реестра. Они явно отделены от проверяемых гарантий:
добавление строки в Markdown не добавляет overflow guard или deadlock detector.

| ID | Предусловие / пробел текущей реализации | Реализация | Проверка / предел проверки |
| --- | --- | --- | --- |
| LIMIT-01 | Индексы Chase–Lev не должны переполнять ptrdiff_t за жизнь deque. Wrap ring-buffer slots допустим и проверяется; wrap signed logical indices этим не разрешается. Runtime reset/overflow guard отсутствует. | [chase_lev_deque][deque] | [queue tests][queue-test]: `deque_wraparound_contention` проверяет reuse slots, **не** PTRDIFF_MAX exhaustion |
| LIMIT-02 | Ring sequence/position comparison использует signed ptrdiff_t subtraction после conversion. Требуется представимость промежуточных разностей; полный integer wraparound на всём size_t диапазоне не доказан и не защищён. Не считать finite-capacity slot reuse проверкой этого случая. | [ring_mpmc::try_pop, push_impl][ring] | [queue tests][queue-test] покрывают обычные rotations; boundary seam для signed subtraction отсутствует |
| LIMIT-03 | Completion references/credits не должны overflow size_t; retain/fork не проверяют максимум. Парковочный epoch не должен совершить полный uint64_t оборот между snapshot и проверкой, иначе возможно ABA; отдельного guard нет. | [CompletionState::retain, CompletionCredit::fork][pool-cpp], [ParkingLot::signal][parking-cpp] | [last credit race][last-credit], [epoch reuse][epoch-reuse] проверяют races, не числовое exhaustion |
| LIMIT-04 | Idle waiters count и diagnostic nested-wait depth должны помещаться в uint32_t; guards отсутствуют. Account published/retired имеют guards (IDLE-05); admission publisher counts проверяются отдельно, а не wrap'аются в flag bits. | [IdleAccounting::wait][idle-cpp], [wait_impl, enter_publication][pool-cpp], [TaskScope::reserve][scope-cpp] | [worker-wait][worker-wait] проверяет depth=32; boundary counts отдельно не покрыты |
| LIMIT-05 | Iterator pairs должны задавать достижимый неперевёрнутый range, distance помещаться в difference_type. Chunk/tile arithmetic (`n+target-1`, `c*n`, `n*t`, `T*8u`) должна помещаться в используемые типы. В range helpers нет полной checked arithmetic; поддержка SIZE_MAX graph tokens не распространяется на ranges. | [Pool range helpers][pool-hpp], [GraphScope::parallel_for][gs] | [api-test][api-test], [ranges fuzz][ranges-fuzz] покрывают bounded inputs; numeric extremes — пробел |
| LIMIT-06 | Raw Fn должен быть ненулевым вызываемым function pointer; arg и его pointees живут до завершения обращения. SubmitOptions/WorkerWaitPolicy следует передавать допустимыми enum значениями: общей runtime validation для них нет. Graph node priority/overflow валидируются отдельно. | [Pool::submit(Fn), dispatch, wait_impl][pool-cpp], [TaskGraph::seal][graph-cpp] | Обычные API scenarios; invalid Fn/forged enums — не обещанная диагностика |
| LIMIT-07 | Значения spin/backoff/batch/graph budget — текущая политика, не hard real-time guarantee. Нет bounded latency, глобального FIFO, гарантии fairness normal-priority при бесконечном high-priority потоке или ограничения всей outstanding памяти. | [scheduler constants][sched-cpp], [worker_loop][pool-cpp], [execute][graph-cpp] | [fairness][fairness] проверяет конкретные service scenarios; benchmark latency не является контрактом |
| LIMIT-08 | Полнота модели не означает формальную верификацию всех межпоточных расписаний/платформ. Stress/fuzz/sanitizers не заменяют рассуждение о happens-before; source links и tests ниже не являются свежим отчётом о passing builds. | Все concurrency протоколы выше | [tests/README.md][test-readme], [fuzz/README.md][fuzz-readme]; weak-memory validation выполняется отдельно |

<a id="validation"></a>
## Как проверять и обновлять реестр

При изменении runtime обновить затронутый ID, реализацию и regression mapping
в этом файле. Сохранять IDs для того же обязательства; новый независимый
контракт получает новый ID. Изменение очереди, счётчика или wake ordering должно
объяснять конкретную гонку и порядок публикации, а не только сообщать stress pass.
Предусловие нельзя переводить в гарантию без проверки реализации.

Базовая проверка runtime из корня репозитория:

```sh
cmake --preset core
cmake --build --preset core --parallel 2
ctest --preset core --output-on-failure --no-tests=error
```

Для lifetime/concurrency изменений отдельно configure/build/test выбранных
`check-asan`, `check-tsan`, `adversarial-tiny`. Тесты используют Release-active
CHECK. Allocation interposition имеет ограничения backend/platform/sanitizer,
описанные в [tests/README.md][test-readme]. Fuzz build сам по себе не запускает
кампанию: seed replay и timed campaigns различаются, см. [fuzz/README.md][fuzz-readme].
Для чистого изменения документации достаточно проверить ссылки, IDs и сверку
формулировок с кодом; нельзя выдавать это за запуск runtime tests.

Дополнительные объяснения: [архитектура](how_it_works.md),
[владение](explicit-ownership.md), [Pool lifecycle](pool-lifecycle.md),
[idle accounting](idle-accounting.md), [scheduler topology](scheduler-topology.md),
[TaskScope](task-scope-lifetime.md), [batch](batch-submit.md).
Build/CMake/benchmark methodology находятся вне runtime protocol и описаны в
[build-and-layout.md](build-and-layout.md) и соответствующих benchmark reports.

[pool-hpp]: ../include/dagflow/thread_pool.hpp
[pool-cpp]: ../src/thread_pool.cpp
[handle]: ../include/dagflow/handle.hpp
[graph-hpp]: ../include/dagflow/task_graph.hpp
[graph-cpp]: ../src/task_graph.cpp
[scope-hpp]: ../include/dagflow/task_scope.hpp
[scope-cpp]: ../src/task_scope.cpp
[gs]: ../include/dagflow/graph_scope.hpp
[sched-cpp]: ../src/scheduler.cpp
[parking-cpp]: ../src/parking_lot.cpp
[idle-hpp]: ../include/dagflow/detail/idle_accounting.hpp
[idle-cpp]: ../src/idle_accounting.cpp
[deque]: ../include/dagflow/detail/chase_lev_deque.hpp
[ring]: ../include/dagflow/detail/ring_mpmc.hpp
[memory-hpp]: ../include/dagflow/detail/runtime_memory.hpp
[memory-cpp]: ../src/runtime_memory.cpp
[function-hpp]: ../include/dagflow/detail/small_function.hpp
[inplace]: ../include/dagflow/inplace_function.hpp
[vector-hpp]: ../include/dagflow/detail/small_vector.hpp
[ownership]: ../tests/ownership_tests.cpp
[lifecycle]: ../tests/pool_lifecycle_tests.cpp
[failure]: ../tests/runtime_failure_tests.cpp
[queue-test]: ../tests/queue_tests.cpp
[queue-alloc]: ../tests/queue_allocation_tests.cpp
[pool-queue]: ../tests/pool_queue_tests.cpp
[topology]: ../tests/scheduler_topology_tests.cpp
[fairness]: ../tests/scheduler_fairness_tests.cpp
[idle-test]: ../tests/idle_accounting_tests.cpp
[account-alloc]: ../tests/accounting_allocation_tests.cpp
[topology-alloc]: ../tests/topology_allocation_tests.cpp
[scope-test]: ../tests/task_scope_tests.cpp
[graph-test]: ../tests/graph_tests.cpp
[api-test]: ../tests/api_convenience_tests.cpp
[batch-test]: ../tests/batch_submit_tests.cpp
[worker-wait]: ../tests/worker_wait_tests.cpp
[range-rollback]: ../tests/range_publication_rollback_lifetime_test.cpp
[graph-race]: ../tests/graph_cancel_continuation_reuse_race_test.cpp
[reentry]: ../tests/reentrant_capture_cleanup_test.cpp
[cancel-reentry]: ../tests/cancelled_capture_reentry_test.cpp
[exception-release]: ../tests/exception_payload_last_release_test.cpp
[last-credit]: ../tests/completion_last_credit_race_test.cpp
[registration-reentry]: ../tests/completion_reentrant_registration_test.cpp
[multi-error]: ../tests/multi_source_completion_failure_race_test.cpp
[combine-oom]: ../tests/combine_partial_registration_oom_race_test.cpp
[nested]: ../tests/nested_helping_context_restore_test.cpp
[epoch-reuse]: ../tests/parking_epoch_reuse_test.cpp
[address-reuse]: ../tests/pool_address_reuse_tls_test.cpp
[gs-unwind]: ../tests/graph_scope_unwind_cleanup_test.cpp
[memory-test]: ../tests/runtime_memory_tests.cpp
[stl-test]: ../tests/stl_allocation_tests.cpp
[function-test]: ../tests/function_tests.cpp
[container-test]: ../tests/container_tests.cpp
[vector-test]: ../tests/vector_allocation_tests.cpp
[queues-fuzz]: ../fuzz/queues_fuzz.cpp
[credits-fuzz]: ../fuzz/credits_fuzz.cpp
[joins-fuzz]: ../fuzz/joins_fuzz.cpp
[wake-fuzz]: ../fuzz/wake_protocol_fuzz.cpp
[parking-fuzz]: ../fuzz/parking_fuzz.cpp
[account-fuzz]: ../fuzz/accounting_fuzz.cpp
[graph-fuzz]: ../fuzz/graph_fuzz.cpp
[scope-races]: ../fuzz/scope_races_fuzz.cpp
[ranges-fuzz]: ../fuzz/ranges_fuzz.cpp
[test-readme]: ../tests/README.md
[fuzz-readme]: ../fuzz/README.md
