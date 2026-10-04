# Явное владение в runtime

Этот документ описывает действующий контракт времени жизни. Он дополняет
[текущий план улучшений](runtime-improvement-plan.md) и не является обещанием
конкретной реализации очереди или allocator backend.

## Четыре независимые роли

Один объект может участвовать сразу в нескольких отношениях, но эти отношения
нельзя смешивать:

| Роль | Кто отвечает | Что означает завершение |
| --- | --- | --- |
| Storage ownership | `OwnedObject<T>` | Объект уничтожается ровно его `ObjectDeleter<T>` |
| Packet custody | publisher, queue или executor | Право обратиться к `ScheduledTask` переходит ровно одному владельцу |
| Liveness credit | `CompletionCredit` | Пока credit жив, операция может быть продолжена или порождена |
| Observation | `Handle` | Можно ждать и читать ошибку; нельзя порождать новые credits |

Graph definition и `RunState` также различаются: граф владеет builder topology,
скомпилированными CSR-массивами `NodeDef[]` / `Edge[]` и переиспользуемым
`NodeState[]`. `RunState` хранит spans на эти массивы, cancellation/error state
и completion handle конкретного запуска. `seal()` переносит callables только
после успешных аллокаций; сбой не теряет их состояние. Счётчики `NodeState[]`
сбрасываются после завершения предыдущего запуска и до публикации нового.
Task closure получает raw borrow на `RunState` только под
живым credit. После terminal completion никакой packet не имеет права читать
graph definition или run state.

## Передача packet

Переходы custody образуют линейную цепочку:

```text
OwnedObject<Task>
      │ успешная публикация
      ▼
Scheduler queue
      │ acquisition
      ▼
Executor
      │ fn.reset → packet destroy → credit.finish
      ▼
освобождение backend
```

До успешной публикации sender сохраняет RAII owner. После передачи sender не
читает и не освобождает packet. Если выделение или публикация не состоялись,
локальный owner уничтожает packet, а unpublished credit закрывается обычным
RAII-путём. Очередь хранит указатель и custody, но не становится вторым
владельцем storage.

## Completion credit

`CompletionCredit` — move-only obligation. Перемещение передаёт obligation,
`fork()` создаёт дочернее obligation только из живого credit, а destructor или
`finish()` погашает его ровно один раз. После последнего credit:

1. уничтожается operation payload;
2. читается сохранённая ошибка (без mutex, если ошибок не было);
3. публикуется `ready()`, атомарно закрывается регистрация dependents;
   только при наличии регистраций под mutex забираются их credits,
   затем будятся ожидающие через atomic readiness;
4. итеративно распространяется ошибка и закрываются дочерние credits;
5. освобождается последнее storage reference.

Регистрация dependents в `combine` помечает атомарный gate под mutex.
Финальное завершение либо видит эту отметку и забирает список под тем же mutex,
либо закрывает gate первым, и поздняя регистрация сразу гасит credit против
готового источника. Обычный путь без ошибки и dependents mutex не берёт.
Ожидание использует `ready.wait(false)`; payload уничтожается до публикации
ready, а финальный storage reference защищает доставку dependents после неё.

`Handle` удерживает только completion state. Он может быть скопирован или
перемещён и может жить после `Pool`, если работа уже завершена. Копия Handle не
увеличивает liveness и не даёт доступа к `fork()`.

Для каждого `RunState` должен существовать один монотонный учёт outstanding
work (не обязательно отдельное поле, если его роль точно выполняют credits):

- принятие единицы работы увеличивает учёт ровно один раз;
- success, cancel, drop и failure-to-publish уменьшают его ровно один раз;
- terminal наступает только при `outstanding == 0`;
- после terminal zero новый credit невозможен;
- любой spawner сам защищён уже существующим credit.

Этот учёт не равен `Handle` и не равен pool idle accounting: первый описывает
логическое завершение запуска, второй — физические packet epilogues пула.

## TaskScope: кто держит state живым

`TaskScope` — одноразовый динамический scope; DAG-builder называется `GraphScope`.
Owner хранит отдельно выделенный `State` и закрывает/дожидается всей работы перед
его освобождением, в том числе при раскрутке стека. Дети получают временный
`Context&`, который ссылается на state и executor credit, а не на уничтожаемый
owner. Контекст действует только до возврата callback; сохранять его запрещено.

Есть три источника liveness:

- sentinel открытого внешнего admission;
- credit publisher, который уже принят, но ещё строит/публикует задачу;
- credit packet/executor, включая callback, descendants и очистку захватов.

Внешний `spawn` резервирует доступ к root через CAS счётчика admission.
`close` атомарно выставляет closed bit и гасит sentinel, если резервирований нет;
иначе это делает последний publisher, закончивший fork root. Дочерний spawn
проверяет cancellation bit и форкает живой credit родителя без mutex и доступа
к root. Publisher удерживает свой credit до окончания очистки параметров
`enqueue`, в том числе при исключении аллокации packet. Executor закрывает свой
credit только после уничтожения callable. Mutex scope защищает лишь первую
ошибку; обычные spawn/finish его не используют.

`close` запрещает только новые внешние submissions. Живой callback может
порождать потомков через `Context::spawn`, пока не запрошена отмена. `cancel`
закрывает оба пути; первая ошибка сохраняется и также запрашивает отмену.
Отмена сама по себе не является исключением. `wait` только ждёт, `join` после
ожидания повторно бросает первую ошибку. Деструктор всегда дожидается очистки.

Переход последнего credit в zero публикует completion ровно один раз. После
него ни publisher, ни packet, ни callback, ни destructor пользовательских
захватов не читает scope state. Owner всё ещё может читать результат и служебные
поля до собственного уничтожения. Остаточный epilogue работает только с pool
и независимо живущим completion state. `Handle` переживает scope, но не даёт
право на новый spawn. Self/active-ancestor join отклоняется через `logic_error`;
самоуничтожение scope из его работы — нарушение precondition с `terminate`.

Полный API, ограничения внешних гонок и примеры:
[TaskScope lifetime](task-scope-lifetime.md).

## Выбранный allocator

DagFlow не содержит собственного slab-пула, free-list или remote-return
протокола. `dagflow/detail/runtime_memory.hpp` предоставляет небольшой adapter для уникального
владельца и rollback при исключении конструктора. CMake выбирает backend:

| Значение | Backend |
| --- | --- |
| `mimalloc` | default production backend |
| `tbbmalloc` | oneTBB scalable allocator |
| `system` | aligned `operator new/delete`, удобно для sanitizers и fault injection |

Для mimalloc естественно выровненные object-size запросы идут в native small-object
path, прочие — в aligned API; tbbmalloc сохраняет собственную политику classes,
system — точный new/delete. Собственная таблица size classes не добавлена.
Scheduler и ParkingLot также используют непрерывные runtime-owned массивы.

Backend обязан принимать освобождение из другого потока и поддерживать
over-aligned objects. STL-контейнеры runtime используют stateless
`detail::RuntimeAllocator<T>` с тем же backend: builder и scratch графа,
completion dependencies, блоки `parallel_for`, массив потоков и
`Config::worker_shards`. Allocator не хранит указатель на Pool; move/swap
передают buffer, освобождение допустимо после уничтожения исходного Pool.
Для копирования обычного `std::vector` в `worker_shards` используйте
`assign(begin, end)`; присваивание initializer-list сохранено.

Сам DagFlow не переопределяет глобальные `new/delete`, но подключённая библиотека
allocator может перехватывать их: установленный mimalloc в
[замерах STL allocation](benchmarks/stl-allocator-routing.md) делает именно это.
Пользовательские данные и captures, входы примеров/бенчмарков, внутренние выделения
`std::thread`, исключений и synchronization primitives не проходят через явный
allocation boundary DagFlow; их фактический backend также зависит от глобального
перехвата символов в процессе.

## Запрещённые сокращения

- Нельзя считать `Handle` владельцем работы.
- Нельзя захватывать `RunState` через `shared_ptr` вместо графового completion
  barrier.
- Нельзя читать packet после передачи custody в queue или executor.
- Нельзя считать `ready()` доказательством того, что произвольный payload ещё
  жив: payload уже уничтожен до публикации готовности.
- Нельзя добавлять собственный allocator только ради уменьшения числа `new`
  без измерения стоимости межпоточного освобождения и удерживаемой памяти.

Проверки этого контракта находятся в `ownership_tests.cpp` и
`runtime_memory_tests.cpp` и `task_scope_tests.cpp`; системный backend дополнительно используется в
allocation-failure и sanitizer сборках.
