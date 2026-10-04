**## ADR: Default execution context and executor-independent public API

**Status:** Proposed

### Context

Текущий API заставляет пользователя явно создавать `Pool` и привязывать к нему higher-level abstractions даже тогда, когда пользователь не управляет scheduler topology:

```cpp
Pool pool{config};

TaskGraph graph{pool};
TaskScope scope{pool};
```

Это протаскивает внутреннюю execution machinery в обычный application API.

При этом сущности имеют разные семантические роли:

```text
TaskGraph
    описание computation

TaskScope
    lifetime / structured completion domain

Pool
    execution resource

ExecutionPolicy
    способ/ограничения исполнения
```

`TaskGraph` сам по себе не обязан знать, на каком pool он когда-нибудь будет исполнен. Аналогично `TaskScope` в обычном случае не должен требовать от пользователя ручного bootstrap scheduler'а.

Большинство пользователей ожидают zero-configuration путь:

```cpp
TaskScope scope;
scope.spawn(...);

TaskGraph graph;
...
graph.run();
```

Явное управление pool необходимо только при isolation, fixed worker count, affinity/pinning, benchmarking или иных специальных требованиях.

### Decision

DagFlow предоставляет **default execution context**, создаваемый лениво при первом использовании.

Обычные public APIs используют его автоматически:

```cpp
TaskScope scope;

scope.spawn([] {
    ...
});

scope.wait();
```

```cpp
TaskGraph graph;

auto a = graph.emplace(...);
auto b = graph.emplace(...);
graph.add_edge(a, b);

auto run = graph.run();
run.wait();
```

`TaskGraph` **не хранит ссылку на `Pool`** и не связывается с execution resource на этапе построения.

Explicit execution resource остаётся доступным:

```cpp
thread_pool pool{
    pool_config{
        .workers = 8,
        .pin_threads = true,
    }
};

graph.run(pool);
```

То же для scope:

```cpp
TaskScope scope{pool};
```

### Execution resource и execution policy разделяются

Execution resource отвечает на вопрос:

> **где существует capacity для исполнения?**

Например:

```cpp
thread_pool
default_executor
test_executor
inline_executor
```

Execution policy отвечает:

> **какими правилами пользоваться при конкретном исполнении?**

Например:

```cpp
execution_policy{
    .priority = Priority::High,
    .affinity = ...,
};
```

Policy не владеет threads/scheduler state и не создаёт execution resource.

Поэтому:

```cpp
graph.run(pool, policy);
```

означает:

> выполнить данный computation на этом resource с этими правилами.

А:

```cpp
graph.run(policy);
```

при наличии такого overload означает:

> выполнить на default executor с указанной policy.

### Public API должен иметь короткий default path

Предпочтительная форма API:

```cpp
graph.run();
graph.run(executor);
graph.run(executor, policy);

scope.spawn(fn);
scope.spawn(executor, fn);
scope.spawn(executor, policy, fn);
```

Но overload explosion не должен распространяться по всей библиотеке.

Где комбинаций становится много, используется options/policy object вместо добавления очередного positional overload.

Например лучше:

```cpp
scope.spawn(fn, {
    .priority = Priority::High,
});
```

чем постепенно прийти к:

```cpp
spawn(executor, priority, affinity, overflow, cancellation, fn);
```

### Generic execution integration

Execution resource определяется через concept, а не inheritance hierarchy:

```cpp
template<class E>
concept executor = requires(E& ex, task_packet* task) {
    dagflow::execute(ex, task);
};
```

Customization boundary может быть представлен CPO:

```cpp
dagflow::execute(executor, work);
```

Higher-level algorithms зависят от executor contract, а не от конкретного `thread_pool`.

Например:

```cpp
template<executor E>
Handle TaskGraph::run(E& executor);
```

`thread_pool` является одной из реализаций этого контракта, а не фундаментальным типом всего публичного API.

### Default runtime lifetime

Default execution context имеет process lifetime.

Он:

- лениво инициализируется;
- после инициализации не меняет фундаментальную scheduler topology;
- не требует явного shutdown от обычного пользователя;
- не является механизмом для workload-specific tuning.

Пользователь, которому необходимы deterministic construction/destruction, изоляция или специальная topology, создаёт explicit `thread_pool`.

То есть:

```cpp
parallel_for(...);
```

не должен внезапно требовать:

```cpp
initialize_runtime();
...
shutdown_runtime();
```

Иначе zero-config API опять исчез.

### Default configuration не является глобальным mutable knob

Не вводится API вида:

```cpp
set_global_worker_count(8);
set_global_affinity(...);
```

после начала работы runtime.

Это создаёт неявную глобальную mutable state и вопросы:

```text
когда изменение вступило в силу?
что с уже созданными workers?
что с выполняющимися scopes?
что если два компонента хотят разные настройки?
```

Для специальных настроек используется explicit execution resource.

### Graph definition не зависит от scheduler topology

После sealing graph содержит только computation-related representation:

```text
NodeDef[]
Edges[]
compiled options
```

Он не содержит:

```text
Pool*
worker id
shard id
local deque
parking state
```

Execution-specific mapping создаётся на run boundary.

Это позволяет один и тот же graph:

```cpp
graph.run();
graph.run(pool_a);
graph.run(pool_b);
```

при соблюдении правила одного активного run, если оно остаётся частью `TaskGraph` contract.

### `TaskScope` привязан к execution domain на lifetime scope

В отличие от `TaskGraph`, у `TaskScope` execution resource имеет смысл выбрать при construction:

```cpp
TaskScope scope;       // default executor
TaskScope scope{pool}; // explicit executor
```

После этого descendants scope используют тот же execution domain по умолчанию:

```cpp
scope.spawn(...);
```

Это сохраняет structured-concurrency invariant и не заставляет каждый child отдельно таскать executor.

Если когда-нибудь понадобится cross-executor spawn, это должна быть **явная отдельная операция**, а не случайный overload.

### Internal types не должны протекать наружу

Публичный API не должен требовать знания:

```text
Scheduler
Shard
LocalQueue
ParkingLot
CompletionCredit
ScheduledTask
runtime_memory
```

Они являются implementation machinery.

Пользователь работает с:

```text
TaskGraph
TaskScope
Handle
thread_pool
execution_policy
parallel_* algorithms
```

### Consequences

Плюсы:

```text
обычный API становится zero-config;
TaskGraph перестаёт быть искусственно связан с Pool;
можно менять scheduler internals без ломания graph API;
появляется естественная точка для custom executors;
benchmark/test executors становятся проще;
execution policy перестаёт смешиваться с ownership threads;
public surface становится TBB/STL-like.
```

Цена:

```text
появляется process-wide default runtime;
нужно чётко определить его lifetime/shutdown;
generic executor API требует concepts/CPO machinery;
нужно аккуратно избежать overload explosion;
TaskGraph run-state теперь обязан получать execution context на run boundary.
```

### Rejected alternatives

**Всегда требовать explicit `Pool`.**

Отвергается, потому что заставляет обычного пользователя конфигурировать implementation detail и делает trivial use cases неоправданно многословными.

**Хранить `Pool&` внутри `TaskGraph`.**

Отвергается, потому что computation description не зависит от конкретного execution resource и такая связь мешает reuse/testing/custom executors.

**Передавать только `ExecutionPolicy`, а pool создавать из неё автоматически.**

Отвергается, потому что policy и resource имеют разные lifetime и semantics. Это также легко приводит к созданию новых pools для отдельных операций.

**Глобально конфигурируемый singleton pool.**

Отвергается как основной tuning mechanism из-за mutable global state и конфликтующих требований разных компонентов.

---

 **API target**
```cpp
// 90% use case
dagflow::parallel_for(0uz, n, fn);

dagflow::TaskScope scope;
scope.spawn(a);
scope.spawn(b);
scope.wait();

dagflow::TaskGraph graph;
auto a = graph.emplace(foo);
auto b = graph.emplace(bar);
graph.add_edge(a, b);
graph.run().wait();


// Explicit control
dagflow::thread_pool pool{
    {.workers = 8}
};

dagflow::TaskScope isolated{pool};
graph.run(pool).wait();
```**





Need to finish api by making concept s for api shape for internals and api

есть метод pop_batch?                → concept/requires
возвращает ли он size_t?             → concept/requires
move noexcept?                       → type trait / concept

один ли producer по контракту?       → semantic trait
можно ли stealing concurrent?        → semantic trait
сохраняются ли addresses?            → semantic trait
кто имеет destroy authority?         → semantic trait/type role