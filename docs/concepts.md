# Static API contracts

Finish the public and internal generic API by describing its compile-time
contracts with concepts, semantic traits, and explicit type roles.

The core rule is:

> Do not duplicate structural information that the compiler can derive from
> valid expressions. Use explicit metadata only for semantic properties that
> cannot be inferred from shape.

## Three kinds of static information

### 1. Structural shape

Use concepts / requires-expressions for properties visible from the interface:

- does `pop_batch` exist?
- what does it return?
- can the object be moved?
- is that move `noexcept`?
- does a queue expose the required task type?
- can a policy perform a particular operation?

Example:

```cpp
template<class Q>
concept TaskQueue =
    requires(Q& q, ScheduledTask* task) {
        { q.try_push(task) } -> std::same_as<bool>;
        { q.try_pop() } -> std::same_as<ScheduledTask*>;
    };

template<class Q>
concept BatchPushQueue =
    TaskQueue<Q> &&
    requires(Q& q, std::span<ScheduledTask*> tasks) {
        { q.try_push_batch(tasks) } -> std::same_as<std::size_t>;
    };
```

No semantic trait should duplicate information already implied by these
expressions.

### 2. Semantic properties

Use traits for contracts that cannot be inferred from interface shape:

- is publication single-producer?
- is consumption single-consumer?
- is the queue bounded?
- is stealing safe concurrently with owner operations?
- does storage preserve object addresses?
- does an operation require owner-thread access?

Example:

```cpp
template<class Q>
struct queue_traits {
    static constexpr bool single_producer = false;
    static constexpr bool single_consumer = false;
    static constexpr bool bounded = false;
    static constexpr bool concurrent_steal_safe = false;
    static constexpr bool stable_storage = false;
};
```

Concepts may then combine structural and semantic requirements:

```cpp
template<class Q>
concept OwnerLocalQueue =
    TaskQueue<Q> &&
    queue_traits<Q>::single_producer;

template<class Q>
concept StealableQueue =
    TaskQueue<Q> &&
    requires(Q& q) {
        { q.try_steal() } -> std::same_as<ScheduledTask*>;
    } &&
    queue_traits<Q>::concurrent_steal_safe;

template<class Q>
concept BatchOwnerQueue =
    OwnerLocalQueue<Q> &&
    BatchPushQueue<Q>;
```

### 3. Type roles and authority

Do not encode dynamic authority purely as boolean traits when the authority
belongs to a value rather than to the whole type.

Examples include:

- queued task vs executable task;
- completion source vs completion observer;
- publication reservation;
- destroy authority;
- storage ownership.

Prefer distinct role/capability types where that distinction prevents invalid
operations from being expressible.

## Candidate internal concepts

Potential generic boundaries include:

- `ExternalSubmissionPolicy`
- `LocalQueue`
- `StealableQueue`
- `BatchPublisher`
- `WakePolicy`
- `ShardSelector`
- allocator/storage policies

Potential role types or concepts, depending on whether multiple
implementations actually exist:

- `RunnableTask`
- `CompletionSource`
- `CompletionObserver`

Concepts should be introduced where they constrain an actual generic boundary,
rather than only to classify an otherwise concrete type.

## Semantic models vs boolean traits

Avoid turning semantic traits into a large bag of independent booleans when the
properties form mutually exclusive or protocol-level states.

Instead of:

```cpp
template<class Q>
struct queue_traits {
    static constexpr bool single_producer = true;
    static constexpr bool multi_producer = false;
    static constexpr bool single_consumer = false;
    static constexpr bool multi_consumer = true;
};
```

prefer explicit models:

```cpp
enum class producer_model {
    single,
    multi
};

enum class consumer_model {
    single,
    multi
};

template<class Q>
struct queue_traits {
    static constexpr auto producers = producer_model::single;
    static constexpr auto consumers = consumer_model::multi;
};
```

For asymmetric queue protocols, an even stronger model may be more useful:

```cpp
enum class queue_access_model {
    spsc,
    spmc,
    mpsc,
    mpmc,
    owner_stealable
};
```

For example, a Chase-Lev-style queue is more accurately described as
`owner_stealable` than as a random collection of flags such as
`single_producer=true` and `supports_steal=true`.

## Layering rule

Use the following split:

```text
requires-expression
    -> syntactic / structural capability

trait or semantic model
    -> static semantic protocol

role type
    -> authority

runtime invariant
    -> temporal / concurrent correctness
```

Concepts should not be used to pretend that temporal concurrency protocols are
statically proven when they are not.

## Open questions

- Which semantic properties should be booleans and which should instead use an
  enum/model type?
- Which authority transitions deserve distinct types?
- Which internal components are actually intended to be policy-replaceable?
- Where can semantic traits be derived from stronger existing type roles rather
  than declared independently?
- Which concepts belong to the public API and which should remain internal
  implementation contracts?

Need to finish api by making concept s for api shape for internals and api

есть метод pop_batch?                → concept/requires
возвращает ли он size_t?             → concept/requires
move noexcept?                       → type trait / concept

один ли producer по контракту?       → semantic trait
можно ли stealing concurrent?        → semantic trait
сохраняются ли addresses?            → semantic trait
кто имеет destroy authority?         → semantic trait/type role


by don't need dumplicate describe shape which compiler can deduce from code, but need to describe semantic traits and type roles which compiler can't deduce from code(or for harder contraits mabe could do that but need to think about that)


and also for internals need we can introduce some concepts for internal types, like:
RunnableTask
CompletionSource
CompletionObserver
ExternalSubmissionPolicy
LocalQueue
StealableQueue
BatchPublisher
WakePolicy
ShardSelector
template<class Q>
concept OwnerLocalQueue =
    TaskQueue<Q> &&
    queue_traits<Q>::single_producer;

template<class Q>
concept StealableQueue =
    TaskQueue<Q> &&
    queue_traits<Q>::supports_steal;

template<class Q>
concept BatchOwnerQueue =
    OwnerLocalQueue<Q> &&
    BatchPushQueue<Q>;

    template<class T>
struct queue_traits {
    static constexpr bool single_producer = false;
    static constexpr bool single_consumer = false;
    static constexpr bool bounded = false;
    static constexpr bool supports_steal = false;
    static constexpr bool stable_storage = false;
};

template<class Q>
concept TaskQueue =
    requires(Q& q, ScheduledTask* task) {
        { q.try_push(task) } -> std::same_as<bool>;
        { q.try_pop() } -> std::same_as<ScheduledTask*>;
    };

template<class Q>
concept BatchPushQueue =
    TaskQueue<Q> &&
    requires(Q& q, std::span<ScheduledTask*> tasks) {
        { q.try_push_batch(tasks) } -> std::same_as<std::size_t>;
    };