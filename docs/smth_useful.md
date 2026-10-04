* Pool destruction = stop+join, а не drain.
* Quiescence как внешний precondition для уничтожения Pool.
* wait() на worker = cooperative execution/reentrancy.
* outstanding_ ≠ Handle::Counter.
* Порядок fn.reset → slot release → Counter::complete → outstanding--.
* Один текущий custodian у ScheduledTask.
* Local enqueue ownership transfer.
* Central enqueue ownership transfer.
* External submit при saturation ждёт, worker submit может inline.
* Chase–Lev: owner bottom / thief top.
* Steal batch должен быть опубликован до запуска первого украденного user code.
* Central Vyukov queue: publication/consumption и snapshot semantics.
* drain() ограничен local.free_capacity().
* Failure local.try_push() после рассчитанного capacity = invariant violation.
* Per-worker local queues.
* Central shards.
* affinity = placement hint, не consumption ownership.
* home shard ≠ exclusive shard.
* Shards > workers всё равно должны обслуживаться.
* External round-robin / next_external_shard.
* Self-replenishing local deque может starve external work.
* Поэтому periodic direct external polling.
* High/Normal priority semantics и возможная starvation Normal.
* Порядок local / home drain / steal / foreign drain — сам является policy.
* Idle acquire path O(workers + shards).
* Per-thread allocation locality расходится с execution locality после steal.
* Storage origin ≠ execution custodian.
* Remote return/free protocol.
* Runtime slab reclamation отдельно от slot reuse.
* Persistent stable slot storage / incarnation reuse.
* Completion counter = credits, не количество tasks.
* Каждый acquired credit должен быть погашен ровно один раз.
* Terminal zero запрещает появление новых credits.
* Любой spawner должен сам находиться под существующим credit.
* combine() строит completion DAG без scheduler task.
* Sentinel в combine.
* Sentinel в for_each.
* Partial publication failure в for_each.
* for_each callable может реально исполняться concurrent.
* for_each_ws recursive spawn-credit protocol.
* Persistent TaskGraph definition ≠ per-run state.
* RunState storage lifetime ≠ authority lifetime доступа к Graph.
* Token identity ≠ lane identity ≠ scheduler packet identity.
* next/token claim uniqueness.
* lanes как число logical execution paths.
* Последняя lane делает node barrier и releases successors.
* Successor activation только на predecessor transition 1 → 0.
* Bypass переносит тот же logical execution path без нового packet semantics.
* Budget yield: lane переживает конкретный ScheduledTask.
* Graph completion zero = больше никогда не будет graph-definition access.
* При этом wrapper/RunState storage может ещё физически доживать.
* Cancellation cooperative, а не immediate kill.
* Exception → error + cancel, explicit cancel ≠ exception.
* Persistent node callable может быть вызван несколькими lanes concurrent.
* Graph reusable после completed run.
* Старый completed RunState может ещё физически существовать рядом с новым.
* Structural graph mutation только после соответствующей quiescence.
* Graph destructor на worker может сам cooperative-wait'ить.
* GraphScope destructor может запускать построенный граф; TaskScope публикует работу сразу при spawn и в destructor только закрывает admission и ждёт.
* ran_ — bookkeeping obligation, а не просто «run сейчас выполняется».
* dirty_ участвует в destructor/run semantics.
* Bare NodeId/JobHandle не несёт graph identity/generation.
* Handle владеет completion state, но не work.
* shared_ptr<RunState> сейчас bridge/lifetime crutch для API wrapper.
* Wrapper вообще нужен, потому что logical graph execution ≠ physical Pool packet.
* API run() -> Handle требует отдельной operation/completion entity.
* Arena backing lifetime ≠ object lifetime.
* Arena не должна диктовать lifetime Graph/Run/Task/Counter.
* Если убрать shared_ptr, object lifetime bookkeeping никуда не исчезает.
* Full allocator требует stable slots + construction/destruction + remote free + reuse.
* Per-thread pools + stealing делают allocator частью scheduling economics.
