# Overflow и завершение Pool

## Admission и shutdown

`close()` необратимо закрывает **внешнюю** публикацию. Уже принятый publisher
заканчивает публикацию, включая retry при полной ingress. Задачи, исполняемые
worker этого Pool, могут продолжать submit/spawn — также из деструктора capture
или completion payload. Worker другого Pool считается внешним caller.

`shutdown()` и деструктор выполняют один порядок:

1. Закрыть внешнюю admission.
2. Дождаться принятых внешних publishers.
3. Дождаться physical idle: callbacks, descendants, packet/capture destruction,
   completion propagation и retirement.
4. Установить stop, разбудить и join'ить workers.
5. Только деструктор освобождает scheduler/accounting/parking storage.

`close()` можно вызывать из worker. `shutdown()` — только извне; собственный
worker получает `std::logic_error` **до** захвата shutdown mutex. Иначе внешний
shutdown, ожидающий этот worker, и сам worker могли бы заблокировать друг друга.
Деструктор Pool из его worker недопустим и приводит к terminate. Повторные и
конкурентные внешние shutdown безопасны; join сериализован. Задачи не отменяются,
ошибки остаются в Handles, detached exceptions по-прежнему отбрасываются.
Если callback не завершается или ждёт несостоявшейся внешней публикации, drain
тоже не завершится: shutdown не прерывает пользовательский код.

Admission linearizes в CAS перед accounting publication. Старший бит atomic
закрывает вход, остальные биты считают принятых внешних publishers. Release
последнего publisher и acquire в shutdown закрывают промежуток между принятием
и появлением packet в очереди. Одного `if (!closed)` недостаточно: shutdown мог
бы увидеть idle, остановить workers, а publisher затем положил бы задачу.
Stop нельзя выставлять до drain: workers нужны и для освобождения полной ingress,
и для descendants. На worker publication нет общего admission RMW — исполняемый
родитель остаётся outstanding до завершения публикации детей и всего epilogue.

Единица admission — packet либо подготовленная внешняя batch-группа до 64 tasks.
Callable конструируется до admission и может быть уничтожен при отказе. После
close внешняя публикация бросает `logic_error`; более ранний принятый prefix
batch/range/graph сохраняет свои обязательства lifetime. Для range существующий
rollback ждёт принятые обращения к range; graph записывает publication failure
в результат и отменяет ещё не начатые invocations. TaskScope publication failure
записывает ошибку, отменяет scope и пробрасывает исключение.

`wait_idle()` не закрывает admission и не join'ит workers. `shutdown()` ждёт
pool tasks, а не произвольные внешние completion credits: открытый sentinel
TaskScope пользователь закрывает через scope.close()/join(). Handles можно
сохранять после уничтожения Pool. Graphs/scopes и потоки, вызывающие методы Pool,
должны закончить доступ до уничтожения его storage. Конкурентный submit/close/
shutdown допустим, конкурентный доступ к уже уничтоженному объекту — нет.
Данные, заимствованные captures, должны пережить drain: порядок объявления
локальных переменных по-прежнему важен.

## Overflow

Bounded Chase–Lev и MPMC не менялись. Если worker не смог опубликовать в них,
он добавляет packet в FIFO выбранного shard/priority. Queue links защищены mutex,
пустая очередь проверяется atomic probe. При drain один lock выдаёт bounded
пачку в local deque; все siblings видны helping/thieves до запуска первого task.
Периодический shared probe извлекает один task. Это не lock-free очередь, но worker
никогда не ждёт освобождения **capacity**. Дополнительной аллокации после commit
accounting нет: link находится в ScheduledTask. Публикация использует обычный
wake того же shard, final scan перед парковкой проверяет и overflow.

Все workers и cooperative helpers видят эти очереди, включая пустые placement
domains. Shared acquisition чередует ingress/overflow, если обе дают работу;
периодический probe сохраняет обслуживание shared work при заполненной local
deque. Его предпочтение источника меняется раз в полный оборот shards, отдельно
от drain: переключение на каждом успешном probe могло бы вечно выбирать ingress
в shard A и overflow в shard B, не обслуживая overflow A. Priority остаётся предпочтением внутри tier, не глобальным строгим
порядком. После shared acquisition сохраняется wake relay.

Private immediate slot скрывал бы ребёнка от helper/thief, который ждёт его
completion. Inline execute увеличивал бы стек при рекурсивной публикации.
Отдельный allocating list мог бы бросить после accounting commit. Intrusive
queue избегает этих трёх проблем. Она не ограничивает общее количество
outstanding worker tasks: память всё ещё ограничивается приложением и allocator.
External publishers по-прежнему используют bounded ingress с backpressure.

Pop отсоединяет link под mutex до передачи executor. После destroy/release
packet либо последнего completion credit очередь и executor больше не обращаются
к нему. Это относится и к graph-owned InitialTask, переиспользуемым между runs.
Overflow не вызывает callable внутри submit. Явный nested wait всё ещё может
вкладывать helping callbacks в стек; произвольная глубина пользовательских waits
не становится stackless из-за этого изменения.

## Цена и проверки

На текущем 64-bit ABI link добавляет 8 B: ScheduledTask prefix 24 → 32 B,
graph InitialTask 40 → 48 B (без diagnostics). На 4096 graph nodes это ещё 32 KiB
retained storage. Дополнительных heap allocations для overflow нет. External
scalar admission добавляет успешный CAS и release fetch_sub; batch амортизирует
их на группу, worker publication не использует admission atomic.

`pool_lifecycle_tests` проверяет destructor drain, сохранение ошибки Handle,
закрытие с descendants, concurrent shutdown и producers, полную ingress во время
shutdown, child из capture destructor, ожидание его epilogue, helping overflow
другим worker, оба priority и пустой shard, graph packet reuse и TaskScope после
close. Queue tests проверяют отсутствие inline execution при saturation и ошибки
overflow tasks. Harness diagnostics для 65536 children/одного worker проверяют
48128 overflow publications/acquisitions и нулевой inline_execute. Отдельно
проверены 50000 звеньев spawn-цепочки без роста callback nesting, доступность
siblings после overflow batch-drain и отказ между подготовленными batch-группами.
Тест бесконечно пополняемых local/ingress/overflow очередей воспроизводит
голодание при прежнем выборе источника на каждом probe и проходит с ротацией.

Финальный код: Release **29/29**, ASan/UBSan/LSan **29/29**, TSan
**7/7** (lifecycle, pool queues, batch, topology, graph, scope, ownership).
Diagnostics main harness тоже проходит, включая точные overflow counters.

[Парное O3 сравнение](benchmarks/overflow-shutdown.md): пустой scalar external
detached на одном worker около +26%, насыщенный однопоточный overflow около +45%
к старому inline пути; external batch=16 около +1%. Корректность здесь имеет
измеримую стоимость, общего ускорения изменение не обещает.
