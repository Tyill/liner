# Старт хранилища и семантика сбоев

## Клиент vs внутренние задачи

Путь **клиента** обрабатывает ошибки store без паники: операции с бэкендом могут завершаться неуспехом (таймауты Redis, `SQLITE_BUSY`, I/O). Это **`false` / `NULL`** плюс **`lnr_last_error_code`** (`LNR_ERR_STORE`, …) и stderr / опциональный log hook.

**Listener** и **sender** используют **тот же** handle store клиента (`Arc<Mutex<dyn Store>>`) — при `run` **второй** независимый `open_store` **не** открывается.

## Старт listener внутри `run`

После TCP **bind** и **regist_topic** в каталоге `run` создаёт **listener** (mio poll / register / waker и `get_topic_key` для source topic). При сбое этого старта **`run` возвращает `false`** и ставит **`LNR_ERR_STARTUP`**. Процесс на этом пути **не** паникует. Частичное состояние откатывается (unregister топика при необходимости; listener/sender не остаются running).

Сбои resolve/bind TCP — **`LNR_ERR_BIND`**. Ошибки store на regist — **`LNR_ERR_STORE`**.

## Операционный вывод

- Текущие операции клиента: коды ошибок + stderr/log hook.
- Сбой старта listener после успешного create: **`run` → false + `LNR_ERR_STARTUP`**, не abort процесса.

Конструктор sender не использует прежний fail-fast `expect` по store.
