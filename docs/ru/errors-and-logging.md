# Ошибки и логирование

## Куда пишутся сообщения

Сбои в ядре на Rust часто логируются макросом `print_error!`: строка вида:

`Error <file>:<line>: <message>`

По умолчанию она идёт в **stderr**. Опционально — **процессно-глобальный** log hook: **`lnr_set_log_cb`** / Python **`set_log_callback`** (`NULL` / `None` возвращает stderr). Hook не привязан к клиенту.

Синхронные вызовы API также выставляют **код последней ошибки** на клиенте (`lnr_last_error_code` / `Client::last_error`). Публичного API строки сообщения об ошибке **нет** — текст смотрите в stderr или в log hook.

## Коды sync-ошибок (`LNR_OK` / `LNR_ERR_*`)

| Код | Имя | Типичный случай |
|------|------|----------------|
| 0 | `LNR_OK` | успех / сброс (включая идемпотентный `run` при уже running) |
| 1 | `LNR_ERR_NOT_RUNNING` | send до `run` / после `stop` |
| 2 | `LNR_ERR_ALREADY_RUNNING` | `set_advertise_addr` во время running |
| 3 | `LNR_ERR_SELF_TOPIC` | send/sub на свой топик |
| 4 | `LNR_ERR_INTERNAL_TOPIC` | sub/unsub `__#internal_channel` |
| 5 | `LNR_ERR_NO_ADDR` | у топика нет адресов |
| 6 | `LNR_ERR_BIND` | ошибка resolve/bind |
| 7 | `LNR_ERR_STORE` | сбой Redis/SQLite/Postgres |
| 8 | `LNR_ERR_INVALID_ARG` | некорректный advertise |
| 9 | `LNR_ERR_CLEAR_WHILE_RUNNING` | `clear_*` во время running |
| 10 | `LNR_ERR_STARTUP` | старт listener после TCP bind (mio / topic_key) |

## C API (`include/liner.h`)

| Ситуация | Типичный результат |
|----------|---------------------|
| Неверный дескриптор клиента (`NULL`) в любой функции с `lnr_hClient` | `FALSE` / `0`; возможна запись `client was not created` |
| `lnr_new_client_redis` / `lnr_new_client` | `NULL` при сбое: null/некорректные UTF-8 указатели, пустые `unique_name`, `topic`, `localhost` или строка хранилища, либо **хранилище не открылось** |
| `lnr_new_client_sqlite` | `NULL` при тех же правилах, **ошибке открытия SQLite**, **некорректном непустом `receivers_json`** или ошибках **`seed_receivers`**. Пустой/`[]` receivers — не ошибка |
| `lnr_new_client_postgres` | `NULL` при сбое (нужна фича **`postgres`**) |
| `lnr_run` | `TRUE` при старте или **уже running** (`LNR_OK`); `FALSE` при bind/регистрации (`LNR_ERR_BIND` / `LNR_ERR_STORE`); старт listener после bind → `FALSE` + **`LNR_ERR_STARTUP`** (без паники; см. [store-startup-failure-semantics.md](store-startup-failure-semantics.md)) |
| `lnr_stop` | `TRUE` (идемпотентно); очищает `published_addr`, сохраняет `bound_listen_addr` |
| `lnr_set_advertise_addr` | `TRUE` до `run`; `FALSE` + `LNR_ERR_ALREADY_RUNNING` во время running |
| `lnr_last_error_code` | `LNR_OK` / `LNR_ERR_*` для последнего sync-вызова; для null handle — `LNR_OK` |
| `lnr_set_log_cb` | Всегда `TRUE`; ставит или сбрасывает (`cb == NULL`) глобальный sink ошибок |
| `lnr_list_addresses` | `TRUE` + ноль или более вызовов `lnr_addr_cb` (пустой топик ⇒ без колбэков); `FALSE` + `LNR_ERR_STORE` при ошибке БД |
| `lnr_pending_count` | Глубина офлайн-очереди этого sender; `0` если пусто; `-1` при ошибке |
| `lnr_set_max_message_size` / `lnr_set_compress_threshold` | Процессно-глобально; `FALSE` при `bytes == 0`; лучше до `run` |
| `lnr_set_status_cb` | `TRUE` при валидном handle; `FALSE` при null/неизвестном |
| `lnr_send_to`, `lnr_send_all`, … | `FALSE` при ошибках; смотрите **`lnr_last_error_code`**; детали — [using-the-api.md](using-the-api.md) |

### Синхронный возврат vs status callback

| Что | Куда попадает |
|-----|----------------|
| Create / `run` / `send_*` / валидация subscribe | Сразу **`NULL` / `false`** + **`lnr_last_error_code`** (+ часто stderr / log hook) |
| Peer connect/disconnect/sub/unsub (только связанные топики) | Status callback `LNR_PEER_*` |
| Сбой TCP connect / закрытие потока / flush (**sender**) | Status callback `LNR_SENDER_ROUTE_LOST` / `LNR_SENDER_SEND_ERROR` (+ stderr / log hook) |
| Фоновые ошибки хранилища на reconnect/persist (**sender**) | Status callback `LNR_SENDER_STORE_ERROR` (+ stderr / log hook) |
| Фоновые ошибки хранилища на ack/lookup (**listener**) | Status callback `LNR_LISTENER_STORE_ERROR` (+ stderr / log hook) |

См. [using-the-api.md](using-the-api.md) (*Status / background-error callback*).

## Rust `Client`

| API | Успех | Сбой |
|-----|---------|---------|
| `Client::new_*` | `Some(Client)` | `None` (см. EN-док для нюансов stderr) |
| `run` | `true` при старте или уже running (`ErrorCode::Ok`) | `false` + `Bind` / `Store` / **`Startup`** / … |
| `stop` | `true` (идемпотентно) | — |
| `set_advertise_addr` | `true` вне running | `false` + `AlreadyRunning` / `InvalidArg` |
| `list_addresses` / `pending_count` | `Some(...)` (пустой список / `0` — успех) | `None` + `Store` |
| `send_to` / `send_all` / sub / refresh / clear | `true` | `false` + соответствующий `ErrorCode` |

## Rust `Liner`

При `NULL` от C-конструктора обёртка **`panic!`**. Для не-паникующего создания предпочитайте `Client::new_*`.

## Итог для production

1. Считайте **`NULL` / `None` / `FALSE`** нормальными режимами сбоя; смотрите **`lnr_last_error_code`** и **stderr** (или log hook). По желанию — **`lnr_set_status_cb`**.
2. **`lnr_run` → FALSE + `LNR_ERR_STARTUP`** — сбой старта listener после bind, не abort процесса.
3. Для контроля ошибок создания используйте **`Client::new_*`**, а не `Liner::new*`.
