# Ошибки и логирование

Этот документ объясняет, как liner сообщает о сбоях: текст в stderr / log hook, коды sync-ошибок на клиенте, возвращаемые значения C и Rust, и как они соотносятся с опциональным status callback.

Жизненный цикл и порядок вызовов — в [using-the-api.md](using-the-api.md). Сбои старта listener внутри `run` — в [store-startup-failure-semantics.md](store-startup-failure-semantics.md).

---

## Куда пишутся человекочитаемые сообщения

Сбои в ядре на Rust часто логируются макросом `print_error!`. Строка выглядит так:

```text
Error <file>:<line>: <message>
```

**Сток по умолчанию:** стандартный поток ошибок (`eprintln!`).

**Опциональный сток:** **процессно-глобальный** log hook.

| Привязка | Установка / сброс |
|----------|-------------------|
| C | `lnr_set_log_cb(lnr_log_cb cb, lnr_uData)` — `cb == NULL` возвращает stderr |
| Rust | `liner_broker::set_log_cb` / `Liner::set_log_callback` |
| Python | `set_log_callback` |

Замечания:

- Hook **один на процесс**, не на клиент. Демоны обычно ставят его один раз при старте.
- Повторный вызов `lnr_set_log_cb` **заменяет** предыдущий колбэк и userdata.
- `lnr_set_log_cb` всегда возвращает **`TRUE`** при корректном вызове (установка или сброс).
- Публичного API, который возвращает строку ошибки вызывающему коду, **нет**. Текст смотрите в stderr или в log hook; стабильный машинный код — через **`lnr_last_error_code`**.

---

## Коды sync-ошибок (`LNR_OK` / `LNR_ERR_*`)

Каждый клиент хранит **код последней ошибки**, который обновляют синхронные вызовы API. Успешные вызовы ставят **`LNR_OK`**.

| Код | Имя | Типичный случай |
|------|------|-----------------|
| 0 | `LNR_OK` | Успех или сброс после успешного вызова. Также выставляется идемпотентным **`run`** при уже running. |
| 1 | `LNR_ERR_NOT_RUNNING` | `send_to` / `send_all` до `run` или после `stop`, пока клиент не running. |
| 2 | `LNR_ERR_ALREADY_RUNNING` | `set_advertise_addr` во время running. |
| 3 | `LNR_ERR_SELF_TOPIC` | Send или subscribe/unsubscribe на собственный исходный топик. |
| 4 | `LNR_ERR_INTERNAL_TOPIC` | Публичный subscribe/unsubscribe `__#internal_channel`. |
| 5 | `LNR_ERR_NO_ADDR` | У целевого топика нет адресов в кэше/store. |
| 6 | `LNR_ERR_BIND` | Не удалось разрешить строку bind или выполнить TCP `bind`. |
| 7 | `LNR_ERR_STORE` | Сбой операции Redis / SQLite / PostgreSQL. |
| 8 | `LNR_ERR_INVALID_ARG` | Некорректный advertise-адрес; пустой send payload; или send с payload, у которого несжатое кадрированное тело превысило бы `max_message_size` |
| 9 | `LNR_ERR_CLEAR_WHILE_RUNNING` | `clear_stored_messages` / `clear_addresses_of_topic` во время running. |
| 10 | `LNR_ERR_STARTUP` | Сбой старта listener после TCP bind и регистрации в каталоге (mio poll/register/waker или `get_topic_key`). |
| 11 | `LNR_ERR_BUSY` | In-memory очередь sender на пира заполнена (`max_send_queue`). |

**Доступ**

- C: `int lnr_last_error_code(lnr_hClient client)` — для null handle возвращает `LNR_OK`
- Rust: `Client::last_error() -> ErrorCode`
- Python: `last_error_code() -> int`

---

## Исходы C API (`include/liner.h`)

| Ситуация | Типичный результат |
|----------|---------------------|
| Неверный дескриптор клиента (`NULL`) в любой функции с `lnr_hClient` | `FALSE` / `0` / `NULL` по смыслу функции; возможна запись `client was not created` |
| `lnr_new_client_redis` / `lnr_new_client` | `NULL` при сбое: null/некорректные UTF-8 указатели, пустые `unique_name`, `topic`, `localhost` или строка хранилища, либо **хранилище не открылось** (Redis недоступен и т.п.) |
| `lnr_new_client_sqlite` | `NULL` при тех же правилах для указателей/пустых строк, **ошибке открытия SQLite**, **некорректном непустом `receivers_json`** или ошибках **`seed_receivers`** / БД. **`NULL` или пустой `receivers_json`**, либо JSON **`[]`**, — **не** ошибка (сидинга нет). |
| `lnr_new_client_postgres` | `NULL` при сбое (нужна сборка с фичей **`postgres`**): null/некорректные указатели, пустые строки или ошибки **подключения / схемы PostgreSQL** |
| `lnr_run` | `TRUE`, если старт прошёл или клиент уже running (`LNR_OK`). `FALSE` + `LNR_ERR_BIND` / `LNR_ERR_STORE` при сбое bind или регистрации. `FALSE` + **`LNR_ERR_STARTUP`**, если после bind не удалось поднять listener (без паники). |
| `lnr_stop` | `TRUE` (идемпотентно). Очищает `published_addr`, сохраняет `bound_listen_addr`. Ставит `LNR_OK`. |
| `lnr_set_advertise_addr` | `TRUE` до `run` (включая сброс через `NULL`/`""`). `FALSE` + `LNR_ERR_ALREADY_RUNNING` во время running; `FALSE` + `LNR_ERR_INVALID_ARG` при плохом адресе. |
| `lnr_last_error_code` | Код последнего sync-вызова; для null handle — `LNR_OK`. |
| `lnr_set_log_cb` | Всегда `TRUE`; ставит или сбрасывает (`cb == NULL`) глобальный sink ошибок. |
| `lnr_list_addresses` | `TRUE` и ноль или более вызовов `lnr_addr_cb` (пустой топик ⇒ без колбэков). `FALSE` + `LNR_ERR_STORE` при ошибке БД. |
| `lnr_pending_count` | Неотрицательная глубина офлайн-блобов этого sender; `0` если пусто; `-1` при ошибке (тогда смотрите `lnr_last_error_code`). |
| `lnr_set_max_message_size` / `lnr_set_compress_threshold` | Процессно-глобально. `FALSE` при `bytes == 0`. Лучше задавать до `run`. |
| `lnr_set_status_cb` | `TRUE` при валидном handle; `FALSE` при null/неизвестном. Регистрирует или снимает (`cb == NULL`) status callback. |
| `lnr_send_to`, `lnr_send_all`, subscribe, refresh, clear, … | `FALSE` при логических или I/O ошибках; смотрите **`lnr_last_error_code`** и stderr/log hook. |

### Синхронный возврат vs status callback

Эти каналы отвечают на разные вопросы:

| Что | Куда попадает |
|-----|----------------|
| Create / `run` / `send_*` / валидация subscribe / clear / advertise | Сразу **`NULL` / `FALSE`** + **`lnr_last_error_code`**, часто плюс stderr / log hook |
| Peer connect / disconnect / subscribe / unsubscribe (только связанные топики) | Status callback `LNR_PEER_*` |
| Сбой TCP connect / закрытие потока / flush (**sender**) | Status callback `LNR_SENDER_ROUTE_LOST` / `LNR_SENDER_SEND_ERROR`, плюс stderr / log hook |
| Фоновые ошибки хранилища на reconnect/persist (**sender**) | Status callback `LNR_SENDER_STORE_ERROR`, плюс stderr / log hook |
| Фоновые ошибки хранилища на ack/lookup (**listener**) | Status callback `LNR_LISTENER_STORE_ERROR`, плюс stderr / log hook |

Status callback **не** заменяет sync-коды возврата. Виды событий и фильтр связанных топиков — в [using-the-api.md](using-the-api.md) (*Колбэк статусов / фоновых ошибок*).

---

## Rust `Client` (`liner_broker::client::Client`)

| API | Успех | Сбой |
|-----|---------|---------|
| `Client::new_redis` / `Client::new` | `Some(Client)` | `None`, если store не открылся — **молча** (без `print_error!` на этом пути); проверяйте `None` |
| `Client::new_sqlite` | `Some(Client)` | `None`, если store не открылся (молча), **`receivers_json` нельзя разобрать** как JSON-массив seed-записей (пишет в лог), **`seed_receivers`** падает (пишет в лог); некорректный UTF-8 у Rust-`&str` на практике не возникает |
| `Client::new_postgres` | `Some(Client)` | `None`, если PostgreSQL не открылся (нужна фича **`postgres`** на этапе компиляции) |
| `run` | `true`, если цикл событий стартовал, или уже running (`ErrorCode::Ok`) | `false` + `Bind` / `Store` / **`Startup`** / … |
| `stop` | `true` (идемпотентно) | N/A для живого клиента |
| `set_advertise_addr` | `true` вне running | `false` + `AlreadyRunning` / `InvalidArg` |
| `set_status_cb` | для живого клиента всегда успешен (регистрация или сброс) | N/A (невалидный handle только через C `lnr_set_status_cb`) |
| `list_addresses` | `Some(rows)`, в том числе пустой | `None` + `Store` |
| `pending_count` | `Some(n)` (`0`, если пусто) | `None` + `Store` |
| `send_to` / `send_all` | `true`, если путь отправки сообщил успех | `false` + last error (`NotRunning`, `SelfTopic`, `InvalidArg` если payload превышает max framed size, `NoAddr`, `Store`, …) |
| `subscribe` / `unsubscribe` | `true` | `false` + last error |
| `refresh_address_topic` | `true`, если адреса найдены | `false` + `NoAddr` / `Store` |
| `clear_stored_messages` / `clear_addresses_of_topic` | `true` только когда клиент **не** running | `false` + `ClearWhileRunning` или `Store` |

Внутренние ошибки store оборачиваются в `DbError` (строка из Redis или SQLite / `rusqlite`). Там, где клиент проверяет `Result`, они превращаются в `false` / неуспех и **не** паникуют сами по себе в клиентском слое.

---

## Rust-обёртка `Liner` (`liner_broker::Liner`)

`Liner::new` / `Liner::new_sqlite` / `Liner::new_postgres` используют C-конструкторы. Если вернулся null handle, обёртка делает **`panic!`** (`error create client`). Также используется `CString::new(...).unwrap()` — строки со **встроенным NUL**-байтом вызовут панику.

Если нужно непаникующее создание и типизированный `ErrorCode` после sync-вызовов, предпочитайте напрямую **`Client::new_*`**.

---

## Отравление Mutex

На нескольких путях используется `Mutex::lock().unwrap()` на внутреннем mutex клиента. Если другой поток запаниковал, держа этот lock, последующие операции могут **запаниковать** с poison error. Это не связано с «занятостью» Redis/SQLite; это признак более ранней паники в процессе.

---

## Итог для production

1. Считайте **`NULL` / `None` / `FALSE`** нормальными режимами сбоя. Читайте **`lnr_last_error_code`** для машинного кода и **stderr** (или log hook) для детальной строки. По желанию регистрируйте **`lnr_set_status_cb`** для peer-событий и фоновых операционных ошибок.
2. **`lnr_run`**, вернувший `FALSE` с **`LNR_ERR_STARTUP`**, означает сбой настройки listener после bind — процесс жив; исправьте конфигурацию / store / ресурсы и повторите.
3. Для максимального контроля над ошибками создания используйте **`Client::new_*` в Rust**, а не `Liner::new` / `Liner::new_sqlite`.
