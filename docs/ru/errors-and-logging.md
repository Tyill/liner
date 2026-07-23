# Ошибки и логирование

## Куда пишутся сообщения

Сбои в ядре на Rust часто логируются макросом `print_error!`: в **стандартный поток ошибок** выводится строка вида:

`Error <file>:<line>: <message>`

Отдельного перечисления кодов ошибок для C, кроме **успех/неуспех** на каждом вызове, нет. Подробности — в stderr (или оберните библиотеку и перехватывайте stderr в тестах).

## C API (`include/liner.h`)

| Ситуация | Типичный результат |
|----------|---------------------|
| Неверный дескриптор клиента (`NULL`) в любой функции с `lnr_hClient` | `FALSE` / `0`; возможна запись `client was not created` |
| `lnr_new_client_redis` / `lnr_new_client` | `NULL` при сбое: null/некорректные UTF-8 указатели, пустые `unique_name`, `topic`, `localhost` или строка хранилища, либо **хранилище не открылось** (Redis недоступен и т.д.) |
| `lnr_new_client_sqlite` | `NULL` при тех же правилах для указателей/пустых строк, **ошибке открытия SQLite**, **некорректном непустом `receivers_json`** или ошибках **`seed_receivers`** / БД. **`NULL` или пустой `receivers_json`**, либо JSON **`[]`** — **не** ошибка (без сидирования). |
| `lnr_new_client_postgres` | `NULL` при сбое (нужна сборка с фичей **`postgres`**): указатели, пустые строки, ошибка подключения к PostgreSQL |
| `lnr_run` | `TRUE`, если клиент уже помечен как running; `FALSE`, если регистрация или bind не удались (см. ниже); **возможна паника**, если внутренний старт listener/sender по хранилищу падает (см. [store-startup-failure-semantics.md](store-startup-failure-semantics.md)) |
| `lnr_set_status_cb` | `TRUE`, если дескриптор клиента валиден; `FALSE` при null/неизвестном handle. Регистрирует или снимает (`cb == NULL`) status-колбэк |
| `lnr_send_to`, `lnr_send_all`, … | `FALSE` при логических или I/O ошибках; отдельные операции — в [using-the-api.md](using-the-api.md) |

### Синхронный возврат vs status callback

| Что | Куда попадает |
|-----|----------------|
| Create / `run` / `send_*` / валидация subscribe | Сразу **`NULL` / `false`** (+ часто stderr) |
| Peer connect/disconnect/sub/unsub (только связанные топики) | Status callback `LNR_PEER_*` |
| Сбой TCP connect / закрытие потока / flush (**sender**) | Status callback `LNR_SENDER_ROUTE_LOST` / `LNR_SENDER_SEND_ERROR` (+ stderr) |
| Фоновые ошибки хранилища на reconnect/persist (**sender**) | Status callback `LNR_SENDER_STORE_ERROR` (+ stderr) |
| Фоновые ошибки хранилища на ack/lookup (**listener**) | Status callback `LNR_LISTENER_STORE_ERROR` (+ stderr) |

См. [using-the-api.md](using-the-api.md) (*Колбэк статусов / фоновых ошибок*) про kinds и фильтр связанных топиков.

Вспомогательные функции создания проверяют указатели и C-строки; при неверном вводе возвращают `NULL` без обязательной печати в каждом случае.

## Rust `Client` (`liner_broker::client::Client`)

| API | Успех | Неудача |
|-----|--------|---------|
| `Client::new_redis` / `Client::new` | `Some(Client)` | `None`, если хранилище не открылось — **без** `print_error!` с этого пути; проверяйте `None` |
| `Client::new_sqlite` | `Some(Client)` | `None`, если хранилище не открылось (тихо), **JSON `receivers_json` не разобрался** как массив записей сидирования (логи), **`seed_receivers`** упал (логи), либо (для Rust `&str`) некорректный UTF-8 |
| `Client::new_postgres` | `Some(Client)` | `None`, если PostgreSQL не открылся (нужна фича **`postgres`** при сборке) |
| `run` | `true`, если цикл событий может стартовать | `false`, если `regist_topic` не удался, `localhost` не резолвится или TCP bind не удался; причина в логах. `true`, если клиент **уже** был в running (идемпотентный успех) |
| `set_status_cb` | для живого клиента всегда успех (регистрация или снятие) | N/A (неверный handle только через C `lnr_set_status_cb`) |
| `send_to` / `send_all` | `true`, если путь отправки сообщил об успехе | `false`, если не running, свой топик, неизвестные адреса топика или сбой sender |
| `subscribe` / `unsubscribe` | `true` | `false` при ошибках хранилища или неверном топике |
| `refresh_address_topic` | `true`, если адреса найдены | `false`, если ничего нет или ошибка хранилища |
| `clear_stored_messages` / `clear_addresses_of_topic` | `true` только когда клиент **не** в running | `false`, если уже running или ошибка хранилища |

Внутренние ошибки хранилища оборачиваются в `DbError` (строка от Redis или SQLite / `rusqlite`). Они дают `false` / неуспех там, где клиент проверяет `Result`; в слое клиента они **не** паникуют автоматически.

## Обёртка Rust `Liner` (`liner_broker::Liner`)

`Liner::new` / `Liner::new_sqlite` / `Liner::new_postgres` используют C-конструкторы. Если вернулся null-дескриптор, обёртка делает **`panic!`** (`error create client`). Также используется `CString::new(...).unwrap()` — строки со **встроенным NUL** вызовут панику. Для неконструирующего создания предпочтительнее напрямую **`Client`**.

## Отравление Mutex

В нескольких местах используется `Mutex::lock().unwrap()` на внутреннем mutex клиента. Если другой поток паникует, удерживая lock, последующие операции могут **паниковать** с ошибкой poison. Это не связано с «занятостью» Redis/SQLite; это признак более ранней паники в процессе.

## Кратко для продакшена

1. Считайте **`NULL` / `None` / `FALSE`** нормальными режимами отказа; контекст — в **stderr**. По желанию зарегистрируйте **`lnr_set_status_cb`** для peer- и фоновых operational-событий.
2. Не полагайте, что `lnr_run` == `TRUE` гарантирует отсутствие аварийного завершения позже — потоки listener/sender всё ещё могут паниковать при неожиданном сбое хранилища на их старте (отдельно задокументировано).
3. Для максимального контроля над ошибками создания в Rust используйте **`Client::new_*`**, а не `Liner::new` / `Liner::new_sqlite`.
