# Указатель по устранению неполадок

Эта страница — **карта**: каждая строка указывает на существующий документ. Полные объяснения здесь не дублируются.

| Симптом или вопрос | Где читать |
|--------------------|------------|
| `lnr_new_client_*` возвращает **NULL** / у Rust `Client::new_*` — **None** | [errors-and-logging.md](errors-and-logging.md), [backends.md](backends.md) (сеть, пути, права) |
| **`lnr_run`** / **`run`** возвращает **false** | [using-the-api.md](using-the-api.md), [errors-and-logging.md](errors-and-logging.md) (адрес привязки, `regist_topic`, stderr) |
| **Паника** сразу после **`run`** (хранилище / регистрация топика) | [store-startup-failure-semantics.md](store-startup-failure-semantics.md) |
| **Отправка** не удаётся или «not found addr for topic» | [behavior-topics-delivery-and-errors.md](behavior-topics-delivery-and-errors.md), [routing-and-store-layout.md](routing-and-store-layout.md), [using-the-api.md](using-the-api.md) (`refresh_address_topic`) |
| Сообщения **теряются** после переподключения или **дублируются** | [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md) (`at_least_once_delivery`, `number_mess`) |
| Ключи **Redis** / что затрагивает **`clear_*`** | [operations-redis-sqlite.md](operations-redis-sqlite.md), [routing-and-store-layout.md](routing-and-store-layout.md) |
| **SQLite** WAL, резервное копирование, блокировка / `BUSY` | [backends.md](backends.md), [operations-redis-sqlite.md](operations-redis-sqlite.md) |
| **Крупные сообщения**, память, пороги сжатия | [capacity-and-limits.md](capacity-and-limits.md) |
| **TLS**, границы доверия, кто может читать/писать хранилище | [security-defaults.md](security-defaults.md) |
| Ошибки **линковки / DLL**, пути к заголовку, Windows и Linux | [c-api-compatibility-and-build.md](c-api-compatibility-and-build.md), [bindings.md](bindings.md) |
| Python **`loadLib`**, ctypes, **Makefile** для C++ | [bindings.md](bindings.md) |
| **`liner_debug`**, `cargo test`, интеграционные тесты | [debug-and-tests.md](debug-and-tests.md), [README](../../README.md) |
| Строки stderr **`Error file:line:`** | [errors-and-logging.md](errors-and-logging.md) |

Если ничего не подошло, найдите в репозитории точную строку лога или имя символа, затем откройте документ из таблицы выше.
