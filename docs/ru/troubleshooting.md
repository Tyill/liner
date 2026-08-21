# Указатель по устранению неполадок

Эта страница — **карта**: каждая строка указывает на существующий документ. Полные объяснения здесь не дублируются.

| Симптом или вопрос | Где читать |
|--------------------|------------|
| `lnr_new_client_*` возвращает **NULL** / у Rust `Client::new_*` — **None** | [errors-and-logging.md](errors-and-logging.md), [backends.md](backends.md) (сеть, пути, права) |
| **`lnr_run`** / **`run`** возвращает **false** | Смотрите **`lnr_last_error_code`**: bind → [using-the-api.md](using-the-api.md); regist в store → [errors-and-logging.md](errors-and-logging.md); listener после bind (`LNR_ERR_STARTUP`) → [store-startup-failure-semantics.md](store-startup-failure-semantics.md) |
| Ожидали **панику** на старте listener после `run` | Этот путь теперь возвращает **`false` + `LNR_ERR_STARTUP`** (без паники). См. [store-startup-failure-semantics.md](store-startup-failure-semantics.md) |
| Как прочитать последний sync-сбой | [errors-and-logging.md](errors-and-logging.md) (`lnr_last_error_code` / `Client::last_error`; текст — в stderr или log hook) |
| Увести **`Error file:line:`** со stderr | [errors-and-logging.md](errors-and-logging.md) (`lnr_set_log_cb`) |
| Пиры не коннектятся после bind на `0.0.0.0` / порт `0` | [using-the-api.md](using-the-api.md) (*TCP bind и advertise*), геттеры `bound_listen_addr` / `published_addr` |
| **`set_advertise_addr`** падает во время running | Ожидаемо: **`LNR_ERR_ALREADY_RUNNING`**. Сначала `stop` или задайте до `run`. |
| **Отправка** не удаётся или «not found addr for topic» | [behavior-topics-delivery-and-errors.md](behavior-topics-delivery-and-errors.md), [routing-and-store-layout.md](routing-and-store-layout.md), [using-the-api.md](using-the-api.md) (`refresh_address_topic`, `list_addresses`) |
| Посмотреть каталог / глубину офлайн-очереди из API | [using-the-api.md](using-the-api.md) (*Интроспекция*: `list_addresses`, `pending_count`) |
| Сообщения **теряются** после переподключения или **дублируются** | [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md) (`at_least_once_delivery`, `number_mess`) |
| **`clear_*`** падает, пока клиент running | Сначала **`stop`**, или clear до `run`. Код **`LNR_ERR_CLEAR_WHILE_RUNNING`**. |
| Ключи **Redis** / что затрагивает **`clear_*`** | [operations-redis-sqlite.md](operations-redis-sqlite.md), [routing-and-store-layout.md](routing-and-store-layout.md) |
| **SQLite** WAL, резервное копирование, блокировка / `BUSY` | [backends.md](backends.md), [operations-redis-sqlite.md](operations-redis-sqlite.md) |
| **Крупные сообщения**, память, пороги сжатия | [capacity-and-limits.md](capacity-and-limits.md) (`lnr_set_max_message_size`, `lnr_set_compress_threshold`) |
| **TLS**, границы доверия, кто может читать/писать хранилище | [security-defaults.md](security-defaults.md) |
| Ошибки **линковки / DLL**, пути к заголовку, Windows и Linux | [c-api-compatibility-and-build.md](c-api-compatibility-and-build.md), [bindings.md](bindings.md) |
| Python **`loadLib`**, ctypes, **Makefile** для C++ | [bindings.md](bindings.md) |
| **`liner_debug`**, `cargo test`, интеграционные тесты | [debug-and-tests.md](debug-and-tests.md), [README](../../README.md) |
| Строки stderr **`Error file:line:`** | [errors-and-logging.md](errors-and-logging.md) |

Если ничего не подошло, найдите в репозитории точную строку лога или имя символа, затем откройте документ из таблицы выше.
