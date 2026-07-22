# Документация liner (использование библиотеки)

Дополняет [документацию крейта на docs.rs](https://docs.rs/liner_broker/) и [README проекта](../../README.md).

**Отладка и тесты:** [debug-and-tests.md](debug-and-tests.md) (фича `liner_debug`, ссылки на README для команд интеграции).

**Быстрый указатель:** [troubleshooting.md](troubleshooting.md) (симптом → какой документ открыть).

| Документ | Содержание |
|----------|------------|
| [troubleshooting.md](troubleshooting.md) | Симптом → ссылка на нужный документ |
| [bindings.md](bindings.md) | Python `ctypes` и пример C++: сборка, линковка, жизненный цикл; SQLite: см. [using-sqlite.md](using-sqlite.md) |
| [behavior-topics-delivery-and-errors.md](behavior-topics-delivery-and-errors.md) | **Поведение продукта:** топики, маршрутизация, доставка, ошибки (без формата по проводу) |
| [store-startup-failure-semantics.md](store-startup-failure-semantics.md) | Ошибки хранилища у клиента и у listener/sender и fail-fast при старте |
| [errors-and-logging.md](errors-and-logging.md) | Как проявляются ошибки (C `BOOL`, Rust `bool` / `Option`, логирование в stderr) |
| [backends.md](backends.md) | Redis и SQLite: URL, файлы, блокировки, `unique_name` |
| [using-redis.md](using-redis.md) | **Redis:** `new_redis`, общий URL, ключи `lnr_*`, C/Rust/Python, `test/redis/` |
| [using-sqlite.md](using-sqlite.md) | **SQLite:** `new_sqlite`, `receivers_json`, C API, разбор эталонного теста |
| [using-postgres.md](using-postgres.md) | **PostgreSQL:** фича `postgres`, общий URL, C/Rust/Python, тесты |
| [using-the-api.md](using-the-api.md) | Жизненный цикл, потоки, типичные ловушки |
| [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md) | Отключения, очереди в хранилище, интервал переподключения, `number_mess`, дедупликация, привязка индекса слота accept на listener |
| [routing-and-store-layout.md](routing-and-store-layout.md) | Маршрутизация топик → адрес, ключи Redis, таблицы SQLite, разбор для операторов |
| [operations-redis-sqlite.md](operations-redis-sqlite.md) | Префикс `lnr_*`, область `clear_*`, Redis ≥ 6.2, WAL и резервное копирование SQLite |
| [capacity-and-limits.md](capacity-and-limits.md) | Лимиты mempool / bytestream, порог zstd, чеклист по размерам |
| [security-defaults.md](security-defaults.md) | Без TLS, модель доверия для TCP, Redis и SQLite |
| [c-api-compatibility-and-build.md](c-api-compatibility-and-build.md) | Ожидания по стабильности C-символов/заголовка, артефакты `cargo`, линковка Linux и Windows |
| [debug-and-tests.md](debug-and-tests.md) | Фича `liner_debug`, `cargo test`, ссылка на README для интеграции / Python-тестов |

Все материалы ориентированы на интеграторов (Rust, C, Python-привязки поверх C API).
