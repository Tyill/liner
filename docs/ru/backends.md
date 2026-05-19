# Бэкенды хранилища (Redis, SQLite и PostgreSQL)

Все бэкенды реализуют один контракт `Store`, поэтому клиенты, listener и sender ведут себя одинаково на уровне обмена сообщениями.

## Выбор бэкенда

- **Redis** — общая память/диск, несколько хостов. **URL Redis** (например `redis://127.0.0.1/`). Всегда в сборке по умолчанию. Пошагово: [using-redis.md](using-redis.md).
- **SQLite** — один файл, без отдельного сервера. **Путь к файлу** БД.
- **PostgreSQL** — общая SQL-БД (как Redis), опциональная фича **`postgres`**. **URL libpq** (например `postgresql://user:pass@127.0.0.1/liner`). Пошагово: [using-postgres.md](using-postgres.md).

Используйте **`lnr_new_client_redis` / `lnr_new_client_sqlite` / `lnr_new_client_postgres`** (C) или **`Client::new_redis` / `Client::new_sqlite` / `Client::new_postgres`** (Rust). Символы PostgreSQL есть только при сборке с **`--features postgres`**.

## Изолированный SQLite (один файл на процесс)

Во многих схемах **у каждого процесса свой файл SQLite**. Тогда таблицы каталога (`topic_addr`, `topic_key`) **локальны**; они **не** разделяются так, как один **URL Redis**. Sender с пустым локальным каталогом не сможет разрешить топик пира, пока каталог не заполнен — из **предыдущего запуска** на том же файле, ручного `INSERT` или необязательного аргумента **`receivers_json`** у **`Client::new_sqlite` / `lnr_new_client_sqlite`**.

- **`receivers_json`:** UTF-8 JSON **массив** объектов `{ "topic", "addr", "client_name" }` — обычно **одна запись на пира** (свой топик в строке не обязателен). **`NULL` / `""` / пробелы / `[]`** — допустимо и означает **без сидирования** (используйте то, что уже в файле). **Непустой** массив требует эти три поля у каждого объекта; ошибки разбора дают **`NULL` / `None`** и лог в stderr. Сидирование **upsert** по первичному ключу (тот же топик / та же пара `(topic, addr)` обновляется на месте). Для изолированных пустых БД реализация назначает по проводу **`topic_key.k = 1`** для каждого топика пира из каталога, **`INSERT OR IGNORE`** то же для **вашего** `source_topic`, и первый **`connection_key` = 1** (не в JSON). Устаревшее свойство **`topic_key`** в JSON **игнорируется**.
- **`topic_key` по проводу** должен совпадать со значением в таблице **`topic_key`** для этого топика на **процессе listener**. При сидировании из `receivers_json` на пустых изолированных файлах это **1** для сидированных топиков. Проверка: SQLite `SELECT k FROM topic_key WHERE topic = ?` в файле пира или Redis **`GET lnr_topic:{topic}:key`** при Redis.
- **Сквозной пример** (два временных файла БД, каталог в JSON-файле, второй клиент сидируется из файла): модульный тест **`isolated_sqlite_two_clients_via_receivers_json_catalog_file`** в [`src/client.rs`](../../src/client.rs).
- **`at_least_once_delivery`:** при **одном файле SQLite на процесс** подтверждения listener попадают в **файл получателя**, а sender опрашивает **`conn_mess_number` в своём файле** — они не пересекаются. Предпочтительно **`at_least_once_delivery == false`** для `send_to` / `send_all` в такой схеме, либо **общий** файл БД / Redis, если нужен at-least-once между процессами. Подробности: [using-sqlite.md](using-sqlite.md) (*Изолированные файлы и `at_least_once_delivery`*), [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md).

Типичные шаги: (1) пир A **`run`** на своей БД и топике; (2) прочитать **`bound_listen_addr`** (или известную строку bind) и **`unique_name`**; (3) записать одну запись массива JSON (`topic`, `addr`, `client_name`) в файл или конфиг; (4) пир B **`new_sqlite(..., receivers_json)`**, затем **`run`**; (5) B **`send_to`** на топик A.

- **Несколько пиров на изолированных файлах:** при **одном файле `.sqlite` на процесс** без ошибок хранилища надёжно работает только **one-to-one** (одна запись пира на сторону в `receivers_json`). **One-to-many**, **many-to-one** или несколько объектов в одном JSON требуют **общего** файла SQLite, **Redis** или **ручной** правки **`connection_key`** / **`conn_sender`** / **`conn_key_map`** в каждом файле. Симптомы и обходы: [using-sqlite.md](using-sqlite.md) (*Изолированные БД: только one-to-one*).

## `unique_name`

Строка `unique_name` идентифицирует экземпляр клиента в хранилище (вместе с топиками и адресами). С **Redis** или **PostgreSQL** пиры используют **один URL** и согласованные данные топик/адрес. С **SQLite** процессы с **одним файлом БД** видят один каталог; **разные файлы** — нет — используйте **`receivers_json`** (или SQL). Каждому клиенту нужен **уникальный** `unique_name` и своя привязка `localhost`.

## Redis

**Пошаговое использование:** [using-redis.md](using-redis.md).

- Подключение через официальный крейт `redis`: **`Client::open`**, затем **`get_connection`** в `Redis::new`. Если сервер недоступен, **`Client::new_*` возвращает `None`** / в C **`NULL`**.
- Дальнейшие операции могут завершаться **`DbError`** с сообщениями от Redis (таймауты, обрывы и т.д.). Это даёт неуспех API и логи в stderr, не обязательно выход процесса.
- **Префикс ключей, поведение `clear_*`, версия сервера (рекомендуется ≥ 6.2):** [operations-redis-sqlite.md](operations-redis-sqlite.md).

## SQLite

**Пошаговое использование (Rust/C, `receivers_json`, экспорт каталога, ссылка на тест):** [using-sqlite.md](using-sqlite.md).

- Открытие через **`rusqlite`** со **встроенным** SQLite (см. `Cargo.toml`).
- При открытии библиотека выставляет **`PRAGMA journal_mode=WAL`**, **`PRAGMA busy_timeout`** **5000 ms** и **`foreign_keys=ON`**.
- **`SQLITE_BUSY`**: SQLite ждёт до busy timeout. Если блокировка так и не получена, операция падает со строкой ошибки из `rusqlite` (как **`DbError`**). Это **не** отдельный таймаут «жизни соединения»; он на каждую операцию, требующую блокировки.
- Несколько процессов или потоков с **одним файлом** конкурируют за блокировку БД; проектируйте нагрузку (мало писателей или сериализация доступа).
- **Операции (резервное копирование, WAL-файлы, `clear_*` в SQLite):** [operations-redis-sqlite.md](operations-redis-sqlite.md).
- **Лимиты памяти / размера сообщений, сжатие:** [capacity-and-limits.md](capacity-and-limits.md).

## PostgreSQL

**Пошаговое использование:** [using-postgres.md](using-postgres.md).

- Фича **`postgres`**, сборка: **`cargo build --features postgres`**.
- Та же **схема таблиц**, что у SQLite; **`lock_timeout`** 5 с.
- **Без `receivers_json`** на клиенте — каталог через **`run`** / **`refresh_address_topic`**, как у Redis.
- **Операции:** [operations-redis-sqlite.md](operations-redis-sqlite.md).

## Смешанные развёртывания

**Не** смешивайте Redis, SQLite и PostgreSQL в одной логической сети без намеренной изоляции. Данные не разделяются между бэкендами.

**Постура безопасности (без TLS, границы доверия):** [security-defaults.md](security-defaults.md).

## См. также

- [store-startup-failure-semantics.md](store-startup-failure-semantics.md) — клиент и listener/sender при открытии хранилища на `run`.  
- [routing-and-store-layout.md](routing-and-store-layout.md) — имена ключей Redis и таблицы SQLite (для отладки и бэкапов).  
- [operations-redis-sqlite.md](operations-redis-sqlite.md) — политика префикса, `clear_*`, Redis 6.2+, WAL и бэкап SQLite.
