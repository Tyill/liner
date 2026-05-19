# Использование liner с Redis

Руководство для интеграторов с **Redis** как общим хранилищем: сборка по умолчанию, создание клиентов, **один URL** для всех процессов и Python-интеграционные тесты.

**См. также:** [backends.md](backends.md), [routing-and-store-layout.md](routing-and-store-layout.md) (ключи Redis), [operations-redis-sqlite.md](operations-redis-sqlite.md), [errors-and-logging.md](errors-and-logging.md), [bindings.md](bindings.md).

---

## Когда подходит Redis

- **Общий каталог** — все процессы используют **один URL Redis** (как **PostgreSQL**; не как отдельные файлы SQLite).
- **Низкая задержка**, преимущественно in-memory — удобный вариант по умолчанию.
- **Тот же API обмена**, что у других бэкендов: `run`, `send_to`, `subscribe`, TCP.

**Не подходит**, если нужен **один переносимый файл без сервера** — **SQLite**. Для **SQL и таблиц** — **PostgreSQL** ([using-postgres.md](using-postgres.md)).

У Redis-клиентов **нет `receivers_json`**. Пиры видят друг друга после **`run`** или через **`refresh_address_topic`** (как у PostgreSQL).

---

## Сборка

Redis входит в **сборку по умолчанию**, без дополнительной фичи:

```bash
cargo build --release
```

Крейт **`redis`**: **`Client::open`**, затем **`get_connection`** при создании store.

**C / Python:** в стандартной **`libliner_broker.so`** уже есть **`lnr_new_client_redis`**.

---

## URL подключения

Например:

```text
redis://127.0.0.1/
redis://127.0.0.1:6379/3
```

Выделите **отдельную логическую БД** (`/N` в URL), чтобы ключи **`lnr_*`** не смешивались с другими приложениями — [operations-redis-sqlite.md](operations-redis-sqlite.md).

Непустые **`unique_name`**, **`topic`**, **`localhost`**. Если Redis недоступен — **`None`** / C **`NULL`**.

**Версия сервера:** рекомендуется Redis **≥ 6.2**.

---

## Общее хранилище

| Вопрос | Поведение |
|--------|-----------|
| **Каталог** | Хеш **`lnr_topic:{topic}:addr`** общий для всех клиентов с одним URL. |
| **Старт сети** | A **`run`** → B **`run`** → B **`refresh_address_topic(topic_a)`** → **`send_to`**. |
| **`at_least_once_delivery`** | На **одном URL** ack в **`lnr_connection:{id}:mess_number`** виден отправителю. |
| **Несколько пиров** | Fan-out / fan-in на **одном URL** без JSON (см. изолированный SQLite в [using-sqlite.md](using-sqlite.md)). |

Подробнее о ключах: [routing-and-store-layout.md](routing-and-store-layout.md).

---

## Создание клиента

### Rust

```rust
let mut c = Client::new_redis(
    "my_unique_name",
    "my_source_topic",
    "127.0.0.1:0",
    "redis://127.0.0.1/",
)
.expect("open redis store");
```

**`Client::new`** — псевдоним для **`new_redis`**.

### C (`include/liner.h`)

```c
lnr_hClient c = lnr_new_client_redis(
    "my_unique_name", "my_source_topic", "127.0.0.1:0", "redis://127.0.0.1/");
```

**`lnr_new_client`** (устаревший) вызывает **`lnr_new_client_redis`**.

### Python (`python/liner.py`)

```python
liner.loadLib("target/release/libliner_broker.so")
c = liner.Client("my_unique_name", "my_source_topic", "127.0.0.1:0", "redis://127.0.0.1/")
```

После **`run`** — **`bound_listen_addr()`**, **`unique_name()`**.

---

## Проверка в Redis

```bash
redis-cli HGETALL lnr_topic:my_topic:addr
redis-cli GET lnr_topic:my_topic:key
```

В проде — **`SCAN`** с шаблоном `lnr_*`, не **`KEYS`**.

---

## Два пира (кратко)

1. Запустить Redis, один URL для всех.
2. `cargo build --release`.
3. A: `new_redis`, `run` на `topic_a`.
4. B: тот же URL, `run` на `topic_b`.
5. B: `refresh_address_topic("topic_a")`, `send_to(...)`.

Бенч: **`bench_pair_sendto_redis`**.

---

## Тесты

```bash
cargo build --release
python3 test/redis/run_integration.py
```

Часто поднимают Redis в **Docker** на порту **16379**. См. [debug-and-tests.md](debug-and-tests.md).

---

## См. также

- [using-sqlite.md](using-sqlite.md), [using-postgres.md](using-postgres.md)  
- [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md)
