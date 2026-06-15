# Использование liner с PostgreSQL

Руководство для интеграторов, которые запускают брокер с **PostgreSQL** вместо Redis или SQLite: сборка с фичей **`postgres`**, создание клиентов, **общая БД** для процессов и Python-интеграционные тесты.

**См. также:** [backends.md](backends.md), [routing-and-store-layout.md](routing-and-store-layout.md) (те же таблицы, что у SQLite), [operations-redis-sqlite.md](operations-redis-sqlite.md), [errors-and-logging.md](errors-and-logging.md), [bindings.md](bindings.md).

---

## Когда подходит PostgreSQL

- **Общий каталог** — все процессы используют **один URL** (как **Redis**, в отличие от отдельных файлов SQLite на процесс).
- **Персистентное SQL-хранилище** — маршрутизация, офлайн-очереди и курсоры ack в таблицах (`topic_addr`, `conn_messages`, …).
- **Тот же API обмена**, что у других бэкендов: `run`, `send_to`, `subscribe`, TCP между пирами.

**Не подходит**, если нужен **файл без сервера** — используйте **SQLite**. Если уже есть **Redis**, он остаётся бэкендом по умолчанию (без дополнительной фичи).

У **`new_postgres` / `lnr_new_client_postgres` нет `receivers_json`**. Пиры видят друг друга после **`run`**; **внутренний канал** синхронизирует кэш адресов в рантайме (см. [using-the-api.md](using-the-api.md)).

---

## Сборка

Поддержка PostgreSQL — **опциональная** фича в `Cargo.toml`:

```bash
cargo build --release --features postgres
```

Используется крейт **`postgres`** с **`NoTls`** (обычный TCP). TLS в штатной сборке **не** подключён — [security-defaults.md](security-defaults.md).

**C / Python:** библиотеку нужно собрать **с** `--features postgres`, иначе символ **`lnr_new_client_postgres`** отсутствует.

---

## URL подключения

Строка **libpq**, например:

```text
postgresql://user:password@127.0.0.1:5432/liner
```

Непустые **`unique_name`**, **`topic`**, **`localhost`**. При недоступном сервере **`Client::new_postgres`** → **`None`** / C **`NULL`**.

При первом подключении создаётся схема (`CREATE TABLE IF NOT EXISTS …`) и выставляется **`lock_timeout`** **5s**.

---

## Общая база

| Вопрос | Поведение |
|--------|-----------|
| **Каталог** | **`topic_addr`** общая для всех клиентов с одним URL. |
| **Старт сети** | A **`run`** → B **`run`** → **`send_to`**. Кэш обновляется по внутреннему каналу; **`refresh_address_topic`** по желанию — см. [using-the-api.md](using-the-api.md). |
| **`at_least_once_delivery`** | На **одном URL** ack в **`conn_mess_number`** виден отправителю — как Redis / **общий файл SQLite**. |
| **Несколько пиров** | Fan-out / fan-in на **одном URL** без JSON-каталога (см. ограничения изолированного SQLite в [using-sqlite.md](using-sqlite.md)). |

Публичные конструкторы клиента **не принимают JSON каталога** для PostgreSQL.

---

## Создание клиента

### Rust

```rust
let mut c = Client::new_postgres(
    "my_unique_name",
    "my_source_topic",
    "127.0.0.1:0",
    "postgresql://user:pass@127.0.0.1/liner",
)
.expect("open postgres store");
```

### C (`include/liner.h`)

```c
lnr_hClient c = lnr_new_client_postgres(
    "my_unique_name",
    "my_source_topic",
    "127.0.0.1:0",
    "postgresql://user:pass@127.0.0.1/liner"
);
```

### Python

```python
c = liner.Client.new_postgres(
    "my_unique_name", "my_source_topic", "127.0.0.1:0",
    "postgresql://user:pass@127.0.0.1/liner",
)
```

---

## Запросы оператора

Те же **имена таблиц**, что у SQLite — [routing-and-store-layout.md](routing-and-store-layout.md).

---

## Два пира (кратко)

1. Одна БД и URL в PostgreSQL.  
2. `cargo build --release --features postgres`.  
3. A: `new_postgres`, `run` на `topic_a`.  
4. B: тот же URL, `run` на `topic_b`.  
5. B: `refresh_address_topic("topic_a")`, `send_to(...)`.

Тест **`shared_postgres_two_clients_send_to`** в [`src/client.rs`](../../src/client.rs); Python — **`test/postgres/`**.

---

## Тесты

```bash
export LINER_TEST_POSTGRES_URL='postgresql://user:pass@127.0.0.1/liner_test'
cargo build --release --features postgres
pip install psycopg2-binary
python3 test/postgres/run_integration.py
```

---

## См. также

[backends.md](backends.md), [debug-and-tests.md](debug-and-tests.md), [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md).
