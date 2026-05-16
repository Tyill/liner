# Использование liner с SQLite

Руководство для интеграторов, которые запускают брокер с **файлом SQLite** вместо Redis: как создавать клиентов, когда нужен **`receivers_json`**, как **сидированные ключи по проводу** связаны с маршрутизацией и как повторить **эталонный модульный тест** в своём коде.

**Также прочитайте:** [backends.md](backends.md) (выбор бэкенда, модель изолированной БД), [operations-redis-sqlite.md](operations-redis-sqlite.md) (WAL, бэкап, `clear_*`), [errors-and-logging.md](errors-and-logging.md) (`NULL` / `None`, stderr), [bindings.md](bindings.md) (C / Python поверх `include/liner.h`).

---

## Когда уместен SQLite

- **Один файл**, без сервера Redis — встроенные тесты, ноутбуки, устройства.
- **Тот же API обмена**, что у Redis: `run`, `send_to`, `subscribe`, TCP между пирами.
- **Каталог** (какой топик слушает на каком адресе и какой целочисленный **ключ топика** по проводу) хранится в таблицах SQLite `topic_addr` и `topic_key` (см. [routing-and-store-layout.md](routing-and-store-layout.md)).

**Оговорка:** если **у каждого процесса свой файл `.sqlite`**, каталоги **не** общие. Пустая БД sender’а не знает топик пира, пока вы **не засидируете** каталог (`receivers_json`, предыдущий запуск на том же файле или ручной SQL). Redis избегает этого за счёт одного общего URL.

**Изолированные файлы и `at_least_once_delivery`:** ack listener пишутся в **файл того процесса** (`conn_mess_number`), а sender обновляет ack из **своего** файла. При **разных путях БД у пиров** эти представления **не** сливаются — держите **`at_least_once_delivery` = false** (`lnr_send_to` / `send_to` / `send_all` последний аргумент, в Rust `false`), чтобы стек не держал неподтверждённый at-least-once трафик в RAM в ожидании обновления хранилища, которое никогда не придёт. Для настоящего at-least-once между процессами используйте **один общий путь SQLite** (как один URL Redis) или Redis.

**Изолированная пара (пустые БД):** первый логический канал использует по проводу **`connection_key` = 1** (в этой модели документируется как **token id**). `receivers_json` **не** несёт этот id; перечисляйте **только пиров** (их `topic` / `addr` / `client_name`). Сидирование вставляет **`conn_sender(1, peer_topic)`** и **`conn_key_map`** для этих строк, и **`INSERT OR IGNORE topic_key(your_source_topic, 1)`**, чтобы у listener’а был проволочный ключ без самострочной строки в JSON. **По проводу `topic_key.k = 1`** и для каждого засидированного топика пира (не передаётся в JSON).

**Один общий файл SQLite** (один путь для согласующихся процессов): передавайте **`receivers_json` пустым** (`""` / `[]`); пиры регистрируются в одном хранилище, `conn_sender` и ключи остаются согласованными без JSON-каталога.

### Изолированные БД: только one-to-one (ограничение на несколько пиров)

Если **у каждого процесса свой файл `.sqlite`**, путь сидирования каталога рассчитан на **одного удалённого пира на процесс** в `receivers_json` — классическая схема **one-to-one** (A указывает только B, B — только A). Именно это покрывает эталонный тест **`isolated_sqlite_two_clients_via_receivers_json_catalog_file`**.

**Из коробки не поддерживается** на изолированных пустых файлах (без ручной правки хранилища):

- **Один sender, много listener’ов** — например `send_all` на топик с несколькими строками `(addr, client_name)` в `receivers_json` sender’а.
- **Много sender’ов, один listener** — несколько серверов в `receivers_json` listener’а.
- **Несколько пиров в одном массиве `receivers_json`** на одном процессе, когда нужны разные каналы по проводу.

**Почему:** при сидировании всем строкам пиров пишется проволочный **`connection_key = 1`**, но в таблице **`conn_key_map`** действует **`UNIQUE(connection_key)`** (см. [routing-and-store-layout.md](routing-and-store-layout.md)). Только один composite может владеть ключом **1**; у остальных пиров строка в `conn_key_map` может пропасть или при отправке появятся динамические ключи **2**, **3**, …, тогда как в БД listener’ов после seed остаётся только **`conn_sender(1, …)`**. Файлы sender и receiver **не** согласуют эти id автоматически.

**Типичные симптомы:** stderr `couldn't get_sender_topic, conn_key N, err Query returned no rows`; в колбэке пустой или неверный **`from`**, при нормальном **`to`**.

**Что использовать вместо этого:**

| Задача | Подход |
|--------|--------|
| Много ко многим, `send_all`, несколько sender’ов | **Один общий путь `.sqlite`** для всех процессов (как один URL Redis), **пустой** `receivers_json`, или **Redis**. |
| Изолированные файлы, один логический пир | Только **one-to-one** в seed; **`at_least_once_delivery == false`**. |
| Продвинутый / без гарантий | Ручная правка таблиц (ниже) в файле **каждого** процесса или скрипт **`python/set_sqlite_connection_key.py`**. |

Сейчас библиотека **не** выдаёт при seed разные **`connection_key` 1, 2, …** по числу пиров в JSON; не полагайтесь на многострочный `receivers_json` для изолированного fan-out/fan-in без ручной настройки таблиц.

#### Ручное выравнивание `connection_key` (оператор)

Только если нужен **свой `.sqlite` на процесс** и **больше одного пира** (например один server, два listener). Для пары sender→listener все процессы должны использовать **одно и то же целое по проводу**.

**Безопасность**

- **Остановите** все клиенты liner с открытым файлом (без `run()` на этом пути). Правка под WAL при работающем клиенте может привести к падению; см. [test/sqlite/_support.py](../../test/sqlite/_support.py).
- Для изолированных файлов — **`at_least_once_delivery == false`**.
- После правок перезапустите пиры.

**Имена**

| Имя | Смысл |
|-----|--------|
| **`self` `unique_name` / `source_topic`** | Процесс-владелец этого `.sqlite` (аргументы `Client::new_sqlite`). |
| **`unique_name` пира** | Удалённый `client_name` (`client1`, `server1`, …). |
| **`topic` пира** | В файле **listener**: `source_topic` удалённого sender’а (колбэк **`from`**, напр. `topic_server1`). В файле **sender**: топик listener’а для `send_to` / `send_all` (напр. `topic_client`). |
| **`connection_key`** | Число в заголовке сообщения; должно совпадать в `conn_key_map` (sender) и `conn_sender` (listener). |

**Composite (только файл sender):** `{self_unique}:{self_source_topic}:{peer_unique_name}` — пример: `server1:topic_server1:client2`.

---

**1. БД sender’а** (вызывает `send_to` / `send_all`)

| Таблица | Действие |
|---------|----------|
| **`conn_key_map`** | `INSERT OR REPLACE` `(composite, connection_key)` на каждого listener-пира. На одно значение **`connection_key`** — одна строка (**`UNIQUE`**); при назначении ключа `2` может понадобиться удалить другой composite с тем же `2`. |
| **`conn_sender`** | По желанию до отправки; при send библиотека пишет `(connection_key, self_source_topic)`. |
| **`seq`** | `UPDATE seq SET v = MAX(v, N) WHERE id = 1`, где **N** — максимальный используемый ключ. |

Пример SQL (`server1.sqlite`, ключ **2** для `client2`):

```sql
INSERT OR REPLACE INTO conn_key_map (composite, connection_key)
  VALUES ('server1:topic_server1:client2', 2);
INSERT OR REPLACE INTO conn_sender (connection_key, sender_topic)
  VALUES (2, 'topic_server1');
UPDATE seq SET v = MAX(v, 2) WHERE id = 1;
```

---

**2. БД listener’а** (принимает сообщения)

| Таблица | Действие |
|---------|----------|
| **`conn_sender`** | `INSERT OR REPLACE (connection_key, sender_topic)` — **`sender_topic`** это **`source_topic` удалённого sender’а** (строка **`from`**), не ваш топик. **PK по `connection_key`**: два sender’а с одним ключом на одном файле — только если устраивает одна запись `from`. |
| **`conn_key_map`** | Не нужна, если процесс только принимает. |
| **`seq`** | Поднять `v`, как выше. |

Пример SQL (`client2.sqlite`, с wire **2** от `topic_server1`):

```sql
INSERT OR REPLACE INTO conn_sender (connection_key, sender_topic)
  VALUES (2, 'topic_server1');
UPDATE seq SET v = MAX(v, 2) WHERE id = 1;
```

---

**3. Согласование (один server, два listener)**

| Пир | `conn_key_map` (server) | `conn_sender` (client) |
|-----|-------------------------|-------------------------|
| `client1` | `…:client1` → **1** | `(1, 'topic_server1')` |
| `client2` | `…:client2` → **2** | `(2, 'topic_server1')` |

Запускайте **один раз на файл**: `--side listener` на каждой client БД, `--side sender` на server (`--topic` только для listener; на sender для `conn_sender` — `--self-topic`).

**Скрипт** (из корня репозитория):

```bash
python3 python/set_sqlite_connection_key.py client2.sqlite \
  --side listener --conn-key 2 \
  --topic topic_server1 --unique-name server1

python3 python/set_sqlite_connection_key.py server1.sqlite \
  --side sender --conn-key 2 \
  --self-name server1 --self-topic topic_server1 \
  --topic topic_client --unique-name client2
```

---

**4. Ответ клиента server’у (обратное направление)**

Если **клиент** должен вызвать **`send_to` на топик server’а** (например `topic_server1`) при изолированных файлах, роли меняются: **файл клиента — sender**, **файл server’а — listener**. Нужны те же правки в **обеих** БД для нового проволочного ключа **K** (это **не обязательно** тот же номер, что server→client для этого пира).

| Процесс | Роль при ответе | Что задать |
|---------|----------------|------------|
| **`clientN.sqlite`** | sender | `conn_key_map`: `clientN:topic_client:server1` → **K**; поднять **`seq`** |
| **`server1.sqlite`** | listener | `conn_sender`: **(K, `topic_client`)** — в колбэке **`from`** будет `source_topic` клиента; поднять **`seq`** |

В запуске для **server (listener)** параметр **`--topic`** — это **`topic_client`**, не `topic_server1`.

Пример: server→`client2` уже использует ключ **2** (п. 3); ответ client2→server задают другим ключом **K = 12** на обоих файлах, чтобы не перегружать **`conn_sender(2, …)`** на server (там ключ **2** уже может быть занят исходящим `conn_key_map` к `client2`, а в **`conn_sender`** только одна строка на `connection_key`).

```bash
# client2 шлёт server’у
python3 python/set_sqlite_connection_key.py client2.sqlite \
  --side sender --conn-key 12 \
  --self-name client2 --self-topic topic_client \
  --topic topic_server1 --unique-name server1

# server принимает от client2
python3 python/set_sqlite_connection_key.py server1.sqlite \
  --side listener --conn-key 12 \
  --topic topic_client --unique-name client2
```

Далее `client2` вызывает `send_to("topic_server1", payload, false)` (изолированные файлы → для ответа тоже **`at_least_once_delivery` false**, если нет общей БД).

**One-to-one** (только A и B, каждый засидировал другого в `receivers_json`): для обоих направлений часто хватает ключа **1** на каждом файле (composite `A:topic_a:b` и `B:topic_b:a` разные, `conn_key_map` не конфликтует). **Один server, много clients** и ответы: закладывайте **разные ключи** для входящего и исходящего на **файле server’а**, если одно и то же число ломает **`conn_sender`**.

---

## Создание клиента

### Rust

Используйте **`liner_broker::Client`** или **`liner_broker::Liner`**; оба принимают **`at_least_once_delivery`** в **`send_to`** / **`send_all`**. В README крейта по-прежнему показан **`Liner::new`** для Redis.

**`Liner::new_sqlite`** принимает те же пять логических аргументов, что C API (`receivers_json` может быть `""`).

```rust
use liner_broker::Liner;

let mut c = Liner::new_sqlite(
    "my_unique_name",
    "my_source_topic",
    "127.0.0.1:0",
    "/path/to/db.sqlite",
    "", // или JSON-строка каталога для изолированных БД
);
c.clear_stored_messages();
c.clear_addresses_of_topic();
assert!(c.run(Box::new(|_to, _from, _data| { /* … */ })));
```

После успешного **`run`** у **`Liner`** можно вызывать:

- **`bound_listen_addr()`** → `Option<String>` — реальный адрес привязки, если использовали порт **`0`** (для экспорта в каталог).
- **`unique_name()`** → `String` — значение, которое пиры должны указать в JSON в поле **`client_name`**.

Те же аксессоры есть у **`Client`** (`bound_listen_addr` / `unique_name` возвращают `Option<&str>` / `&str`).

### C (`include/liner.h`)

```c
lnr_hClient c = lnr_new_client_sqlite(
    "my_unique_name",
    "my_source_topic",
    "127.0.0.1:0",
    "/path/to/db.sqlite",
    NULL   /* или "[]" или UTF-8 строка JSON-массива */
);
if (!c) { /* неверные аргументы, ошибка открытия или некорректный непустой JSON */ }

BOOL ok = lnr_run(c, my_receive_cb, my_udata);
```

- **`receivers_json == NULL`**, пустая строка, только пробелы или JSON **`[]`** — **не** ошибка; сидирования нет (в БД уже могут быть строки от прошлой сессии).
- Некорректный **непустой** JSON → клиент **`NULL`**, сообщение в stderr.

---

## Формат каталога `receivers_json`

UTF-8 JSON: **один массив** объектов. У каждого объекта должны быть три поля:

| Поле | Смысл |
|------|--------|
| **`topic`** | **Зарегистрированное имя топика пира** (его `source_topic` / то, что вы передаёте в `send_to`, чтобы достучаться до него). Это же строка **`from`**, когда этот пир шлёт вам. (Устаревший вариант: строка, равная **вашему** `source_topic`, игнорируется для `conn_sender`; ваш проволочный `topic_key` обеспечивается сидером.) |
| **`addr`** | Удалённый TCP-адрес listener’а, например `127.0.0.1:54321` — должен совпадать с тем, как пир привязался (после `run` часто берут **`bound_listen_addr()`**, если использовали порт `0`). |
| **`client_name`** | Удалённый **`unique_name`** (что listener зарегистрировал в `topic_addr`). |

**Не в JSON:** проволочные **`topic_key`** и **`connection_key`** на канал — для изолированных пустых БД реализация сидирует **`topic_key.k = 1`** для каждого топика из каталога пира, то же для **вашего** `source_topic`, и **`conn_sender`** / **`conn_key_map`** для первого канала, как описано выше.

Повтор `(topic, addr)` или тот же **`topic`** с новой строкой: сидирование **upsert** (последняя строка побеждает при дубликатах топика в одном батче). См. [backends.md](backends.md) (*Изолированный SQLite*). Устаревшее свойство **`topic_key`** в объектах JSON парсер **игнорирует**.

Пример (один пир):

```json
[
  {
    "topic": "peer_topic",
    "addr": "127.0.0.1:2256",
    "client_name": "peer_unique"
  }
]
```

**Замечание по Redis:** `Store::seed_receivers` на бэкенде Redis — **no-op**; этот путь JSON для **изолированных** файлов SQLite.

---

## Эталонный разбор (те же шаги, что в модульном тесте)

В репозитории есть сквозной тест с **двумя разными файлами SQLite**, **файлом каталога JSON** и **`send_to`**. Используйте его как канонический рецепт.

- **Место:** [`src/client.rs`](../../src/client.rs), тест **`isolated_sqlite_two_clients_via_receivers_json_catalog_file`**.
- **Запуск:** `cargo test isolated_sqlite_two_clients_via_receivers_json_catalog_file`

**Шаги (как в тесте):**

1. **Приёмник (клиент A)**  
   - Отдельный путь к БД, например `…/a.sqlite`.  
   - `Client::new_sqlite("unique_a_iso", "topic_iso_a", "127.0.0.1:0", path_a, "")` — пустой `receivers_json`.  
   - `run` с колбэком, который распознаёт отправляемый payload (в тесте проверяют `b"ping"`).

2. **Экспорт каталога после запуска A**  
   - `listen = client_a.bound_listen_addr().expect("…")` — реальный host:port после bind.  
   - Собрать JSON: один объект с `"topic": "topic_iso_a"`, `"addr": listen`, `"client_name": client_a.unique_name()` (без `topic_key`; сидирование назначает проволочный ключ **1**).  
   - Записать строку в файл (в тесте `catalog.json` во временной директории).

3. **Отправитель (клиент B)**  
   - Другой путь БД `…/b.sqlite`, другой **`unique_name`**, другой исходный топик (например `"topic_iso_b"`).  
   - Прочитать файл в строку и вызвать `Client::new_sqlite(..., &catalog_json)`.  
   - `run` (в тесте для B — no-op колбэк приёма).

4. **Отправка**  
   - B вызывает `send_to("topic_iso_a", payload, false)` в цикле повторов, пока не вернёт `true` (TCP и маршрутизация могут потребовать несколько попыток). Здесь **`false`**, потому что у A и B **разные** файлы SQLite; **`true`** ждал бы ack в БД B, которые обновляет только listener A (см. *Изолированные файлы и `at_least_once_delivery`* выше).

5. **Проверка доставки**  
   - Колбэк A должен увидеть payload в вашем таймауте.

Тест использует `std::env::temp_dir()` для путей и в конце удаляет директорию; в приложении — реальные пути или конфигурация.

---

## Общий файл SQLite и изолированные файлы

| Модель | Как разделяется каталог |
|--------|-------------------------|
| **Один файл `.sqlite`**, открытый согласующимися процессами (один хост, дисциплина блокировок) | `regist_topic` / `run` обновляют общие `topic_addr` / `topic_key`; пиры видят друг друга без JSON, если разделяют файл. Предпочтительно **пустой** `receivers_json`. |
| **Один файл на процесс** (изолированные пустые БД) | **Только one-to-one:** в **`receivers_json`** каждой БД — **не больше одного** удалённого пира (см. *Изолированные БД: только one-to-one* выше). Проволочный **`connection_key`** — **1**; **`topic_key.k`** — **1**; seed пишет **`conn_sender`** для **`from`**. **`at_least_once_delivery == false`**. Для **one-to-many / many-to-one** — **общий** файл SQLite или Redis, а не несколько изолированных файлов с многострочным JSON. |

**Не** смешивайте **Redis** и **SQLite** для одной логической сети, если не хотите двух изолированных систем ([backends.md](backends.md)).

---

## См. также

- [using-the-api.md](using-the-api.md) — порядок `run`, потоки, ловушки `send_to`.  
- [c-api-compatibility-and-build.md](c-api-compatibility-and-build.md) — сборка `libliner_broker` и линковка C/C++.
