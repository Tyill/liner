# Маршрутизация и раскладка хранилища (для операторов)

Поведение **топиков / доставки / ошибок** без внутренностей хранилища — [behavior-topics-delivery-and-errors.md](behavior-topics-delivery-and-errors.md).

Этот документ для **отладки в продакшене**: что пишется в **Redis** или **SQLite**, как **топики** сопоставляются с **TCP-адресами**, как **ключи соединения** связаны с **офлайн-очередями**. Поведение общее для бэкендов, если не оговорено иное.

## Понятия

| Понятие | Смысл |
|---------|--------|
| **`unique_name`** | Стабильная строковая идентичность **экземпляра клиента** (на процесс). Используется в ключах хранилища и в составном id канала sender–listener. |
| **`source_topic`** | **Текущий топик** клиента (переданный при создании / `set_source_topic`). |
| **`localhost` (строка привязки)** | Строка **TCP listen-адреса** (например `127.0.0.1:2255`). Хранится как **имя поля** в каталоге топика (поле hash в Redis / колонка `topic_addr.addr` в SQLite). |
| **Топик (строка)** | Логическое имя, которым пользуются пиры (`send_to("other_topic", …)`). |
| **Адрес listener’а** | **Достижимый TCP endpoint** другого клиента, берётся из хранилища при отправке. |

## Как работает маршрутизация (высокий уровень)

1. **Каталог: топик → TCP-адреса**  
   Когда клиент регистрирует топик (`regist_topic`), хранилище фиксирует отображение: **имя топика → (строка bind этого клиента → `unique_name` этого клиента)**.  
   Sender’ы разрешают целевой топик через **`get_addresses_of_topic`**, затем открывают **TCP** к этим адресам. Несколько адресов под одним топиком означают несколько реплик; Rust-клиент **round-robin** по ним (`last_send_index`).

2. **Кто на другом конце**  
   Для каждого адреса listener’а sender вызывает **`get_listener_unique_name(destination_topic, addr)`**, чтобы узнать **`unique_name` listener’а**. Это имя входит в **композит**, по которому выделяется стабильный целочисленный **`connection_key`** для пары sender–listener.

3. **После успешного TCP connect**  
   Sender сохраняет **`save_listener_for_sender(addr, listener_topic)`**, чтобы после рестарта можно было **`get_listeners_of_sender`** и снова подключиться к тем же пирам.

4. **Устаревший кэш адресов**  
   Клиент кэширует адреса по топику в памяти. Если пир перерегистрировался на **новом** порту, вызывающему коду может понадобиться **`refresh_address_topic`**, чтобы обновить кэш из хранилища (см. [using-the-api.md](using-the-api.md)).

5. **Порядок в SQLite**  
   Адреса для топика читаются **`ORDER BY addr ASC`**, что влияет на порядок round-robin. В Redis используется порядок **`HGETALL`** (на порядок для операций не полагайтесь).

---

## Справочник ключей Redis

Все ключи с буквальным префиксом **`lnr_`**. Плейсхолдеры:

- `{topic}` — строка топика  
- `{localhost}` — строка bind как **имя поля** hash  
- `{unique}` — `unique_name` клиента  
- `{source_topic}` — топик sender’а  
- `{listener_name}` — **`unique_name` listener’а** (из каталога топика)  
- `{composite}` — строка `"{unique}:{source_topic}:{listener_name}"` (три компонента через двоеточие)  
- `{connection_key}` — десятичная строка целочисленного id  
- `{sender_key}` — строка `"{unique}:{source_topic}"` (два компонента)

| Ключ / шаблон | Тип | Назначение |
|---------------|-----|------------|
| `lnr_topic:{topic}:addr` | **HASH** поле → значение | Имя поля = строка bind **`localhost`** регистранта; значение = **`unique_name`** этого клиента. Каталог, кто слушает под `topic`. |
| `lnr_topic:{topic}:key` | **STRING** (int) | Стабильный небольшой целочисленный **ключ топика** для кодирования по проводу / подписок. |
| `lnr_unique_key` | **STRING** (счётчик) | Глобальный **`INCR`** для новых числовых id (`connection_key`, ключ топика). |
| `lnr_connection:{composite}:key` | **STRING** (int) | Отображает **`{unique}:{source_topic}:{listener_name}`** → **`connection_key`**. |
| `lnr_connection:{connection_key}:sender` | **STRING** | Строка **исходного топика** sender’а для этого логического канала. |
| `lnr_connection:{connection_key}:mess_number` | **STRING** (uint) | Последний **подтверждённый** номер сообщения для офлайн / дедупа (см. [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md)). |
| `lnr_connection:{connection_key}:messages` | **LIST** (бинарные блобы) | FIFO очередь закодированных сообщений, ожидающих это соединение. Слив в стиле **`RPUSH`** / **`LPOP`**. |
| `lnr_sender:{sender_key}:listener` | **HASH** поле → значение | Поле = строка **TCP-адреса** listener’а; значение = строка **топика** listener’а. Подсказки переподключения для этой идентичности sender’а. |

### Заметки по обслуживанию Redis

- **`clear_addresses_of_topic`** / **`clear_stored_messages`**: кратко здесь; полное **что удаляется и что нет**, политика префикса и заметка **Redis ≥ 6.2** → [operations-redis-sqlite.md](operations-redis-sqlite.md).

---

## Схема SQLite (взгляд оператора)

Один файл БД; **WAL** и **`busy_timeout`** выставляются при открытии (см. [backends.md](backends.md)). Таблицы:

| Таблица | Роль |
|---------|------|
| **`seq`** | Одна строка `id = 1`, колонка **`v`**: монотонный счётчик для новых значений **`connection_key`** (паттерн `UPDATE … RETURN` через `SELECT` после инкремента). |
| **`topic_addr`** | Строки `(topic, addr, client_name)` — те же семантики, что Redis `lnr_topic:{topic}:addr`: **`addr`** — строка bind, **`client_name`** — `unique_name`. **Первичный ключ `(topic, addr)`**. |
| **`topic_key`** | `(topic, k)` — целочисленный **ключ топика** на имя топика. |
| **`conn_key_map`** | `(composite, connection_key)`, где **`composite`** = `"{unique}:{source_topic}:{listener_name}"`. У **`connection_key`** ограничение **`UNIQUE`** (только один composite на данное целое). При изолированном seed всем пирам пишется ключ **1**; при **нескольких пирах в одном `receivers_json`** поздние строки могут вытеснить ранние composite или при отправке появятся динамические ключи — см. [using-sqlite.md](using-sqlite.md) (*Изолированные БД: только one-to-one*). |
| **`conn_sender`** | `(connection_key, sender_topic)` — проволочный **`connection_key`** → топик sender’а для колбэка **`from`**. **Первичный ключ** по **`connection_key`** (один топик на ключ в этом процессе). |
| **`conn_mess_number`** | `(connection_key, v)` — последний номер ack сообщения (та же роль, что Redis `mess_number`). |
| **`sender_listener`** | `(sender_key, addr, listener_topic)`, где **`sender_key`** = `"{unique}:{source_topic}"`. То же, что Redis `lnr_sender:…:listener`. |
| **`conn_messages`** | `(id, connection_key, payload)` с **`AUTOINCREMENT id`**, индекс **`(connection_key, id)`**. Очередь закодированных блобов; **FIFO** по возрастанию **`id`**. |

### Заметки по обслуживанию SQLite

- **Область `clear_*`** и процедуры **бэкапа / WAL** → [operations-redis-sqlite.md](operations-redis-sqlite.md).

---

## Кратко «куда смотреть»

| Симптом | Redis | SQLite |
|---------|-------|--------|
| Нет адресов для топика `T` | `HGETALL lnr_topic:T:addr` | `SELECT * FROM topic_addr WHERE topic = 'T';` |
| Офлайн-очередь застряла | `LLEN lnr_connection:{id}:messages` | `SELECT COUNT(*) FROM conn_messages WHERE connection_key = ?;` |
| Дедуп / курсор ack | `GET lnr_connection:{id}:mess_number` | `SELECT v FROM conn_mess_number WHERE connection_key = ?;` |
| Неверный пир / старый порт | Проверьте имена полей в `…:addr` на актуальные bind-строки | То же в **`topic_addr.addr`** |

---

## См. также

- [offline-delivery-and-message-numbers.md](offline-delivery-and-message-numbers.md) — `connection_key`, `number_mess`, тайминг переподключения.  
- [backends.md](backends.md) — выбор Redis vs SQLite.  
- [using-the-api.md](using-the-api.md) — `refresh_address_topic`, `clear_*` только когда не running.  
- [operations-redis-sqlite.md](operations-redis-sqlite.md) — детали `clear_*`, версия Redis, бэкап SQLite.
