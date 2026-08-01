# Старт хранилища и семантика сбоев

Этот документ уточняет, что завершается без паники, что возвращает `run`, и как listener/sender связаны со store. Он заменяет старую формулировку, которая неверно описывала второй `open_store` и панику процесса при старте listener.

Полная таблица кодов ошибок — в [errors-and-logging.md](errors-and-logging.md). Рекомендуемый порядок вызовов — в [using-the-api.md](using-the-api.md).

---

## Путь клиента vs фоновые задачи

### Клиентский API

Путь **клиента** обрабатывает ошибки store без паники. Операции с Redis, SQLite или PostgreSQL могут завершаться неуспехом (таймауты, `SQLITE_BUSY`, I/O, обрыв соединения). Такие сбои проявляются как:

- **`false` / `NULL`** на вызове API,
- код на клиенте **`lnr_last_error_code`**, например **`LNR_ERR_STORE`**,
- и строка деталей в **stderr** или в опциональном **log hook**.

### Listener и sender

После **`run`** задачи **listener** и **sender** используют **тот же** handle store, что и клиент: `Arc<Mutex<dyn Store>>`.

Они **не** открывают второе независимое подключение к store в момент `run`. Более старая документация, где описывался отдельный `open_store` для listener/sender, **устарела**.

---

## Что делает `run` (с точки зрения сбоев)

Упрощённый порядок при успешном первом `run`:

1. Resolve и **TCP bind** строки bind из конструктора (`localhost`).
2. Вычисление **published**-адреса для каталога (advertise или bound-адрес).
3. **`regist_topic`** в store, чтобы пиры могли найти этого клиента.
4. Создание **listener** (mio poll, register TCP listener, создание waker, `get_topic_key` для source topic).
5. Создание **sender**, подписка на внутренний канал, событие `client_connected`.

Каждый шаг падает по-своему:

| Сбой | Возврат | Last error | Паника процесса? |
|------|---------|------------|------------------|
| Невалидная строка bind / сбой TCP bind | `false` | `LNR_ERR_BIND` | Нет |
| Сбой `regist_topic` в каталоге | `false` | `LNR_ERR_STORE` | Нет |
| Сбой конструктора listener после bind + regist | `false` | **`LNR_ERR_STARTUP`** | **Нет** |
| Сбой subscribe на internal channel после создания listener/sender | `false` | `LNR_ERR_STORE` | Нет |
| Уже running (второй `run`) | `true` | `LNR_OK` | Нет |

### `LNR_ERR_STARTUP` подробнее

Старт listener включает локальный I/O и ранние lookup’и в store, нужные для входа в event loop, например:

- создание экземпляра mio `Poll`,
- регистрация TCP listener,
- создание mio `Waker`,
- вызов `get_topic_key` для source topic.

Если любой из этих шагов падает, **`run` возвращает `false`** и ставит **`LNR_ERR_STARTUP`**. Частичное состояние откатывается: регистрация топика снимается (**unregister**), `published_addr` очищается, listener/sender не остаются running. Процесс **не** завершается аварийно.

Это намеренно отделено от **`LNR_ERR_BIND`**: bind значит «не удалось начать слушать TCP-адрес»; startup значит «TCP listen удался, но event loop listener поднять не удалось».

---

## Операционный вывод

- В стационарной работе клиентских операций ожидайте аккуратные **`false` + last_error`** и строки лога, а не паники, при проблемах store.
- После успешного **create** неуспешный **`run`** с **`LNR_ERR_STARTUP`** — это **восстанавливаемый сбой настройки**. Исправьте ресурсы / согласованность store и вызовите `run` снова (или пересоздайте клиент, если так удобнее).
- Конструктор sender в этой последовательности старта не использует прежний fail-fast путь `expect` по store.

---

## См. также

- [using-the-api.md](using-the-api.md) — жизненный цикл, advertise, stop/restart
- [errors-and-logging.md](errors-and-logging.md) — полная таблица `LNR_ERR_*`
- [backends.md](backends.md) — заметки по бэкендам Redis / SQLite / PostgreSQL
