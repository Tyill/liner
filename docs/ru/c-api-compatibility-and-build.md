# Совместимость C API и сборка

## Стабильность символов и раскладки

C-интерфейс задаётся **`include/liner.h`** и точками входа **`#[no_mangle] pub extern "C"`** в Rust-крейте (см. `src/lib.rs`). **`lnr_hClient`** — **непрозрачный указатель** на Rust-`Client`; внутренняя раскладка **не** часть C-контракта, поэтому изменения внутри `Client` сами по себе не ломают ABI указателя.

Проект **пока не публикует отдельную политику стабильности ABI** (например «совместимость символов только в патчах»). **Семантическое версионирование Rust-крейта** (`Cargo.toml` / crates.io) отслеживает **библиотеку целиком**, а не формально проверенную матрицу C ABI. На практике:

- **Аддитивные** изменения (новые функции и enum’ы) обратно совместимы для вызывающего кода, который использует только старые символы.
- **Переименования, смена сигнатур или удаление** C-функций либо **поведенческие изменения** из release notes требуют **пересборки и повторного тестирования** всех нативных привязок.
- Изменения в **`liner.h`** (типы, колбэки, константы) следует считать **потенциально ломающими** для потребителей C/C++, пока не проверите иное.

**Рекомендация:** закрепите **точную версию крейта / git-тег**, который поставляете, положите **`liner.h`** рядом с привязкой и прогоняйте интеграционные тесты при обновлении.

### Аддитивные символы в крейте 1.4.0

Следующие символы добавлены без изменения сигнатур существующих функций. Код, который на них не ссылается, продолжает работать с той же shared library:

**Статус и логирование**

- `lnr_set_status_cb`, `lnr_status_cb`, константы видов статуса (`LNR_PEER_*`, `LNR_SENDER_*`, `LNR_LISTENER_*`)
- `lnr_set_log_cb`, `lnr_log_cb`

**Ошибки и жизненный цикл**

- `LNR_OK` / `LNR_ERR_*` (включая **`LNR_ERR_STARTUP`**, **`LNR_ERR_BUSY`**)
- `lnr_last_error_code`, `lnr_last_error_message`
- `lnr_version`
- `lnr_set_advertise_addr`
- `lnr_stop`, `lnr_is_running`
- `lnr_advertise_addr`, `lnr_bound_listen_addr`, `lnr_published_addr`

**Интроспекция и лимиты**

- `lnr_list_addresses`, `lnr_addr_cb`
- `lnr_pending_count`, `lnr_pending_by_peer`
- `lnr_set_max_message_size`, `lnr_get_max_message_size`
- `lnr_set_compress_threshold`, `lnr_get_compress_threshold`
- `lnr_set_max_send_queue`, `lnr_get_max_send_queue`

Сигнатуры существующих конструкторов (`lnr_new_client_*`), `lnr_run` и `lnr_send_*` не менялись.

---

## Канонический заголовок

Поставляйте и компилируйте с **`include/liner.h`** (примеры на C++ подключают его через `cpp/liner_broker.h`). Держите заголовок **в синхроне** с артефактом `liner_broker`, который линкуете.

---

## Сборка разделяемой библиотеки (Linux и Windows)

Корневой [README](../../README.md) описывает **кроссплатформенное** использование (Linux, Windows) и стандартную сборку Rust:

```bash
cargo build --release
```

Артефакты попадают в **`target/release/`**. Крейт собирается как **`cdylib`** (`Cargo.toml`), поэтому получается **нативная разделяемая библиотека** с базовым именем по правилам Cargo (например **`libliner_broker.so`** на типичных Linux GNU, **`liner_broker.dll`** на Windows MSVC — уточните точное имя в `target/release` после первой сборки). Линкуйте её из C/C++ так же, как любую другую Rust-`cdylib` для вашего target triple.

Примерный **`cpp/Makefile`** предполагает Unix-подобную строку линковки (`-L ../target/release -lliner_broker`). На **Windows** направьте тулчейн на пару **import library `.lib` / `.dll`** (или эквивалент вашей среды) для MSVC или GNU target; флаги отличаются от `g++` на Linux — следуйте документации MSVC или MinGW по линковке Rust-DLL.

---

## Зависимости времени выполнения

- **Бэкенд Redis:** доступный **Redis**, совместимый с версиями из [operations-redis-sqlite.md](operations-redis-sqlite.md).
- **Бэкенд SQLite:** отдельного сервера нет; используется встроенный в бинарник Rust SQLite.
- **Бэкенд PostgreSQL:** опционально; сборка с **`cargo build --features postgres`**. Нужен доступный **PostgreSQL** и символ **`lnr_new_client_postgres`** в слинкованном артефакте. См. [using-postgres.md](using-postgres.md).
- **Платформа:** стандартная библиотека Rust и **libc** (на Unix), как у любой другой `cdylib`; сборки под Windows используют обычное MSVC или GNU runtime для вашего тулчейна Rust.

---

## См. также

- [errors-and-logging.md](errors-and-logging.md) — возвращаемые значения C, коды ошибок и дескрипторы `NULL`.
- [using-the-api.md](using-the-api.md) — жизненный цикл и осторожность с потоками в FFI.
- [bindings.md](bindings.md) — примеры обёрток Python и C++ поверх этого API.
