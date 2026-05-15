# Отладка и тесты

## Фича Cargo `liner_debug`

В `Cargo.toml` объявлена пустая фича **`liner_debug`**. При её включении макрос **`print_debug!`** выводит **трассировку в стиле `println!`**; при выключении (по умолчанию) **`print_debug!` — no-op** (см. `src/lib.rs`).

Сборка с отладочным выводом:

```bash
cargo build --release --features liner_debug
```

Сейчас **`print_debug!`** используется лишь в нескольких местах (например неудачные TCP-подключения в sender и часть путей listener). Это **не** замена структурированному логированию; **`print_error!`** по-прежнему пишет в **stderr** независимо от этого флага (см. [errors-and-logging.md](errors-and-logging.md)).

## Модульные тесты

Из корня репозитория:

```bash
cargo test
```

## Интеграционные тесты и обвязка на Python

**Актуальные команды для копирования**, переменные окружения Redis, автозапуск Docker для Python-тестов и опции `test/run_integration.py` поддерживаются в разделе **Tests** [README проекта](../../README.md) (ищите «Run Rust unit tests» / `LINER_TEST_REDIS` / `run_integration.py`).

### Интеграционные тесты на SQLite (`test/sqlite/`)

Параллельный набор сценариев на **общем файле SQLite** (без Redis). Запуск из корня репозитория:

```bash
python3 test/sqlite/run_integration.py
```

Список скриптов, фильтр по имени и продолжение после ошибки — как у `test/run_integration.py` (`--list`, `--only`, `--continue-on-fail`). Нужны собранный **`target/release/libliner_broker.so`** и модуль **`python/liner.py`** с **`Client.new_sqlite`**.

Внутри тестов каталог для «слушатель офлайн» задаётся через **`receivers_json`** при создании клиента, а не через отдельный процесс `sqlite3` на том же файле, пока живы клиенты liner на этом пути (иначе возможен краш процесса).

## Участие в разработке

Изменения приветствуются по обычной схеме **fork → branch → pull request**. Соблюдайте принятый **стиль Rust** и лицензию **MIT**; при изменении FFI держите C-заголовки (`include/liner.h`) в согласовании с кодом. При изменении поведения по возможности добавляйте или расширяйте **тесты**.
