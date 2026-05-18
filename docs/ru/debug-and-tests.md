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

**Актуальные команды** для Redis, SQLite и PostgreSQL — в разделе **Tests** [README проекта](../../README.md).

### Интеграционные тесты на Redis (`test/redis/`)

Сценарии на **общем Redis** (по умолчанию `redis://localhost/`). Из корня репозитория:

```bash
cargo build --release
python3 test/redis/run_integration.py
```

Поддерживаются `--list`, `--only`, `--continue-on-fail`. Часть тестов поднимает Redis через Docker; переменные `LINER_TEST_REDIS_*` — в README.

### Интеграционные тесты на SQLite (`test/sqlite/`)

Параллельный набор сценариев на **общем файле SQLite** (без Redis). Запуск из корня репозитория:

```bash
python3 test/sqlite/run_integration.py
```

Список скриптов, фильтр по имени и продолжение после ошибки — как у `test/redis/run_integration.py` (`--list`, `--only`, `--continue-on-fail`). Нужны собранный **`target/release/libliner_broker.so`** и модуль **`python/liner.py`** с **`Client.new_sqlite`**.

Внутри тестов каталог для «слушатель офлайн» задаётся через **`receivers_json`** при создании клиента, а не через отдельный процесс `sqlite3` на том же файле, пока живы клиенты liner на этом пути (иначе возможен краш процесса).

### Интеграционные тесты на PostgreSQL (`test/postgres/`)

Те же сценарии, что у SQLite, на **общей БД PostgreSQL** (без `receivers_json`). Из корня репозитория:

```bash
export LINER_TEST_POSTGRES_URL='postgresql://user:pass@127.0.0.1/liner_test'
cargo build --release --features postgres
pip install psycopg2-binary
python3 test/postgres/run_integration.py
```

Подробнее: [using-postgres.md](using-postgres.md).

## Участие в разработке

Изменения приветствуются по обычной схеме **fork → branch → pull request**. Соблюдайте принятый **стиль Rust** и лицензию **MIT**; при изменении FFI держите C-заголовки (`include/liner.h`) в согласовании с кодом. При изменении поведения по возможности добавляйте или расширяйте **тесты**.
