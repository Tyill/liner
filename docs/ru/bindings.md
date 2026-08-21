# Привязки Python и C++ (поверх C API)

**Контракт** для нативного кода — это **`include/liner.h`** плюс **`cdylib`**, получаемый командой **`cargo build --release`**. Поставляемые слои **Python** и **C++** — **примеры**: они тонко оборачивают подмножество C API. Считайте их отправной точкой, а не вторым источником истины по символам.

По семантике жизненного цикла (`run`, `stop`, advertise, last error, интроспекция, лимиты) опирайтесь на [using-the-api.md](using-the-api.md) и [errors-and-logging.md](errors-and-logging.md), а не на догадки по именам методов обёртки.

## Сначала соберите разделяемую библиотеку

См. [c-api-compatibility-and-build.md](c-api-compatibility-and-build.md). После release-сборки библиотека лежит в **`target/release/`** (имя зависит от ОС, например `libliner_broker.so` для Linux GNU).

## C++

- **Заголовок:** компилируйте с **`include/liner.h`**. Пример класса подключает его через **`cpp/liner_broker.h`** (`#include "../include/liner.h"`).
- **Строка линковки (пример Unix):** в **`cpp/Makefile`** используется  
  `-L ../target/release -lliner_broker`  
  и сборка каждого `*.cpp` рядом с Makefile. Запускайте **`make`** из **`cpp/`** после `cargo build --release`.
- **Windows:** следуйте правилам вашего тулчейна для линковки DLL / import library **`liner_broker`** из `target/release` (флаги отличаются от `g++` на Linux); см. документ о совместимости.
- **Строки:** `std::string`, передаваемые как `.c_str()`, не должны содержать встроенных **NUL**; топики и адреса — C-строки.
- **SQLite:** пример на C++ использует только **`lnr_new_client_redis`**. Для SQLite вызывайте **`lnr_new_client_sqlite`** с пятью C-строками (`unique_name`, `topic`, `localhost`, `sqlite_path`, **`receivers_json`**) или расширьте обёртку. Передавайте **`NULL`** или **`"[]"`** для `receivers_json`, если каталог уже есть в файле БД. Полный разбор: [using-sqlite.md](using-sqlite.md).
- **`sendTo` / `sendAll`:** обёртка пробрасывает **`at_least_once_delivery`** в C API (необязательный третий аргумент C++, по умолчанию **`true`**). Используйте **`false`**, если у каждого процесса свой файл SQLite и ack не разделяются — см. [using-sqlite.md](using-sqlite.md) (*Изолированные файлы и `at_least_once_delivery`*).
- **Жизненный цикл:** `LinerBroker` вызывает **`lnr_new_client_redis`** в конструкторе и **`lnr_delete_client`** в деструкторе. Предпочтительно явный **`lnr_stop`** (или эквивалент в обёртке) перед уничтожением, если нужно сбросить состояние / перезапустить на том же handle. Не уничтожайте объект, пока на другом потоке логически активен **`lnr_run`**, если не согласовали остановку. Вызывайте **`lnr_run`** до **`lnr_send_to`** / **`lnr_send_all`**; см. [using-the-api.md](using-the-api.md).
- **Аддитивные C-хелперы (1.4.0):** если пример класса их ещё не оборачивает, вызывайте напрямую из `liner.h`: `lnr_last_error_code`, `lnr_set_advertise_addr`, `lnr_stop`, `lnr_is_running`, геттеры (`lnr_bound_listen_addr`, `lnr_published_addr`), `lnr_set_log_cb`, `lnr_list_addresses`, `lnr_pending_count`, сеттеры/геттеры размеров. Список символов: [c-api-compatibility-and-build.md](c-api-compatibility-and-build.md).

## Python

- **Загрузите библиотеку один раз:** `liner.loadLib(path)` должен выполниться до создания **`liner.Client`**. `path` — полный путь к разделяемой библиотеке (`.so` / `.dylib` / `.dll`), не имя Rust-крейта.
- **Поставляемый `python/liner.py`:** **`Client`** / **`new_redis`** используют **`lnr_new_client_redis`**; **`new_sqlite`** вызывает **`lnr_new_client_sqlite`** (пять строк, включая **`receivers_json`**); **`new_postgres`** вызывает **`lnr_new_client_postgres`** (четыре строки, общий URL — библиотека должна быть собрана с **`--features postgres`**). См. [using-sqlite.md](using-sqlite.md) и [using-postgres.md](using-postgres.md).
- **Завершение:** **`Client.close()`** вызывает **`lnr_delete_client`**. Предпочтительно **`with Client(...) as c:`**, чтобы `close` выполнился при выходе. Используйте **`stop()`**, когда нужно снять регистрацию и дождаться потоков без уничтожения handle (после этого снова допустимы `clear_*` / `run`). Если процесс завершится без `close`, вы полагаетесь на разбор процесса (рискованно для аккуратной остановки потоков).
- **Новые хелперы в примере `Client`:** `last_error_code`, `set_advertise_addr`, `stop`, `is_running`, `bound_listen_addr`, `published_addr`, `list_addresses`, `pending_count`. На уровне модуля: `set_log_callback`, `set_max_message_size` / `get_max_message_size`, `set_compress_threshold` / `get_compress_threshold`.
- **Колбэки:** `run` устанавливает колбэк **`CFUNCTYPE`**, сохранённый в **`self.recvCBack_`**, чтобы его не собрал GC, пока Rust может вызывать его. Держите колбэк **коротким**; тяжёлая работа может задержать I/O внутри библиотеки. То же для status- и log-колбэков.
- **Потоки:** библиотека выполняет работу listener/sender на своих потоках; колбэки приёма могут вызываться с этих путей. Избегайте рекурсивного вызова того же **`Client`** из колбэка способом, который может привести к **взаимной блокировке** с вашими блокировками. При необходимости ставьте работу в очередь другому потоку.
- **Данные:** в примере `send_to` / `send_all` используют **`bytearray`**; другие типы буферов могут потребовать копирования в форму, которую ctypes может закрепить на время вызова.
- **`at_least_once_delivery`:** необязательный третий аргумент **`send_to`** / **`send_all`** (по умолчанию **`True`**). Установите **`False`** для отправок между пирами, когда у каждого процесса **свой** путь SQLite; иначе sender может держать неподтверждённый трафик в RAM (то же правило, что в C / Rust — [using-sqlite.md](using-sqlite.md)).

## Синхронизация привязок с библиотекой

При обновлении **`liner_broker`** пересоберите `cdylib`, обновите **`include/liner.h`** в своём дереве и перезапустите тесты. Ожидания по ABI и аддитивный список символов для **1.4.0** — в [c-api-compatibility-and-build.md](c-api-compatibility-and-build.md).

## См. также

- [using-the-api.md](using-the-api.md) — порядок `run` / `stop`, advertise, интроспекция, заметки о потоках.
- [errors-and-logging.md](errors-and-logging.md) — `NULL` / `FALSE`, коды last-error, log hook.
- [troubleshooting.md](troubleshooting.md) — краткий указатель по симптомам.
