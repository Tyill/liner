//
// Liner project
// Copyright (C) 2024 by Contributors <https://github.com/Tyill/liner>
//
// This code is licensed under the MIT License.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files(the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and / or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions :
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//

#ifndef LINER_C_API_H_
#define LINER_C_API_H_

#define LINER_API

#include <stddef.h>

#if defined(__cplusplus)
extern "C" {
#endif /* __cplusplus */

typedef enum BOOL{ FALSE = 0, TRUE = 1}BOOL;

typedef void* lnr_uData;
typedef void(*lnr_receive_cb)(const char* to, const char* from, const char* data, size_t data_size, lnr_uData);

typedef void* lnr_hClient;

/// Crate / shared-library version string (e.g. "1.4.0"), static storage.
LINER_API const char* lnr_version(void);

/// Create new client backed by Redis.
/// @param unique_name
/// @param topic - current topic
/// @param localhost - local ip
/// @param redis_url - Redis connection URL (e.g. redis://127.0.0.1/)
/// @return lnr_hClient
LINER_API lnr_hClient lnr_new_client_redis(const char* unique_name, const char* topic, const char* localhost, const char* redis_url);

/// Create new client backed by SQLite (single database file).
/// @param unique_name
/// @param topic - current topic
/// @param localhost - local ip
/// @param sqlite_path - path to SQLite database file
/// @param receivers_json - optional UTF-8 JSON array of {topic,addr,client_name}; NULL, "", whitespace, or "[]" skips seeding. For SQLite: list peers only — `topic` is their registered topic. With one shared SQLite file for all peers, prefer "" so live rows come from the store.
/// @return lnr_hClient
LINER_API lnr_hClient lnr_new_client_sqlite(const char* unique_name, const char* topic, const char* localhost, const char* sqlite_path, const char* receivers_json);

/// Create new client backed by PostgreSQL (libpq URL, e.g. postgresql://user:pass@127.0.0.1/liner).
/// Available only when liner_broker was built with Cargo feature `postgres` (`--features postgres`).
LINER_API lnr_hClient lnr_new_client_postgres(const char* unique_name, const char* topic, const char* localhost, const char* postgres_url);

#if defined(__GNUC__) || defined(__clang__)
#define LINER_DEPRECATED __attribute__((deprecated))
#elif defined(_MSC_VER)
#define LINER_DEPRECATED __declspec(deprecated)
#else
#define LINER_DEPRECATED
#endif

/// Deprecated: use lnr_new_client_redis (same behavior).
/// @param unique_name
/// @param topic - current topic
/// @param localhost - local ip
/// @param redis_path - Redis connection URL
/// @return lnr_hClient
LINER_API LINER_DEPRECATED lnr_hClient lnr_new_client(const char* unique_name, const char* topic, const char* localhost, const char* redis_path);

/// Run transfer data
/// @param lnr_hClient
/// @param receive_cb - callback for receive data from other topics
/// @return true - ok
LINER_API BOOL lnr_run(lnr_hClient client, lnr_receive_cb receive_cb, lnr_uData);

/// Send data to other topic - only to one
/// @param lnr_hClient
/// @param topic - other topic
/// @param data
/// @param data_size
/// @param at_least_once_delivery - if TRUE, requires a shared store (e.g. one Redis URL or one SQLite path) so listener acks and sender reads the same conn_mess_number; with a different SQLite file per process, prefer FALSE
/// @return true - ok
LINER_API BOOL lnr_send_to(lnr_hClient client,
                          const char* topic,
                          const char* data, size_t data_size,
                          BOOL at_least_once_delivery);

/// Send data to other topics - broadcast
/// @param lnr_hClient
/// @param topic - other topic
/// @param data
/// @param data_size
/// @param at_least_once_delivery - same semantics as lnr_send_to
/// @return true - ok
LINER_API BOOL lnr_send_all(lnr_hClient client,
                          const char* topic,
                          const char* data, size_t data_size,
                          BOOL at_least_once_delivery);

/// Subscribe on topic for broadcast
/// @param lnr_hClient
/// @param topic
/// @return true - ok
LINER_API BOOL lnr_subscribe(lnr_hClient client, const char* topic);

/// Unsubscribe on topic for broadcast
/// @param lnr_hClient
/// @param topic
/// @return true - ok
LINER_API BOOL lnr_unsubscribe(lnr_hClient client, const char* topic);

/// Delete client
/// @param lnr_hClient
/// @return true - ok
LINER_API BOOL lnr_delete_client(lnr_hClient client);
  


/// Extended API

/// Last sync-API error code for this client (`LNR_OK` after success). See `LNR_ERR_*`.
LINER_API int lnr_last_error_code(lnr_hClient client);

/// Detail for last sync failure. Empty when OK. NULL if bad handle. Valid until next sync call or delete.
LINER_API const char* lnr_last_error_message(lnr_hClient client);
 
/// Stop listener/sender and unregister from the store (idempotent). Allows `clear_*` / `run` again.
LINER_API BOOL lnr_stop(lnr_hClient client);

/// Whether the client is currently running (`lnr_run` succeeded and not yet stopped).
LINER_API BOOL lnr_is_running(lnr_hClient client);

/// Refresh address of topic (actual for new clients)
/// @param lnr_hClient
/// @param topic
/// @return true - ok
LINER_API BOOL lnr_refresh_address_topic(lnr_hClient client, const char* topic);

/// Clear stored messages
/// @param lnr_hClient
/// @return true - ok
LINER_API BOOL lnr_clear_stored_messages(lnr_hClient client);

/// Clear addresses of topic
/// @param lnr_hClient
/// @return true - ok
LINER_API BOOL lnr_clear_addresses_of_topic(lnr_hClient client);

/// Status / background-error callback kinds (passed as `kind` to `lnr_status_cb`).
enum {
    LNR_PEER_CONNECTED = 1,
    LNR_PEER_DISCONNECTED = 2,
    LNR_PEER_SUBSCRIBED = 3,
    LNR_PEER_UNSUBSCRIBED = 4,
    /** Sender: TCP connect fail or stream close. */
    LNR_SENDER_ROUTE_LOST = 5,
    /** Sender: background store error (reconnect / persist). */
    LNR_SENDER_STORE_ERROR = 6,
    /** Sender: write/flush failure after an accepted send. */
    LNR_SENDER_SEND_ERROR = 7,
    /** Listener: background store error (ack / lookup). */
    LNR_LISTENER_STORE_ERROR = 8,
    /** Sender: in-memory send queue full for a peer (`max_send_queue`). */
    LNR_SENDER_BUSY = 9
};

/// Sync API last-error codes (`lnr_last_error_code`). Detail text still goes to stderr / log hook / `lnr_last_error_message`.
enum {
    LNR_OK = 0,
    LNR_ERR_NOT_RUNNING = 1,
    LNR_ERR_ALREADY_RUNNING = 2,
    LNR_ERR_SELF_TOPIC = 3,
    LNR_ERR_INTERNAL_TOPIC = 4,
    LNR_ERR_NO_ADDR = 5,
    LNR_ERR_BIND = 6,
    LNR_ERR_STORE = 7,
    LNR_ERR_INVALID_ARG = 8,
    LNR_ERR_CLEAR_WHILE_RUNNING = 9,
    /** Listener startup after TCP bind (mio / topic_key). */
    LNR_ERR_STARTUP = 10,
    /** Sender in-memory queue full for a peer. */
    LNR_ERR_BUSY = 11
};

/// Asynchronous status and background errors. Pointers are valid only for the duration of the call.
/// Peer events are filtered to topics this client has sent to, subscribed to, or refreshed.
typedef void(*lnr_status_cb)(int kind, const char* topic, const char* peer, const char* message, lnr_uData);

/// Set or clear the status / background-error callback (additive; does not change `lnr_run`).
/// Pass `cb == NULL` to clear. Safe before or after `lnr_run`.
/// @return true - ok
LINER_API BOOL lnr_set_status_cb(lnr_hClient client, lnr_status_cb cb, lnr_uData);

/// Process-global error log callback. Pass `cb == NULL` to restore stderr. Not per-client.
typedef void(*lnr_log_cb)(const char* message, lnr_uData);
LINER_API BOOL lnr_set_log_cb(lnr_log_cb cb, lnr_uData);

/// Callback for [`lnr_list_addresses`]: one call per (addr, unique_name) row.
typedef void(*lnr_addr_cb)(const char* addr, const char* unique_name, lnr_uData);

/// List topic directory from the store (empty topic → success with zero callbacks).
LINER_API BOOL lnr_list_addresses(lnr_hClient client, const char* topic, lnr_addr_cb cb, lnr_uData);

/// Sum of offline queued messages for this sender identity. `-1` on error (see `lnr_last_error_code`).
LINER_API long long lnr_pending_count(lnr_hClient client);

/// Per-peer offline queue rows for this sender. Empty → success with zero callbacks.
typedef void(*lnr_pending_cb)(const char* addr, const char* topic, const char* unique_name, long long count, lnr_uData);
LINER_API BOOL lnr_pending_by_peer(lnr_hClient client, lnr_pending_cb cb, lnr_uData);

/// Total in-memory sender queue depth (not store/offline). `0` if not running / null handle.
LINER_API long long lnr_send_queue_depth(lnr_hClient client);

/// Per-peer in-memory sender queue depth for known routes. Empty → success with zero callbacks.
typedef void(*lnr_queue_cb)(const char* addr, long long count, lnr_uData);
LINER_API BOOL lnr_send_queue_depth_by_peer(lnr_hClient client, lnr_queue_cb cb, lnr_uData);

/// App subscriptions (excludes internal channel) / related topics (status filter set).
typedef void(*lnr_topic_cb)(const char* topic, lnr_uData);
LINER_API BOOL lnr_list_subscriptions(lnr_hClient client, lnr_topic_cb cb, lnr_uData);
LINER_API BOOL lnr_list_related_topics(lnr_hClient client, lnr_topic_cb cb, lnr_uData);

/// Max framed TCP message size in bytes (default 1GiB). Prefer before `lnr_run`.
LINER_API BOOL lnr_set_max_message_size(size_t bytes);
LINER_API size_t lnr_get_max_message_size(void);

/// Min payload size (bytes) before zstd is attempted (default 1MiB). Prefer before `lnr_run`.
LINER_API BOOL lnr_set_compress_threshold(size_t bytes);
LINER_API size_t lnr_get_compress_threshold(void);

/// Max in-memory sender messages per peer (`0` = unlimited, default). Prefer before `lnr_run`.
LINER_API BOOL lnr_set_max_send_queue(size_t n);
LINER_API size_t lnr_get_max_send_queue(void);

/// Stream availability poll interval (ms). Prefer before `lnr_run`. `0` rejected.
LINER_API BOOL lnr_set_stream_check_timeout_ms(unsigned long long ms);
LINER_API unsigned long long lnr_get_stream_check_timeout_ms(void);

/// Bytestream would-block wait (ms). Prefer before `lnr_run`. `0` rejected.
LINER_API BOOL lnr_set_would_block_timeout_ms(unsigned long long ms);
LINER_API unsigned long long lnr_get_would_block_timeout_ms(void);

/// Optional address published to the store catalog instead of the bind string.
/// Call before `lnr_run`. `NULL` or `""` clears. Fails with `LNR_ERR_ALREADY_RUNNING` while running.
LINER_API BOOL lnr_set_advertise_addr(lnr_hClient client, const char* addr);

/// Client `unique_name` (owned by client; valid until `lnr_delete_client`).
LINER_API const char* lnr_unique_name(lnr_hClient client);

/// Source topic from the constructor (owned by client; valid until delete).
LINER_API const char* lnr_topic(lnr_hClient client);

/// Constructor TCP bind string (`localhost` argument). Not rewritten by ephemeral bind.
LINER_API const char* lnr_bind_addr(lnr_hClient client);

/// Configured advertise string; NULL if never set / cleared. Independent of `lnr_published_addr`.
LINER_API const char* lnr_advertise_addr(lnr_hClient client);

/// Last successful bind address (kept after `lnr_stop`); NULL if never run.
LINER_API const char* lnr_bound_listen_addr(lnr_hClient client);

/// Address currently in the store catalog; NULL after `lnr_stop` / before first successful `lnr_run`.
LINER_API const char* lnr_published_addr(lnr_hClient client);


#if defined(__cplusplus)
}
#endif /* __cplusplus */

#endif /* LINER_C_API_H_ */