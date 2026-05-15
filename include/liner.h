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

/// Delete client
/// @param lnr_hClient
/// @return true - ok
LINER_API BOOL lnr_delete_client(lnr_hClient client);
  
 
#if defined(__cplusplus)
}
#endif /* __cplusplus */

#endif /* LINER_C_API_H_ */