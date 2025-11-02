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

/// Create new client
/// @param unique_name
/// @param topic - current topic
/// @param localhost - local ip
/// @param redis_path
/// @return lnr_hClient
LINER_API lnr_hClient lnr_new_client(const char* unique_name, const char* topic, const char* localhost, const char* redis_path);

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
/// @param at_least_once_delivery
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
/// @param at_least_once_delivery
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