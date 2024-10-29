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

typedef void* ln_uData;
typedef void(*ln_receive_cb)(char* to, char* from, char* data, size_t data_size, ln_uData);

typedef void* ln_hClient;

/// Create new client
/// @param unique_name
/// @param topic - current topic
/// @param localhost - local ip
/// @param redis_path
/// @return ln_hClient
LINER_API ln_hClient ln_new_client(char* unique_name, char* topic, char* localhost, char* redis_path);

/// Check has client
/// @return true - ok
LINER_API BOOL ln_has_client(ln_hClient* client);

/// Run transfer data
/// @param ln_hClient
/// @param receive_cb - callback for receive data from other topics
/// @return true - ok
LINER_API BOOL ln_run(ln_hClient* client, ln_receive_cb receive_cb, ln_uData);

/// Send data to other topic - only to one
/// @param ln_hClient
/// @param topic - other topic
/// @param data
/// @param data_size
/// @param at_least_once_delivery
/// @return true - ok
LINER_API BOOL ln_send_to(ln_hClient* client,
                          char* topic,
                          char* data, size_t data_size,
                          BOOL at_least_once_delivery);

/// Send data to other topics - broadcast
/// @param ln_hClient
/// @param topic - other topic
/// @param data
/// @param data_size
/// @param at_least_once_delivery
/// @return true - ok
LINER_API BOOL ln_send_all(ln_hClient* client,
                          char* topic,
                          char* data, size_t data_size,
                          BOOL at_least_once_delivery);

/// Subscribe on topic for broadcast
/// @param ln_hClient
/// @param topic
/// @return true - ok
LINER_API BOOL ln_subscribe(ln_hClient* client, char* topic);

/// Unsubscribe on topic for broadcast
/// @param ln_hClient
/// @param topic
/// @return true - ok
LINER_API BOOL ln_unsubscribe(ln_hClient* client, char* topic);

/// Clear stored messages
/// @param ln_hClient
/// @return true - ok
LINER_API BOOL ln_clear_stored_messages(ln_hClient* client);

/// Clear addresses of topic
/// @param ln_hClient
/// @return true - ok
LINER_API BOOL ln_clear_addresses_of_topic(ln_hClient* client);

/// Delete client
/// @param ln_hClient
/// @return true - ok
LINER_API BOOL ln_delete_client(ln_hClient client);
  
 
#if defined(__cplusplus)
}
#endif /* __cplusplus */

#endif /* LINER_C_API_H_ */