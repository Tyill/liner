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

#include <stdint.h>

#if defined(__cplusplus)
extern "C" {
#endif /* __cplusplus */

typedef enum BOOL{ FALSE = 0, TRUE = 1}BOOL;

typedef void(*ln_receive_cb)(char* to, char* from, char* data, uint64_t data_size);

typedef void* hClient;

LINER_API hClient ln_new_client(char* unique_name, char* redis_path);

LINER_API BOOL ln_is_init_client(hClient* client);

LINER_API BOOL ln_run(hClient* client, char* topic, char* localhost, ln_receive_cb receive_cb);

LINER_API BOOL ln_send_to(hClient* client,
                          char* topic,
                          char* data, uint64_t data_size,
                          BOOL at_least_once_delivery);

LINER_API BOOL ln_send_all(hClient* client,
                          char* topic,
                          char* data, uint64_t data_size,
                          BOOL at_least_once_delivery);

LINER_API BOOL ln_subscribe(hClient* client, char* topic);

LINER_API BOOL ln_unsubscribe(hClient* client, char* topic);

LINER_API BOOL ln_delete_client(hClient client);
  
 
#if defined(__cplusplus)
}
#endif /* __cplusplus */

#endif /* LINER_C_API_H_ */