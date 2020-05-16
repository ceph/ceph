/*
 * Copyright 2020 Liu Changcheng <changcheng.liu@aliyun.com>
 * Author: Liu Changcheng <changcheng.liu@aliyun.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SPEC_BASE64_H
#define SPEC_BASE64_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef int (*base64_encode_func_t)(char *dst, const char *dst_end, const char *src, const char *src_end);
typedef int (*base64_decode_func_t)(char *dst, const char *dst_end, const char *src, const char *src_end);

/* global static to choose base64 encode/decode implementation on the given architecture. */
extern base64_encode_func_t arch_encode_base64;
extern base64_decode_func_t arch_decode_base64;

extern base64_encode_func_t choose_encode_base64(void);
extern base64_decode_func_t choose_decode_base64(void);

static inline
int ceph_armor(char *dst, const char *dst_end, const char *src, const char *src_end) {
    return arch_encode_base64(dst, dst_end, src, src_end);
}

static inline
int ceph_unarmor(char *dst, const char *dst_end, const char *src, const char *src_end) {
    return arch_decode_base64(dst, dst_end, src, src_end);
}

#ifdef __cplusplus
}
#endif

#endif //SPEC_BASE64_H
