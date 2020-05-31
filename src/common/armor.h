/*
 * Copyright (C) 2020 Intel Corporation.
 * All rights reserved.
 *
 * Author: Changcheng Liu<changcheng.liu@intel.com>
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

#ifndef CEPH_BASE64_H
#define CEPH_BASE64_H

#include <stdint.h>
#include "base64/base64_plain.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef int (*base64_encode_func_t)(char *dst, const char *dst_end, const char *src, const char *src_end);
typedef int (*base64_decode_func_t)(char *dst, const char *dst_end, const char *src, const char *src_end);

/* global static to choose base64 encode/decode implementation on the given architecture. */
extern base64_encode_func_t arch_encode_base64;
extern base64_decode_func_t arch_decode_base64;

static inline
int ceph_armor(char *dst, const char *dst_end, const char *src, const char *src_end) {
  if((int)(src_end - src) < 256) {
    return plain_armor(dst, dst_end, src, src_end);
  } else {
    return arch_encode_base64(dst, dst_end, src, src_end);
  }
}

static inline
int ceph_unarmor(char *dst, const char *dst_end, const char *src, const char *src_end) {
  if((int)(src_end - src) < 256) {
    return plain_unarmor(dst, dst_end, src, src_end);
  } else {
    return arch_decode_base64(dst, dst_end, src, src_end);
  }
}

#ifdef __cplusplus
}
#endif

#endif //CEPH_BASE64_H
