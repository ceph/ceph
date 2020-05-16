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

#include "armor.h"
#include "base64/base64_plain.h"

/* choose best implementation based on the CPU architecture.  */
base64_encode_func_t choose_encode_base64(void) {
    return plain_armor;
}

base64_decode_func_t choose_decode_base64(void) {
    return plain_unarmor;
}

// c++ tricky: initialize global var first
base64_encode_func_t arch_encode_base64 = choose_encode_base64();
base64_decode_func_t arch_decode_base64 = choose_decode_base64();
