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

#include "armor.h"
#include "base64/base64_plain.h"
#include "base64/probe_arch.h"
#include "base64/intel_simd.h"
#include "base64/avx2/base64_intel_avx2.h"

base64_encode_func_t arch_encode_base64 = nullptr;
base64_decode_func_t arch_decode_base64 = nullptr;

/* choose best implementation based on the CPU architecture. */
__attribute__ ((constructor))
static void choose_base64_encode_decode(void) {
  // probe cpu features
  spec_probe_arch();

  // use the fast version if the CPU support and being compiled.
  #if defined(__i386__) || defined(__x86_64__)
  if (spec_arch_intel_avx2 && base64_intel_avx2_exists()) {
    arch_encode_base64 = arch_intel_avx2_encode_base64;
    arch_decode_base64 = arch_intel_avx2_decode_base64;
    return;
  }
  #endif

  arch_encode_base64 = plain_armor;
  arch_decode_base64 = plain_unarmor;
}
