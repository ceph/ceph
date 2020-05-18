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

#include <stdio.h>
#include <stdint.h>
#include "intel_simd.h"

int spec_arch_intel_avx = 0;
int spec_arch_intel_avx2 = 0;
int spec_arch_intel_avx512f = 0;
int spec_arch_intel_avx512er = 0;
int spec_arch_intel_avx512pf = 0;
int spec_arch_intel_avx512vl = 0;
int spec_arch_intel_avx512cd = 0;
int spec_arch_intel_avx512dq = 0;
int spec_arch_intel_avx512bw = 0;

#ifdef __x86_64__
#include <cpuid.h>

#ifndef _XCR_XFEATURE_ENABLED_MASK
#define _XCR_XFEATURE_ENABLED_MASK 0
#endif

#ifndef _XCR_XMM_YMM_STATE_ENABLED_BY_OS
#define _XCR_XMM_YMM_STATE_ENABLED_BY_OS 0x6
#endif

#ifndef _XCR_XMM_YMM_ZMM_STATE_ENABLED_BY_OS
#define _XCR_XMM_YMM_ZMM_STATE_ENABLED_BY_OS \
        ((0x3 << 5) | 0x6)
#endif

static inline int64_t _xgetbv(uint32_t index) {
  uint32_t eax, edx;
  __asm__ __volatile__("xgetbv" : "=a"(eax), "=d"(edx) : "c"(index));
  return ((uint64_t)edx << 32) | eax;
}

static void detect_avx(void) {
  uint32_t eax = 0, ebx = 0, ecx = 0, edx = 0;

  uint32_t max_level = __get_cpuid_max(0, NULL);
  if (max_level == 0) {
    return;
  }
  __cpuid_count(1, 0, eax, ebx, ecx, edx);
  if ((ecx & bit_OSXSAVE) == 0 || (ecx & bit_AVX) == 0) {
    return;
  }

  int64_t xcr_mask = _xgetbv(_XCR_XFEATURE_ENABLED_MASK);
  if (xcr_mask & _XCR_XMM_YMM_STATE_ENABLED_BY_OS) {
    spec_arch_intel_avx = 1;
  }
}

static void detect_avx2(void) {
  uint32_t eax = 0, ebx = 0, ecx = 0, edx = 0;

  uint32_t max_level = __get_cpuid_max(0, NULL);
  if (max_level == 0) {
    return;
  }
  __cpuid_count(1, 0, eax, ebx, ecx, edx);
  if ((ecx & bit_OSXSAVE) == 0 || (ecx & bit_AVX) == 0) {
    return;
  }
  if (max_level < 7) {
    return;
  }
  __cpuid_count(7, 0, eax, ebx, ecx, edx);
  if ((ebx & bit_AVX2) == 0) {
    return;
  }
  int64_t xcr_mask = _xgetbv(_XCR_XFEATURE_ENABLED_MASK);
  if (xcr_mask & _XCR_XMM_YMM_STATE_ENABLED_BY_OS) {
    spec_arch_intel_avx2 = 1;
  }
}

static void detect_avx512(void) {
  uint32_t eax = 0, ebx = 0, ecx = 0, edx = 0;

  uint32_t max_level = __get_cpuid_max(0, NULL);
  if (max_level == 0) {
    return;
  }
  __cpuid_count(1, 0, eax, ebx, ecx, edx);
  if ((ecx & bit_OSXSAVE) == 0) {
    return;
  }
  int64_t xcr_mask = _xgetbv(_XCR_XFEATURE_ENABLED_MASK);
  if ((xcr_mask & _XCR_XMM_YMM_ZMM_STATE_ENABLED_BY_OS) == 0) {
    return;
  }
  if (max_level < 7) {
    return;
  }
  __cpuid_count(7, 0, eax, ebx, ecx, edx);
  if ((ebx & bit_AVX512F) == 0) {
    return;
  }
  spec_arch_intel_avx512f = 1;

  if(ebx & bit_AVX512ER) {
    spec_arch_intel_avx512er = 1;
  }
  if(ebx & bit_AVX512PF) {
    spec_arch_intel_avx512pf = 1;
  }

  if((ebx & bit_AVX512VL)== 0) {
    return;
  }
  spec_arch_intel_avx512vl = 1;

  if(ebx & bit_AVX512CD) {
    spec_arch_intel_avx512cd = 1;
  }
  if(ebx & bit_AVX512DQ) {
    spec_arch_intel_avx512dq = 1;
  }
  if (ebx & bit_AVX512BW) {
    spec_arch_intel_avx512bw = 1;
  }
}

int spec_arch_intel_probe(void) {
  uint32_t max_level = __get_cpuid_max(0, NULL);
  if (max_level == 0) {
    return 0;
  }

  detect_avx();
  detect_avx2();
  detect_avx512();

  return 0;
}

#else // __x86_64__

int spec_arch_intel_probe(void) {
  /* no features */
  return 0;
}

#endif // __x86_64__
