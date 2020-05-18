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

#ifndef CEPH_ARCH_INTEL_SIMD_H
#define CEPH_ARCH_INTEL_SIMD_H

#ifdef __cplusplus
extern "C" {
#endif

extern int spec_arch_intel_avx;      /* ture if we have avx features */
extern int spec_arch_intel_avx2;     /* ture if we have avx2 features */
extern int spec_arch_intel_avx512f;  /* ture if we have avx512 foudation features */
extern int spec_arch_intel_avx512er; /* ture if we have 28-bit precision RCP, RSQRT and EXP transcendentals features */
extern int spec_arch_intel_avx512pf; /* ture if we have avx512 prefetch features */
extern int spec_arch_intel_avx512vl; /* ture if we have avx512 variable length features */
extern int spec_arch_intel_avx512cd; /* ture if we have avx512 conflictdetection features */
extern int spec_arch_intel_avx512dq; /* ture if we have new 32-bit and 64-bit AVX-512 instructions */
extern int spec_arch_intel_avx512bw; /* ture if we have new 8-bit and 16-bit AVX-512 instructions */

extern int spec_arch_intel_probe(void);

#ifdef __cplusplus
}
#endif

#endif //CEPH_ARCH_INTEL_SIMD_H
