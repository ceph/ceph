/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013,2014 Inktank Storage, Inc.
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */
#include <stdio.h>
#include <stdint.h>
#include "arch/probe.h"

/* flags we export */
int ceph_arch_intel_pclmul = 0;
int ceph_arch_intel_sse42 = 0;
int ceph_arch_intel_sse41 = 0;
int ceph_arch_intel_ssse3 = 0;
int ceph_arch_intel_sse3 = 0;
int ceph_arch_intel_sse2 = 0;
int ceph_arch_intel_aesni = 0;

#ifdef __x86_64__
#include <cpuid.h>

int ceph_arch_intel_probe(void)
{
    uint32_t eax = 0, ebx = 0, ecx = 0, edx = 0;

    uint32_t max_level = __get_cpuid_max(0, NULL);
    if (max_level == 0) {
        return 0;
    }

    __cpuid_count(1, 0, eax, ebx, ecx, edx);
    if ((ecx & bit_PCLMUL) != 0) {
        ceph_arch_intel_pclmul = 1;
    }
    if ((ecx & bit_SSE4_2) != 0) {
        ceph_arch_intel_sse42 = 1;
    }
    if ((ecx & bit_SSE4_1) != 0) {
        ceph_arch_intel_sse41 = 1;
    }
    if ((ecx & bit_SSSE3) != 0) {
        ceph_arch_intel_ssse3 = 1;
    }
    if ((ecx & bit_SSE3) != 0) {
        ceph_arch_intel_sse3 = 1;
    }
    if ((edx & bit_SSE2) != 0) {
        ceph_arch_intel_sse2 = 1;
    }
    if ((ecx & bit_AES) != 0) {
        ceph_arch_intel_aesni = 1;
    }

    return 0;
}

#else // __x86_64__

int ceph_arch_intel_probe(void)
{
    /* no features */
    return 0;
}

#endif // __x86_64__
