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
#include "arch/probe.h"

/* flags we export */
int ceph_arch_intel_avx512_vpclmul = 0;
int ceph_arch_intel_pclmul = 0;
int ceph_arch_intel_sse42 = 0;
int ceph_arch_intel_sse41 = 0;
int ceph_arch_intel_ssse3 = 0;
int ceph_arch_intel_sse3 = 0;
int ceph_arch_intel_sse2 = 0;
int ceph_arch_intel_aesni = 0;

#ifdef __x86_64__
#include <cpuid.h>
#include <x86intrin.h>

/* http://en.wikipedia.org/wiki/CPUID#EAX.3D1:_Processor_Info_and_Feature_Bits */

#define CPUID_PCLMUL	(1 << 1)
#define CPUID_SSE42	(1 << 20)
#define CPUID_SSE41	(1 << 19)
#define CPUID_SSSE3	(1 << 9)
#define CPUID_SSE3	(1)
#define CPUID_SSE2	(1 << 26)
#define CPUID_AESNI 	(1 << 25)
#define CPUID_OSXSAVE	(1 << 27)

/* AVX512F:[16] DQ:[17] CD:[28] BW:[30] VL:[31] */
#define CPUID7_AVX512_EBX	(0xD0030000UL)
/* AVX512VBMI2:[6] GFNI:[8] VAES:[9] VPCLMULQDQ:[10] VNNI:[11] BITALG:[12] VPOPCNTDQ:[14] */
#define CPUID7_AVX512_ECX	(0x00005F40UL)
/* SSE:[1] AVX:[2] Opmask:[5] ZMM_HI256:[6] ZMM16-31:[7]*/
#define XGETBV_AVX512		(0x000000E6ULL)

__attribute__((__target__("avx")))	/* For _xgetbv() */
int ceph_arch_intel_probe(void)
{
	/* i know how to check this on x86_64... */
	unsigned int eax, ebx, ecx = 0, edx = 0;
	unsigned int eax_7 = 0, ebx_7 = 0, ecx_7 = 0, edx_7 = 0;
	if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx) ||
	    !__get_cpuid_count(7, 0, &eax_7, &ebx_7, &ecx_7, &edx_7)) {
	  return 1;
	}
	if ((ecx & CPUID_OSXSAVE) != 0) {
		unsigned long long xcr0 = _xgetbv(0);
		if (((xcr0  & XGETBV_AVX512)     == XGETBV_AVX512) &&
		    ((ebx_7 & CPUID7_AVX512_EBX) == CPUID7_AVX512_EBX) &&
		    ((ecx_7 & CPUID7_AVX512_ECX) == CPUID7_AVX512_ECX)) {
			ceph_arch_intel_avx512_vpclmul = 1;
		}
	}
	if ((ecx & CPUID_PCLMUL) != 0) {
		ceph_arch_intel_pclmul = 1;
	}
	if ((ecx & CPUID_SSE42) != 0) {
		ceph_arch_intel_sse42 = 1;
	}
	if ((ecx & CPUID_SSE41) != 0) {
		ceph_arch_intel_sse41 = 1;
	}
	if ((ecx & CPUID_SSSE3) != 0) {
	        ceph_arch_intel_ssse3 = 1;
	}
	if ((ecx & CPUID_SSE3) != 0) {
	        ceph_arch_intel_sse3 = 1;
	}
	if ((edx & CPUID_SSE2) != 0) {
	        ceph_arch_intel_sse2 = 1;
	}
  if ((ecx & CPUID_AESNI) != 0) {
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
