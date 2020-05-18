#ifndef CEPH_ARCH_INTEL_H
#define CEPH_ARCH_INTEL_H

#ifdef __cplusplus
extern "C" {
#endif

extern int ceph_arch_intel_pclmul; /* true if we have PCLMUL features */
extern int ceph_arch_intel_sse42;  /* true if we have sse 4.2 features */
extern int ceph_arch_intel_sse41;  /* true if we have sse 4.1 features */
extern int ceph_arch_intel_ssse3;  /* true if we have ssse 3 features */
extern int ceph_arch_intel_sse3;   /* true if we have sse 3 features */
extern int ceph_arch_intel_sse2;   /* true if we have sse 2 features */
extern int ceph_arch_intel_aesni;  /* true if we have aesni features */
extern int ceph_arch_intel_avx;    /* ture if we have avx features */
extern int ceph_arch_intel_avx2;   /* ture if we have avx2 features */
extern int ceph_arch_intel_avx512f;  /* ture if we have avx512 foudation features */
extern int ceph_arch_intel_avx512er; /* ture if we have 28-bit precision RCP, RSQRT and EXP transcendentals features */
extern int ceph_arch_intel_avx512pf; /* ture if we have avx512 prefetch features */
extern int ceph_arch_intel_avx512vl; /* ture if we have avx512 variable length features */
extern int ceph_arch_intel_avx512cd; /* ture if we have avx512 conflictdetection features */
extern int ceph_arch_intel_avx512dq; /* ture if we have new 32-bit and 64-bit AVX-512 instructions */
extern int ceph_arch_intel_avx512bw; /* ture if we have new 8-bit and 16-bit AVX-512 instructions */

extern int ceph_arch_intel_probe(void);

#ifdef __cplusplus
}
#endif

#endif
