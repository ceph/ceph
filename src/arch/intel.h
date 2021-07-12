#ifndef CEPH_ARCH_INTEL_H
#define CEPH_ARCH_INTEL_H

#ifdef __cplusplus
extern "C" {
#endif

extern int ceph_arch_intel_pclmul; /* true: have PCLMUL features */
extern int ceph_arch_intel_sse42;  /* true: have sse 4.2 features */
extern int ceph_arch_intel_sse41;  /* true: have sse 4.1 features */
extern int ceph_arch_intel_ssse3;  /* true: have ssse 3 features */
extern int ceph_arch_intel_sse3;   /* true: have sse 3 features */
extern int ceph_arch_intel_sse2;   /* true: have sse 2 features */
extern int ceph_arch_intel_aesni;  /* true: have aesni features */
extern int ceph_arch_intel_avx;    /* ture: have avx features */
extern int ceph_arch_intel_avx2;   /* ture: have avx2 features */
extern int ceph_arch_intel_avx512f;  /* true: have avx512 foudation features */
extern int ceph_arch_intel_avx512er; /* true: have 28-bit precision RCP, RSQRT and EXP transcendentals features */
extern int ceph_arch_intel_avx512pf; /* true: have avx512 prefetch features */
extern int ceph_arch_intel_avx512vl; /* true: have avx512 variable length features */
extern int ceph_arch_intel_avx512cd; /* true: have avx512 conflictdetection features */
extern int ceph_arch_intel_avx512dq; /* true: have new 32-bit and 64-bit AVX-512 instructions */
extern int ceph_arch_intel_avx512bw; /* true: have new 8-bit and 16-bit AVX-512 instructions */
extern int ceph_arch_intel_avx512ifma;   /* true: have avx512 Integer Fused Multiply-Add features */
extern int ceph_arch_intel_avx512vbmi;   /* true: have avx512 vector bit manipulation, version 1 */
extern int ceph_arch_intel_avx512vbmi2;  /* true: have avx512 vector bit manipulation, version 2 */
extern int ceph_arch_intel_avx512vaes;   /* true: have avx512 vector AES instructions */
extern int ceph_arch_intel_avx512bitalg; /* true: have avx512 bit algorithm operations */
extern int ceph_arch_intel_avx512vpopcntdq; /* true: have avx512 double and quad word population count instructions */
extern int ceph_arch_intel_avx512vnni;   /* ture: have avx512 vector Neural Network Instructions */
extern int ceph_arch_intel_avx5124vnniw; /* true: have avx512 4-iteration vector Neural Network Instructions */
extern int ceph_arch_intel_avx5124fmaps; /* true: have avx512 4-iteration Fused Multiply Accumulation Instructions */

extern int ceph_arch_intel_probe(void);

#ifdef __cplusplus
}
#endif

#endif
