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

extern int ceph_arch_intel_probe(void);

#ifdef __cplusplus
}
#endif

#endif
