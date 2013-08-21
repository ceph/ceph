#ifndef CEPH_ARCH_INTEL_H
#define CEPH_ARCH_INTEL_H

#ifdef __cplusplus
extern "C" {
#endif

extern int ceph_arch_intel_sse42;  /* true if we have sse 4.2 features */

extern int ceph_arch_intel_probe(void);

#ifdef __cplusplus
}
#endif

#endif
