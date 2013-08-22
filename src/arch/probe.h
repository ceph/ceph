#ifndef CEPH_ARCH_PROBE_H
#define CEPH_ARCH_PROBE_H

#ifdef __cplusplus
extern "C" {
#endif

extern int ceph_arch_probed;  /* non-zero if we've probed features */

extern int ceph_arch_probe(void);

#ifdef __cplusplus
}
#endif

#endif
