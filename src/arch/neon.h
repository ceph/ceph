#ifndef CEPH_ARCH_NEON_H
#define CEPH_ARCH_NEON_H

#ifdef __cplusplus
extern "C" {
#endif

extern int ceph_arch_neon;  /* true if we have ARM NEON abilities */

extern int ceph_arch_neon_probe(void);

#ifdef __cplusplus
}
#endif

#endif
