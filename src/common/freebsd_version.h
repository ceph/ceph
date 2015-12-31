#ifndef CEPH_FREEBSD_VERSION_H
#define CEPH_FREEBSD_VERSION_H

#ifdef __cplusplus
extern "C" {
#endif

#ifndef KERNEL_VERSION
# define KERNEL_VERSION(a,b,c) (((a) * 100*100) + ((b) * 100) + (c))
#endif

int get_freebsd_version(void);

#ifdef __cplusplus
}
#endif

#endif /* CEPH_FREEBSD_VERSION_H */
