#ifndef CEPH_LINUX_VERSION_H
#define CEPH_LINUX_VERSION_H

#ifdef __cplusplus
extern "C" {
#endif

#ifdef HAVE_LINUX_VERSION_H
# include <linux/version.h>
#endif

#ifndef KERNEL_VERSION
# define KERNEL_VERSION(a,b,c) (((a) << 16) + ((b) << 8) + (c))
#endif

int get_linux_version(void);

#ifdef __cplusplus
}
#endif

#endif /* CEPH_LINUX_VERSION_H */
