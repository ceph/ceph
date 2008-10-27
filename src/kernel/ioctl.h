#ifndef FS_CEPH_IOCTL_H
#define FS_CEPH_IOCTL_H

#include <linux/ioctl.h>
#include "types.h"

#define CEPH_IOCTL_MAGIC 0x97

#define CEPH_IOC_GET_LAYOUT _IOR(CEPH_IOCTL_MAGIC, 1, struct ceph_file_layout)
#define CEPH_IOC_SET_LAYOUT _IOW(CEPH_IOCTL_MAGIC, 2, struct ceph_file_layout)

#endif
