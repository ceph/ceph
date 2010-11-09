#ifndef __CEPH_FIEMAP_H
#define __CEPH_FIEMAP_H

#include "acconfig.h"

/*
 * the header is missing on most systems.  for the time being at
 * least, include our own copy in the repo.
 */
#ifdef HAVE_FIEMAP_H
# include <linux/fiemap.h>
#else
# include "linux_fiemap.h"
#endif

#include <linux/ioctl.h>
#ifndef FS_IOC_FIEMAP
# define FS_IOC_FIEMAP                        _IOWR('f', 11, struct fiemap)
#endif

extern "C" struct fiemap *read_fiemap(int fd);

#endif
