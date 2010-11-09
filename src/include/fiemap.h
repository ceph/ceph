#ifndef __CEPH_FIEMAP_H
#define __CEPH_FIEMAP_H

#include "config.h"

#ifdef HAVE_FIEMAP_H

#include <linux/fiemap.h>

extern "C" struct fiemap *read_fiemap(int fd);

#else

/*
 the following structures differ from the original structures.
 Using it for the fiemap ioctl will not work.
*/
struct fiemap_extent {
  __u64 fe_logical;
  __u64 fe_physical;
  __u64 fe_length;
  __u32 fe_flags;
};

struct fiemap {
  __u64 fm_start;
  __u64 fm_length;
  __u32 fm_flags;
  __u32 fm_mapped_extents;
  __u32 fm_extent_count;
  struct fiemap_extent fm_extents[0];
};

static inline struct fiemap *read_fiemap(int fd)
{
  return NULL;
}

#endif


#endif
