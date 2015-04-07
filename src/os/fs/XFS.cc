// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "XFS.h"

#include <xfs/xfs.h>

int XFS::set_alloc_hint(int fd, uint64_t val)
{
  struct fsxattr fsx;
  struct stat sb;
  int ret;

  if (fstat(fd, &sb) < 0) {
    ret = -errno;
    return ret;
  }
  if (!S_ISREG(sb.st_mode)) {
    return -EINVAL;
  }

  if (ioctl(fd, XFS_IOC_FSGETXATTR, &fsx) < 0) {
    ret = -errno;
    return ret;
  }

  // already set?
  if ((fsx.fsx_xflags & XFS_XFLAG_EXTSIZE) && fsx.fsx_extsize == val)
    return 0;

  // xfs won't change extent size if any extents are allocated
  if (fsx.fsx_nextents != 0)
    return 0;

  fsx.fsx_xflags |= XFS_XFLAG_EXTSIZE;
  fsx.fsx_extsize = val;

  if (ioctl(fd, XFS_IOC_FSSETXATTR, &fsx) < 0) {
    ret = -errno;
    return ret;
  }

  return 0;
}
