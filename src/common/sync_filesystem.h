// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_SYNC_FILESYSTEM_H
#define CEPH_SYNC_FILESYSTEM_H

#include <unistd.h>

#if defined(__linux__)
#include <sys/ioctl.h>
#include <syscall.h>
#include "../os/btrfs_ioctl.h"
#endif

inline int sync_filesystem(int fd)
{
  /* On Linux, newer versions of glibc have a function called syncfs that
   * performs a sync on only one filesystem. If we don't have this call, we
   * have to fall back on sync(), which synchronizes every filesystem on the
   * computer. */
#ifdef HAVE_SYS_SYNCFS
  if (syncfs(fd) == 0)
    return 0;
#elif defined(SYS_syncfs)
  if (syscall(SYS_syncfs, fd) == 0)
    return 0;
#elif defined(__NR_syncfs)
  if (syscall(__NR_syncfs, fd) == 0)
    return 0;
#endif

#if defined(HAVE_SYS_SYNCFS) || defined(SYS_syncfs) || defined(__NR_syncfs)
  else if (errno == ENOSYS) {
    sync();
    return 0;
  } else {
    return -errno;
  }
#else
  sync();
  return 0;
#endif
}

#endif
