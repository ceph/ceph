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

#ifndef CEPH_SAFE_IO
#define CEPH_SAFE_IO

#include "common/compiler_extensions.h"
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

  /*
   * Safe functions wrapping the raw read() and write() libc functions.
   * These retry on EINTR, and on error return -errno instead of returning
   * -1 and setting errno).
   */
  ssize_t safe_read(int fd, void *buf, size_t count)
      WARN_UNUSED_RESULT;
  ssize_t safe_write(int fd, const void *buf, size_t count)
      WARN_UNUSED_RESULT;
  ssize_t safe_pread(int fd, void *buf, size_t count, off_t offset)
      WARN_UNUSED_RESULT;
  ssize_t safe_pwrite(int fd, const void *buf, size_t count, off_t offset)
      WARN_UNUSED_RESULT;

  /*
   * Same as the above functions, but return -EDOM unless exactly the requested
   * number of bytes can be read.
   */
  ssize_t safe_read_exact(int fd, void *buf, size_t count)
      WARN_UNUSED_RESULT;
  ssize_t safe_pread_exact(int fd, void *buf, size_t count, off_t offset)
      WARN_UNUSED_RESULT;

#ifdef __cplusplus
}
#endif

#endif
