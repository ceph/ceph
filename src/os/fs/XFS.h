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

#ifndef CEPH_OS_XFS_H
#define CEPH_OS_XFS_H

#include "FS.h"

# ifndef XFS_SUPER_MAGIC
#define XFS_SUPER_MAGIC 0x58465342
# endif

class XFS : public FS {
  const char *get_name() {
    return "xfs";
  }
  int set_alloc_hint(int fd, uint64_t hint);
};

#endif
