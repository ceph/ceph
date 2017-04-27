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

#ifndef CEPH_OS_FS_H
#define CEPH_OS_FS_H

#include <errno.h>
#include <time.h>

#include <string>

#include "include/types.h"
#include "common/Mutex.h"
#include "common/Cond.h"

class FS {
public:
  virtual ~FS() { }

  static FS *create(uint64_t f_type);
  static FS *create_by_fd(int fd);

  virtual const char *get_name() {
    return "generic";
  }

  virtual int set_alloc_hint(int fd, uint64_t hint);

  virtual int get_handle(int fd, std::string *h);
  virtual int open_handle(int mount_fd, const std::string& h, int flags);

  virtual int copy_file_range(int to_fd, uint64_t to_offset,
			      int from_fd,
			      uint64_t from_offset, uint64_t from_len);
  virtual int zero(int fd, uint64_t offset, uint64_t length);

  // -- aio --
};

#endif
