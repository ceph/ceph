// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_OS_BLUESTORE_ALLOCATOR_H
#define CEPH_OS_BLUESTORE_ALLOCATOR_H

#include "kv/KeyValueDB.h"
#include <ostream>
#include "include/assert.h"

class FreelistManager;

class Allocator {
public:
  virtual ~Allocator() {}

  virtual int reserve(uint64_t need) = 0;
  virtual void unreserve(uint64_t unused) = 0;

  virtual int allocate(
    uint64_t need_size, uint64_t alloc_unit, int64_t hint,
    uint64_t *offset, uint32_t *length) = 0;

  virtual int release(
    uint64_t offset, uint64_t length) = 0;

  virtual void commit_start() = 0;
  virtual void commit_finish() = 0;

  virtual void dump(std::ostream& out) = 0;

  virtual void init_add_free(uint64_t offset, uint64_t length) = 0;
  virtual void init_rm_free(uint64_t offset, uint64_t length) = 0;

  virtual uint64_t get_free() = 0;

  virtual void shutdown() = 0;
  static Allocator *create(string type, int64_t size);
};

#endif
