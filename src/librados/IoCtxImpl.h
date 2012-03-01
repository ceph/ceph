// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2012 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_LIBRADOS_IOCTXIMPL_H
#define CEPH_LIBRADOS_IOCTXIMPL_H

#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/snap_types.h"
#include "include/atomic.h"
#include "include/rados.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/types.h"
#include "include/xlist.h"
#include "osd/osd_types.h"

class RadosClient;

struct librados::IoCtxImpl {
  atomic_t ref_cnt;
  RadosClient *client;
  int64_t poolid;
  string pool_name;
  snapid_t snap_seq;
  ::SnapContext snapc;
  uint64_t assert_ver;
  map<object_t, uint64_t> assert_src_version;
  eversion_t last_objver;
  uint32_t notify_timeout;
  object_locator_t oloc;

  Mutex aio_write_list_lock;
  tid_t aio_write_seq;
  Cond aio_write_cond;
  xlist<AioCompletionImpl*> aio_write_list;

  IoCtxImpl();
  IoCtxImpl(RadosClient *c, int pid, const char *pool_name_, snapid_t s);

  void dup(const IoCtxImpl& rhs) {
    // Copy everything except the ref count
    client = rhs.client;
    poolid = rhs.poolid;
    pool_name = rhs.pool_name;
    snap_seq = rhs.snap_seq;
    snapc = rhs.snapc;
    assert_ver = rhs.assert_ver;
    assert_src_version = rhs.assert_src_version;
    last_objver = rhs.last_objver;
    notify_timeout = rhs.notify_timeout;
    oloc = rhs.oloc;
  }

  void set_snap_read(snapid_t s);
  int set_snap_write_context(snapid_t seq, vector<snapid_t>& snaps);

  void get() {
    ref_cnt.inc();
  }

  void put() {
    if (ref_cnt.dec() == 0)
      delete this;
  }

  void queue_aio_write(struct AioCompletionImpl *c);
  void complete_aio_write(struct AioCompletionImpl *c);
  void flush_aio_writes();

  int64_t get_id() {
    return poolid;
  }
};

#endif
