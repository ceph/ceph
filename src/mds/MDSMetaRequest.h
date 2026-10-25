// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MDS_META_REQUEST_H
#define CEPH_MDS_META_REQUEST_H

#include "include/types.h"
#include "include/cephfs/types.h"

struct MDSMetaRequest {
private:
  int op;
  ceph_tid_t tid;
  mds_rank_t target;
public:
  explicit MDSMetaRequest(int o, ceph_tid_t t, mds_rank_t to) :
    op(o), tid(t), target(to) { }
  virtual ~MDSMetaRequest() { }

  int get_op() { return op; }
  ceph_tid_t get_tid() { return tid; }
  mds_rank_t get_target() { return target; }
};

#endif // !CEPH_MDS_META_REQUEST_H
