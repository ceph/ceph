// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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

struct MDSMetaRequest {
private:
  int op;
  ceph_tid_t tid;
public:
  explicit MDSMetaRequest(int o, ceph_tid_t t) :
    op(o), tid(t) { }
  virtual ~MDSMetaRequest() { }

  int get_op() { return op; }
  ceph_tid_t get_tid() { return tid; }
};

#endif // !CEPH_MDS_META_REQUEST_H
