// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef MDS_BATCHOP_H
#define MDS_BATCHOP_H

#include "common/ref.h"

#include "mdstypes.h"

class BatchOp {
public:
  virtual ~BatchOp() {}

  virtual void add_request(const ceph::ref_t<class MDRequestImpl>& mdr) = 0;
  virtual ceph::ref_t<class MDRequestImpl> find_new_head() = 0;

  virtual void print(std::ostream&) = 0;

  void forward(mds_rank_t target);
  void respond(int r);

protected:
  virtual void _forward(mds_rank_t) = 0;
  virtual void _respond(mds_rank_t) = 0;
};

#endif
