// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MDS_SEGMENTBOUNDARY_H
#define CEPH_MDS_SEGMENTBOUNDARY_H

#include "LogSegment.h"

class SegmentBoundary {
public:
  using seq_t = LogSegment::seq_t;

  virtual ~SegmentBoundary() {}
  SegmentBoundary() = default;
  SegmentBoundary(seq_t seq) : seq(seq) {}

  virtual bool is_major_segment_boundary() const {
    return false;
  }

  seq_t get_seq() const {
    return seq;
  }
  void set_seq(seq_t _seq) {
    seq = _seq;
  }
protected:
  seq_t seq = 0;
};

#endif
