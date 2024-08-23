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

#ifndef CEPH_MDS_ELID_H
#define CEPH_MDS_ELID_H

#include <string_view>

#include "../LogEvent.h"
#include "../SegmentBoundary.h"

class ELid : public LogEvent, public SegmentBoundary {
public:
  ELid() : LogEvent(EVENT_LID) {}
  ELid(LogSegment::seq_t _seq) : LogEvent(EVENT_SEGMENT), SegmentBoundary(_seq) {}

  bool is_major_segment_boundary() const override {
    return true;
  }

  void print(std::ostream& out) const override {
    out << "ELid(" << seq << ")";
  }

  void encode(bufferlist& bl, uint64_t features) const override;
  void decode(bufferlist::const_iterator& bl) override;
  void dump(Formatter *f) const override;
  void replay(MDSRank *mds) override;
  static void generate_test_instances(std::list<ELid*>& ls);
};
WRITE_CLASS_ENCODER_FEATURES(ELid)

#endif
