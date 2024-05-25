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

#ifndef CEPH_MDS_ESEGMENT_H
#define CEPH_MDS_ESEGMENT_H

#include <string_view>

#include "../LogEvent.h"
#include "../SegmentBoundary.h"

class ESegment : public LogEvent, public SegmentBoundary {
public:
  ESegment() : LogEvent(EVENT_SEGMENT) {}
  ESegment(LogSegment::seq_t _seq) : LogEvent(EVENT_SEGMENT), SegmentBoundary(_seq) {}

  void print(std::ostream& out) const override {
    out << "ESegment(" << seq << ")";
  }

  void encode(bufferlist& bl, uint64_t features) const override;
  void decode(bufferlist::const_iterator& bl) override;
  void dump(Formatter *f) const override;
  void replay(MDSRank *mds) override;
  static void generate_test_instances(std::list<ESegment*>& ls);
};
WRITE_CLASS_ENCODER_FEATURES(ESegment)

#endif
