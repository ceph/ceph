// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef CEPH_MDS_ERESETJOURNAL_H
#define CEPH_MDS_ERESETJOURNAL_H

#include "../LogEvent.h"
#include "../SegmentBoundary.h"

// generic log event
class EResetJournal : public LogEvent, public SegmentBoundary {
 public:
  EResetJournal() : LogEvent(EVENT_RESETJOURNAL) { }
  ~EResetJournal() override {}

  bool is_major_segment_boundary() const override {
    return true;
  }

  void encode(bufferlist& bl, uint64_t features) const override;
  void decode(bufferlist::const_iterator& bl) override;
  void dump(Formatter *f) const override;
  static void generate_test_instances(std::list<EResetJournal*>& ls);
  void print(std::ostream& out) const override {
    out << "EResetJournal";
  }

  void replay(MDSRank *mds) override;
};
WRITE_CLASS_ENCODER_FEATURES(EResetJournal)

#endif
