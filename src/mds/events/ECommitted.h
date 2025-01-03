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

#ifndef CEPH_MDS_ECOMMITTED_H
#define CEPH_MDS_ECOMMITTED_H

#include "../LogEvent.h"
#include "EMetaBlob.h"

class ECommitted : public LogEvent {
public:
  metareqid_t reqid;

  ECommitted() : LogEvent(EVENT_COMMITTED) { }
  explicit ECommitted(metareqid_t r) :
    LogEvent(EVENT_COMMITTED), reqid(r) { }

  void print(std::ostream& out) const override {
    out << "ECommitted " << reqid;
  }

  void encode(bufferlist &bl, uint64_t features) const override;
  void decode(bufferlist::const_iterator &bl) override;
  void dump(Formatter *f) const override;
  static void generate_test_instances(std::list<ECommitted*>& ls);

  void update_segment() override {}
  void replay(MDSRank *mds) override;
};
WRITE_CLASS_ENCODER_FEATURES(ECommitted)

#endif
