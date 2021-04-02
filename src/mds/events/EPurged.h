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

#ifndef CEPH_MDS_EPURGE_H
#define CEPH_MDS_EPURGE_H

#include "common/config.h"
#include "include/types.h"

#include "../LogEvent.h"

class EPurged : public LogEvent {
public:
  EPurged() : LogEvent(EVENT_PURGED) { }
  EPurged(const interval_set<inodeno_t>& _inos, LogSegment::seq_t _seq, version_t iv)
    : LogEvent(EVENT_PURGED), inos(_inos), seq(_seq), inotablev(iv) {
  }
  void encode(bufferlist& bl, uint64_t features) const override;
  void decode(bufferlist::const_iterator& bl) override;
  void dump(Formatter *f) const override;
  void print(ostream& out) const override {
    out << "Eurged " << inos.size() << " inos, inotable v" << inotablev;
  }

  void update_segment() override;
  void replay(MDSRank *mds) override;

protected:
  interval_set<inodeno_t> inos;
  LogSegment::seq_t seq;
  version_t inotablev{0};
};
WRITE_CLASS_ENCODER_FEATURES(EPurged)

#endif // CEPH_MDS_EPURGE_H
