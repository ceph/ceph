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

// generic log event
class EResetJournal : public LogEvent {
 public:
  EResetJournal() : LogEvent(EVENT_RESETJOURNAL) { }
  ~EResetJournal() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    ::encode(stamp, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    ::decode(stamp, bl);
    DECODE_FINISH(bl);
  }

  void print(ostream& out) {
    out << "EResetJournal";
  }

  void replay(MDS *mds);
};

#endif
