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

#ifndef __MDS_ECOMMITTED_H
#define __MDS_ECOMMITTED_H

#include "../LogEvent.h"
#include "EMetaBlob.h"

class ECommitted : public LogEvent {
public:
  metareqid_t reqid;

  ECommitted() : LogEvent(EVENT_COMMITTED) { }
  ECommitted(metareqid_t r) : 
    LogEvent(EVENT_COMMITTED), reqid(r) { }

  void print(ostream& out) {
    out << "ECommitted " << reqid;
  }

  void encode(bufferlist &bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(reqid, bl);
  } 
  void decode(bufferlist::iterator &bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
    ::decode(reqid, bl);
  }

  void update_segment() {}
  void replay(MDS *mds);
};

#endif
