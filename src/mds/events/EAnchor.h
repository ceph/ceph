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

#ifndef __MDS_EANCHOR_H
#define __MDS_EANCHOR_H

#include <assert.h>
#include "config.h"
#include "include/types.h"

#include "../LogEvent.h"
#include "../Anchor.h"

class EAnchor : public LogEvent {
protected:
  __u32 op;
  inodeno_t ino;
  version_t atid; 
  vector<Anchor> trace;
  version_t version;    // anchor table version
  __s32 reqmds;

 public:
  EAnchor() : LogEvent(EVENT_ANCHOR) { }
  EAnchor(int o, inodeno_t i, version_t v, int rm) :
    LogEvent(EVENT_ANCHOR),
    op(o), ino(i), atid(0), version(v), reqmds(rm) { }
  EAnchor(int o, version_t a, version_t v) :
    LogEvent(EVENT_ANCHOR),
    op(o), atid(a), version(v), reqmds(-1) { }

  void set_trace(vector<Anchor>& t) { trace = t; }
  vector<Anchor>& get_trace() { return trace; }
  
  void encode(bufferlist& bl) const {
    ::encode(op, bl);
    ::encode(ino, bl);
    ::encode(atid, bl);
    ::encode(trace, bl);
    ::encode(version, bl);
    ::encode(reqmds, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(op, bl);
    ::decode(ino, bl);
    ::decode(atid, bl);
    ::decode(trace, bl);
    ::decode(version, bl);
    ::decode(reqmds, bl);
  }

  void print(ostream& out) {
    out << "EAnchor " << get_anchor_opname(op);
    if (ino) out << " " << ino;
    if (atid) out << " atid " << atid;
    if (version) out << " v " << version;
    if (reqmds >= 0) out << " by mds" << reqmds;
  }  

  void update_segment();
  void replay(MDS *mds);  
};

#endif
