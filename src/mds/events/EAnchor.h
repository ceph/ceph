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
  int op;
  inodeno_t ino;
  version_t atid; 
  vector<Anchor> trace;
  version_t version;    // anchor table version
  int reqmds;

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
  
  void encode_payload(bufferlist& bl) {
    bl.append((char*)&op, sizeof(op));
    bl.append((char*)&ino, sizeof(ino));
    bl.append((char*)&atid, sizeof(atid));
    ::_encode(trace, bl);
    bl.append((char*)&version, sizeof(version));
    bl.append((char*)&reqmds, sizeof(reqmds));
  }
  void decode_payload(bufferlist& bl, int& off) {
    bl.copy(off, sizeof(op), (char*)&op);
    off += sizeof(op);
    bl.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    bl.copy(off, sizeof(atid), (char*)&atid);
    off += sizeof(atid);
    ::_decode(trace, bl, off);
    bl.copy(off, sizeof(version), (char*)&version);
    off += sizeof(version);
    bl.copy(off, sizeof(reqmds), (char*)&reqmds);
    off += sizeof(reqmds);
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
