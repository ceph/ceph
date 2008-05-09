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

#ifndef __MDS_ESLAVEUPDATE_H
#define __MDS_ESLAVEUPDATE_H

#include "../LogEvent.h"
#include "EMetaBlob.h"

class ESlaveUpdate : public LogEvent {
public:
  const static int OP_PREPARE = 1;
  const static int OP_COMMIT = 2;
  const static int OP_ROLLBACK = 3;
  
  /*
   * we journal a rollback metablob that contains the unmodified metadata
   * too, because we may be updating previously dirty metadata, which 
   * will allow old log segments to be trimmed.  if we end of rolling back,
   * those updates could be lost.. so we re-journal the unmodified metadata,
   * and replay will apply _either_ commit or rollback.
   */
  EMetaBlob commit, rollback;
  string type;
  metareqid_t reqid;
  __s32 master;
  __u32 op;  // prepare, commit, abort

  ESlaveUpdate() : LogEvent(EVENT_SLAVEUPDATE) { }
  ESlaveUpdate(MDLog *mdlog, const char *s, metareqid_t ri, int mastermds, int o) : 
    LogEvent(EVENT_SLAVEUPDATE), commit(mdlog), rollback(mdlog),
    type(s),
    reqid(ri),
    master(mastermds),
    op(o) { }
  
  void print(ostream& out) {
    if (type.length())
      out << type << " ";
    out << " " << op;
    out << " " << reqid;
    out << " for mds" << master;
    out << commit << " " << rollback;
  }

  void encode(bufferlist &bl) const {
    ::encode(type, bl);
    ::encode(reqid, bl);
    ::encode(master, bl);
    ::encode(op, bl);
    ::encode(commit, bl);
    ::encode(rollback, bl);
  } 
  void decode(bufferlist::iterator &bl) {
    ::decode(type, bl);
    ::decode(reqid, bl);
    ::decode(master, bl);
    ::decode(op, bl);
    ::decode(commit, bl);
    ::decode(rollback, bl);
  }

  void replay(MDS *mds);
};

#endif
