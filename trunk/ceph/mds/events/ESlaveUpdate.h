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
  
  EMetaBlob metablob;
  string type;
  metareqid_t reqid;
  int master;
  int op;  // prepare, commit, abort

  ESlaveUpdate() : LogEvent(EVENT_SLAVEUPDATE) { }
  ESlaveUpdate(MDLog *mdlog, const char *s, metareqid_t ri, int mastermds, int o) : 
    LogEvent(EVENT_SLAVEUPDATE), metablob(mdlog),
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
    out << metablob;
  }

  void encode_payload(bufferlist& bl) {
    ::_encode(type, bl);
    ::_encode(reqid, bl);
    ::_encode(master, bl);
    ::_encode(op, bl);
    metablob._encode(bl);
  } 
  void decode_payload(bufferlist& bl, int& off) {
    ::_decode(type, bl, off);
    ::_decode(reqid, bl, off);
    ::_decode(master, bl, off);
    ::_decode(op, bl, off);
    metablob._decode(bl, off);
  }

  bool has_expired(MDS *mds);
  void expire(MDS *mds, Context *c);
  void replay(MDS *mds);
};

#endif
