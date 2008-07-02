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


#ifndef __SNAPTABLE_H
#define __SNAPTABLE_H

#include "MDSTable.h"
#include "include/interval_set.h"

class MDS;

class SnapTable : public MDSTable {
public:
  struct snapinfo {
    inodeno_t base;
    utime_t stamp;
    string name;

    void encode(bufferlist& bl) const {
      ::encode(base, bl);
      ::encode(stamp, bl);
      ::encode(name, bl);
    }
    void decode(bufferlist::iterator& bl) {
      ::decode(base, bl);
      ::decode(stamp, bl);
      ::decode(name, bl);
    }
  };
  WRITE_CLASS_ENCODER(snapinfo)
  
protected:
  snapid_t last_snap;
  map<snapid_t, snapinfo> snaps;
  set<snapid_t> pending_removal;

public:
  SnapTable(MDS *m) : MDSTable(m, "snap") { }
  
  // alloc or reclaim ids
  snapid_t create(inodeno_t base, const string& name, utime_t stamp);
  void remove(snapid_t sn);
  
  void init_inode();
  void reset_state();
  void encode_state(bufferlist& bl) {
    ::encode(last_snap, bl);
    ::encode(snaps, bl);
    ::encode(pending_removal, bl);
  }
  void decode_state(bufferlist::iterator& bl) {
    ::decode(last_snap, bl);
    ::decode(snaps, bl);
    ::decode(pending_removal, bl);
  }
};
WRITE_CLASS_ENCODER(SnapTable::snapinfo)

#endif
