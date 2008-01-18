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

#ifndef __MONMAP_H
#define __MONMAP_H

#include "msg/Message.h"
#include "include/types.h"

class MonMap {
 public:
  epoch_t epoch;       // what epoch/version of the monmap
  ceph_fsid fsid;
  vector<entity_inst_t> mon_inst;

  int       last_mon;    // last mon i talked to

  MonMap(int s=0) : epoch(s?1:0), mon_inst(s), last_mon(-1) {
    generate_fsid();
  }

  unsigned size() {
    return mon_inst.size();
  }

  void add_mon(entity_inst_t inst) {
    if (!epoch) epoch = 1;
    mon_inst.push_back(inst);
  }

  // pick a mon.  
  // choice should be stable, unless we explicitly ask for a new one.
  int pick_mon(bool newmon=false) { 
    if (newmon || (last_mon < 0)) {
      last_mon = rand() % mon_inst.size();
    }
    return last_mon;    
  }

  const entity_inst_t &get_inst(unsigned m) {
    assert(m < mon_inst.size());
    return mon_inst[m];
  }

  void encode(bufferlist& blist) {
    ::_encode(epoch, blist);
    ::_encode(fsid, blist);
    ::_encode(mon_inst, blist);
  }
  
  void decode(bufferlist& blist) {
    int off = 0;
    ::_decode(epoch, blist, off);
    ::_decode(fsid, blist, off);
    ::_decode(mon_inst, blist, off);
  }


  void generate_fsid() {
    fsid.major = ((uint64_t)rand() << 32) + rand();
    fsid.minor = ((uint64_t)rand() << 32) + rand();
  }

  // read from/write to a file
  int write(const char *fn);
  int read(const char *fn);

};

#endif
