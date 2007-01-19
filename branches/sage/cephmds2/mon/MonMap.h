// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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
  epoch_t   epoch;       // what epoch of the osd cluster descriptor is this
  int       num_mon;
  vector<entity_inst_t> mon_inst;

  int       last_mon;    // last mon i talked to

  MonMap(int s=0) : epoch(0), num_mon(s), mon_inst(s), last_mon(-1) {}

  // pick a mon.  
  // choice should be stable, unless we explicitly ask for a new one.
  int pick_mon(bool newmon=false) { 
    if (newmon || (last_mon < 0)) {
      last_mon = 0;  //last_mon = rand() % num_mon;
    }
    return last_mon;    
  }

  const entity_inst_t get_inst(int m) {
    assert(m < num_mon);
    return mon_inst[m];
  }

  void encode(bufferlist& blist) {
    blist.append((char*)&epoch, sizeof(epoch));
    blist.append((char*)&num_mon, sizeof(num_mon));
    
    _encode(mon_inst, blist);
  }
  
  void decode(bufferlist& blist) {
    int off = 0;
    blist.copy(off, sizeof(epoch), (char*)&epoch);
    off += sizeof(epoch);
    blist.copy(off, sizeof(num_mon), (char*)&num_mon);
    off += sizeof(num_mon);

    _decode(mon_inst, blist, off);
  }

};

#endif
