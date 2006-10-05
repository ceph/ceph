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


#ifndef __MDSMAP_H
#define __MDSMAP_H

#include "common/Clock.h"
#include "msg/Message.h"

#include "include/types.h"

#include <set>
#include <map>
#include <string>
using namespace std;

class MDSMap {
 protected:
  epoch_t epoch;
  utime_t ctime;

  set<int> all_mds;
  set<int> down_mds;
  map<int,entity_inst_t> mds_inst;

  friend class MDSMonitor;

 public:
  MDSMap() : epoch(0) {}

  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  const utime_t& get_ctime() const { return ctime; }

  int get_num_mds() const { return all_mds.size(); }
  int get_num_up_mds() const { return all_mds.size() - down_mds.size(); }

  const set<int>& get_mds() const { return all_mds; }
  const set<int>& get_down_mds() const { return down_mds; }

  bool is_down(int m) const { return down_mds.count(m); }
  bool is_up(int m) const { return !is_down(m); }

  const entity_inst_t& get_inst(int m) {
    assert(mds_inst.count(m));
    return mds_inst[m];
  }
  bool get_inst(int m, entity_inst_t& inst) { 
    if (mds_inst.count(m)) {
      inst = mds_inst[m];
      return true;
    } 
    return false;
  }

  // serialize, unserialize
  void encode(bufferlist& blist) {
    blist.append((char*)&epoch, sizeof(epoch));
    blist.append((char*)&ctime, sizeof(ctime));
    
    _encode(all_mds, blist);
    _encode(down_mds, blist);
    _encode(mds_inst, blist);
  }
  
  void decode(bufferlist& blist) {
    int off = 0;
    blist.copy(off, sizeof(epoch), (char*)&epoch);
    off += sizeof(epoch);
    blist.copy(off, sizeof(ctime), (char*)&ctime);
    off += sizeof(ctime);
    
    _decode(all_mds, blist, off);
    _decode(down_mds, blist, off);
    _decode(mds_inst, blist, off);
  }


  /*** mapping functions ***/

  int hash_dentry( inodeno_t dirino, const string& dn );  
};

#endif
