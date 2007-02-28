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



#ifndef __MDBALANCER_H
#define __MDBALANCER_H

#include <ostream>
#include <list>
using namespace std;

#include <map>
#include <ext/hash_map>
using namespace __gnu_cxx;

#include "include/types.h"
#include "common/Clock.h"
#include "CInode.h"


class MDS;
class Message;
class MHeartbeat;
class CInode;
class Context;
class CDir;

class MDBalancer {
 protected:
  MDS *mds;
  int beat_epoch;

  utime_t last_heartbeat;
  utime_t last_hash;

  // todo
  set<inodeno_t>   hash_queue;

  // per-epoch scatter/gathered info
  hash_map<int, mds_load_t>  mds_load;
  hash_map<int, float>       mds_meta_load;
  map<int, map<int, float> > mds_import_map;

  // per-epoch state
  double          my_load, target_load;
  map<int,double> my_targets;
  map<int,double> imported;
  map<int,double> exported;

  double try_match(int ex, double& maxex,
                   int im, double& maxim);
  double get_maxim(int im) {
    return target_load - mds_meta_load[im] - imported[im];
  }
  double get_maxex(int ex) {
    return mds_meta_load[ex] - target_load - exported[ex];    
  }

 public:
  MDBalancer(MDS *m) : 
    mds(m),
    beat_epoch(0) { }
  
  mds_load_t get_load();

  int proc_message(Message *m);
  
  void send_heartbeat();
  void handle_heartbeat(MHeartbeat *m);

  void tick();

  void do_hashing();

  void export_empties();
  void do_rebalance(int beat);
  void find_exports(CDir *dir, 
                    double amount, 
                    list<CDir*>& exports, 
                    double& have,
                    set<CDir*>& already_exporting);


  void subtract_export(class CDir *ex);
  void add_import(class CDir *im);

  void hit_inode(class CInode *in, int type=0);
  void hit_dir(class CDir *dir, int type=0);
  void hit_recursive(class CDir *dir, int type=0);


  void show_imports(bool external=false);

};



#endif
