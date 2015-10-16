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



#ifndef CEPH_MDBALANCER_H
#define CEPH_MDBALANCER_H

#include <list>
#include <map>
using std::list;
using std::map;

#include "include/types.h"
#include "common/Clock.h"
#include "CInode.h"


class MDSRank;
class Message;
class MHeartbeat;
class CInode;
class CDir;
class Messenger;
class MonClient;

class MDBalancer {
 protected:
  MDSRank *mds;
  Messenger *messenger;
  MonClient *mon_client;
  int beat_epoch;

  int last_epoch_under;  
  int last_epoch_over; 

  utime_t last_heartbeat;
  utime_t last_fragment;
  utime_t last_sample;    
  utime_t rebalance_time; //ensure a consistent view of load for rebalance

  // todo
  set<dirfrag_t>   split_queue, merge_queue;

  // per-epoch scatter/gathered info
  map<mds_rank_t, mds_load_t>  mds_load;
  map<mds_rank_t, float>       mds_meta_load;
  map<mds_rank_t, map<mds_rank_t, float> > mds_import_map;

  // per-epoch state
  double          my_load, target_load;
  map<mds_rank_t,double> my_targets;
  map<mds_rank_t,double> imported;
  map<mds_rank_t,double> exported;

  map<mds_rank_t, int> old_prev_targets;  // # iterations they _haven't_ been targets
  bool check_targets();

  double try_match(mds_rank_t ex, double& maxex,
                   mds_rank_t im, double& maxim);
  double get_maxim(mds_rank_t im) {
    return target_load - mds_meta_load[im] - imported[im];
  }
  double get_maxex(mds_rank_t ex) {
    return mds_meta_load[ex] - target_load - exported[ex];    
  }

public:
  MDBalancer(MDSRank *m, Messenger *msgr, MonClient *monc) : 
    mds(m),
    messenger(msgr),
    mon_client(monc),
    beat_epoch(0),
    last_epoch_under(0), last_epoch_over(0), my_load(0.0), target_load(0.0) { }
  
  mds_load_t get_load(utime_t);

  int proc_message(Message *m);
  
  void send_heartbeat();
  void handle_heartbeat(MHeartbeat *m);

  void tick();

  void do_fragmenting();

  void export_empties();
  //set up the rebalancing targets for export and do one if the
  //MDSMap is up to date
  void prep_rebalance(int beat);
  /*check if the monitor has recorded the current export targets;
    if it has then do the actual export. Otherwise send off our
    export targets message again*/
  void try_rebalance();
  void find_exports(CDir *dir, 
                    double amount, 
                    list<CDir*>& exports, 
                    double& have,
                    set<CDir*>& already_exporting);


  void subtract_export(class CDir *ex, utime_t now);
  void add_import(class CDir *im, utime_t now);

  void hit_inode(utime_t now, class CInode *in, int type, int who=-1);
  void hit_dir(utime_t now, class CDir *dir, int type, int who=-1, double amount=1.0);
  void hit_recursive(utime_t now, class CDir *dir, int type, double amount, double rd_adj);


  void show_imports(bool external=false);

  void queue_split(CDir *dir);
  void queue_merge(CDir *dir);

};



#endif
