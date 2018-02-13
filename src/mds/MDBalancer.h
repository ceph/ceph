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
#include "common/Cond.h"

class MDSRank;
class Message;
class MHeartbeat;
class CInode;
class CDir;
class Messenger;
class MonClient;

class MDBalancer {
  friend class C_Bal_SendHeartbeat;
public:
  MDBalancer(MDSRank *m, Messenger *msgr, MonClient *monc) : 
    mds(m),
    messenger(msgr),
    mon_client(monc),
    beat_epoch(0),
    last_epoch_under(0), my_load(0.0), target_load(0.0)
    { }

  mds_load_t get_load(utime_t);

  int proc_message(Message *m);

  /**
   * Regularly called upkeep function.
   *
   * Sends MHeartbeat messages to the mons.
   */
  void tick();

  void subtract_export(CDir *ex, utime_t now);
  void add_import(CDir *im, utime_t now);

  void hit_inode(utime_t now, CInode *in, int type, int who=-1);
  void hit_dir(utime_t now, CDir *dir, int type, int who=-1, double amount=1.0);

  void queue_split(const CDir *dir, bool fast);
  void queue_merge(CDir *dir);

  /**
   * Based on size and configuration, decide whether to issue a queue_split
   * or queue_merge for this CDir.
   *
   * \param hot whether the directory's temperature is enough to split it
   */
  void maybe_fragment(CDir *dir, bool hot);

  void handle_mds_failure(mds_rank_t who);

  int dump_loads(Formatter *f);

private:
  typedef struct {
    std::map<mds_rank_t, double> targets;
    std::map<mds_rank_t, double> imported;
    std::map<mds_rank_t, double> exported;
  } balance_state_t;

  //set up the rebalancing targets for export and do one if the
  //MDSMap is up to date
  void prep_rebalance(int beat);
  int mantle_prep_rebalance();

  void handle_export_pins(void);

  void export_empties();
  int localize_balancer();
  void send_heartbeat();
  void handle_heartbeat(MHeartbeat *m);
  void find_exports(CDir *dir,
                    double amount,
                    list<CDir*>& exports,
                    double& have,
                    set<CDir*>& already_exporting);

  double try_match(balance_state_t &state,
                   mds_rank_t ex, double& maxex,
                   mds_rank_t im, double& maxim);

  double get_maxim(balance_state_t &state, mds_rank_t im) {
    return target_load - mds_meta_load[im] - state.imported[im];
  }
  double get_maxex(balance_state_t &state, mds_rank_t ex) {
    return mds_meta_load[ex] - target_load - state.exported[ex];
  }

  /**
   * Try to rebalance.
   *
   * Check if the monitor has recorded the current export targets;
   * if it has then do the actual export. Otherwise send off our
   * export targets message again.
   */
  void try_rebalance(balance_state_t& state);

  MDSRank *mds;
  Messenger *messenger;
  MonClient *mon_client;
  int beat_epoch;

  int last_epoch_under;
  string bal_code;
  string bal_version;

  mono_time last_heartbeat = mono_clock::zero();
  mono_time last_sample = mono_clock::zero();
  utime_t rebalance_time; //ensure a consistent view of load for rebalance

  // Dirfrags which are marked to be passed on to MDCache::[split|merge]_dir
  // just as soon as a delayed context comes back and triggers it.
  // These sets just prevent us from spawning extra timer contexts for
  // dirfrags that already have one in flight.
  set<dirfrag_t>   split_pending, merge_pending;

  // per-epoch scatter/gathered info
  map<mds_rank_t, mds_load_t>  mds_load;
  map<mds_rank_t, double>       mds_meta_load;
  map<mds_rank_t, map<mds_rank_t, float> > mds_import_map;
  map<mds_rank_t, int> mds_last_epoch_under_info;

  // per-epoch state
  double          my_load, target_load;
};

#endif
