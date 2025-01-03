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

#include "include/types.h"
#include "common/Clock.h"
#include "common/Cond.h"

#include "msg/Message.h"
#include "messages/MHeartbeat.h"

#include "MDSMap.h"

class MDSRank;
class MHeartbeat;
class CInode;
class CDir;
class Messenger;
class MonClient;

class MDBalancer {
public:
  using clock = ceph::coarse_mono_clock;
  using time = ceph::coarse_mono_time;
  friend class C_Bal_SendHeartbeat;

  MDBalancer(MDSRank *m, Messenger *msgr, MonClient *monc);

  void handle_conf_change(const std::set<std::string>& changed, const MDSMap& mds_map);

  int proc_message(const cref_t<Message> &m);

  /**
   * Regularly called upkeep function.
   *
   * Sends MHeartbeat messages to the mons.
   */
  void tick();

  void handle_export_pins(void);

  void subtract_export(CDir *ex);
  void add_import(CDir *im);
  void adjust_pop_for_rename(CDir *pdir, CDir *dir, bool inc);

  void hit_inode(CInode *in, int type);
  void hit_dir(CDir *dir, int type, double amount=1.0);

  void queue_split(const CDir *dir, bool fast);
  void queue_merge(CDir *dir);
  bool is_fragment_pending(dirfrag_t df) {
    return split_pending.count(df) || merge_pending.count(df);
  }

  /**
   * Based on size and configuration, decide whether to issue a queue_split
   * or queue_merge for this CDir.
   *
   * \param hot whether the directory's temperature is enough to split it
   */
  void maybe_fragment(CDir *dir, bool hot);

  void handle_mds_failure(mds_rank_t who);

  int dump_loads(Formatter *f, int64_t depth = -1) const;

  bool get_bal_export_pin() const {
    return bal_export_pin;
  }
  int64_t get_bal_merge_size() const {
    return bal_merge_size;
  }
  int64_t get_bal_split_size() const {
    return bal_split_size;
  }
  double get_bal_fragment_fast_factor() const {
    return bal_fragment_fast_factor;
  }

private:
  typedef struct {
    std::map<mds_rank_t, double> targets;
    std::map<mds_rank_t, double> imported;
    std::map<mds_rank_t, double> exported;
  } balance_state_t;

  static const unsigned int AUTH_TREES_THRESHOLD = 5;

  //set up the rebalancing targets for export and do one if the
  //MDSMap is up to date
  void prep_rebalance(int beat);
  int mantle_prep_rebalance();

  mds_load_t get_load();
  int localize_balancer();
  void send_heartbeat();
  void handle_heartbeat(const cref_t<MHeartbeat> &m);
  void find_exports(CDir *dir,
                    double amount,
                    std::vector<CDir*>* exports,
                    double& have,
                    std::set<CDir*>& already_exporting);

  double try_match(balance_state_t &state,
                   mds_rank_t ex, double& maxex,
                   mds_rank_t im, double& maxim);

  double get_maxim(balance_state_t &state, mds_rank_t im, double im_target_load) {
    return im_target_load - mds_meta_load[im] - state.imported[im];
  }
  double get_maxex(balance_state_t &state, mds_rank_t ex, double ex_target_load) {
    return mds_meta_load[ex] - ex_target_load - state.exported[ex];
  }

  /**
   * Try to rebalance.
   *
   * Check if the monitor has recorded the current export targets;
   * if it has then do the actual export. Otherwise send off our
   * export targets message again.
   */
  void try_rebalance(balance_state_t& state);
  bool test_rank_mask(mds_rank_t rank);

  bool bal_fragment_dirs;
  int64_t bal_fragment_interval;
  int64_t bal_interval;
  int64_t bal_max_until;
  int64_t bal_mode;
  bool bal_export_pin;
  double bal_sample_interval;
  double bal_split_rd;
  double bal_split_wr;
  double bal_replicate_threshold;
  double bal_unreplicate_threshold;
  double bal_fragment_fast_factor;
  int64_t bal_split_bits;
  int64_t bal_split_size;
  int64_t bal_merge_size;
  int64_t num_bal_times;

  MDSRank *mds;
  Messenger *messenger;
  MonClient *mon_client;
  int beat_epoch = 0;

  std::string bal_code;
  std::string bal_version;

  time last_heartbeat = clock::zero();
  time last_sample = clock::zero();
  time rebalance_time = clock::zero(); //ensure a consistent view of load for rebalance

  time last_get_load = clock::zero();
  uint64_t last_num_requests = 0;
  uint64_t last_cpu_time = 0;
  uint64_t last_num_traverse = 0;
  uint64_t last_num_traverse_hit = 0;

  // Dirfrags which are marked to be passed on to MDCache::[split|merge]_dir
  // just as soon as a delayed context comes back and triggers it.
  // These sets just prevent us from spawning extra timer contexts for
  // dirfrags that already have one in flight.
  std::set<dirfrag_t> split_pending, merge_pending;

  // per-epoch scatter/gathered info
  std::map<mds_rank_t, mds_load_t> mds_load;
  std::map<mds_rank_t, double> mds_meta_load;
  std::map<mds_rank_t, std::map<mds_rank_t, float> > mds_import_map;
  std::map<mds_rank_t, int> mds_last_epoch_under_map;

  // per-epoch state
  double my_load = 0;
  double target_load = 0;
};
#endif
