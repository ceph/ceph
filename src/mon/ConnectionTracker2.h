// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MON_CONNECTIONTRACKER2_H
#define CEPH_MON_CONNECTIONTRACKER2_H

#include "include/types.h"

struct ConnectionReport {
  int rank = -1; // mon rank this state belongs to
  std::map<int, bool> current; // true if connected to the other mon
  std::map<int, double> history; // [0-1]; the connection reliability
  epoch_t epoch = 0; // the (local) election epoch the ConnectionReport came from
  uint64_t epoch_version = 0; // version of the ConnectionReport within the epoch
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(rank, bl);
    encode(current, bl);
    encode(history, bl);
    encode(epoch, bl);
    encode(epoch_version, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(rank, bl);
    decode(current, bl);
    decode(history, bl);
    decode(epoch, bl);
    decode(epoch_version, bl);
    DECODE_FINISH(bl);
  }
  bool operator==(const ConnectionReport& o) const {
    return o.rank == rank && o.current == current &&
      o.history == history && o.epoch == epoch &&
      o.epoch_version == epoch_version;
  }
};
WRITE_CLASS_ENCODER(ConnectionReport);

class RankProvider {
 public:
  /**
   * Get the rank of the running daemon.
   * It can be -1, meaning unknown/invalid, or it
   * can be >1.
   * You should not invoke the functions generate_report_of_peers()
   * or get_total_connection_score() with an unknown rank.
   */
  virtual int get_my_rank() const = 0;
  /**
   * Asks our owner to encode us and persist it to disk.
   * Presently we do this every tenth update.
   */
  virtual void persist_connectivity_scores() = 0;
  virtual ~RankProvider() {}
};

class ConnectionTracker2 {
 public:
  /**
   * Receive a report from a peer and update our internal state
   * if the peer has newer data.
   */
  void receive_peer_report(const ConnectionReport& report);
  /**
   * Bump up the epoch to the specified number.
   * Validates that it is > current epoch and resets
   * version to 0; returns false if not.
   */
  bool increase_epoch(epoch_t e);
  /**
   * Bump up the version within our epoch.
   * If the new version is a multiple of ten, we also persist it.
   */
  void increase_version();
  /**
   * Fill in the report with our liveness and reliability
   * scores for all peers.
   *
   * NOTE: This only shares what the *local* node has seen directly;
   * it does not include the shared data from other peers. Use
   * get_peer_view to grab those and bundle them up when sharing it.
   * This interface isn't great and would be better if you got a single
   * struct for sharing, encode/decode, and import back into another
   * ConnectionTracker, so look at existing implementations carefully
   * if you're messing around here.
   */
  void generate_report_of_peers(ConnectionReport *report) const;
  /**
   * Get our current report from a peer of what it sees.
   */
  const ConnectionReport *get_peer_view(int peer) const;
  
  /**
   * Report a connection to a peer rank has been considered alive for
   * the given time duration. We assume the units_alive is <= the time
   * since the previous reporting call.
   * (Or, more precisely, we assume that the total amount of time
   * passed in is less than or equal to the time which has actually
   * passed -- you can report a 10-second death immediately followed
   * by reporting 5 seconds of liveness if your metrics are delayed.)
   */
  void report_live_connection(int peer_rank, double units_alive);
  /**
   * Report a connection to a peer rank has been considered dead for
   * the given time duration, analogous to that above.
   */
  void report_dead_connection(int peer_rank, double units_dead);
  /**
   * Set the half-life for dropping connection state
   * out of the ongoing score.
   * Whenever you add a new data point:
   * new_score = old_score * ( 1 - units / (2d)) + (units/(2d))
   * where units is the units reported alive (for dead, you subtract them).
   */
  void set_half_life(double d) {
    half_life = d;
  }
  /**
   * Get the connection score and whether it has most recently
   * been reported alive for a peer rank.
   */
  void get_connection_score(int peer_rank, double *rating, bool *alive) const;
  /**
   * Get the total connection score of a rank across
   * all peers, and the count of how many electors think it's alive.
   * For this summation, if a rank reports a peer as down its score is zero.
   */
  void get_total_connection_score(int peer_rank, double *rating,
				  int *live_count) const;

  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator& bl);

 private:
  epoch_t epoch;
  uint64_t version;
  map<int,ConnectionReport*> peer_reports;
  mutable ConnectionReport my_reports;
  double half_life;
  RankProvider *owner;
  int rank;
  int get_my_rank() const { return rank; }
  ConnectionReport *reports(int p);
  const ConnectionReport *reports(int p) const;

  void clear_peer_reports() {
    peer_reports.erase(rank);
    for (auto i : peer_reports) {
      delete i.second;
    }
    peer_reports.clear();
  }

 public:
  ConnectionTracker2(RankProvider *o, int rank, double hl) :
    epoch(0), version(0),
    half_life(hl), owner(o), rank(rank) {
    peer_reports[rank] = &my_reports;
    my_reports.rank = rank;
  }
  ConnectionTracker2(const ConnectionTracker2& o) :
    epoch(o.epoch), version(o.version),
    half_life(o.half_life), owner(o.owner), rank(o.rank)
  {
    my_reports = o.my_reports;
    for (const auto& i : o.peer_reports) {
      if (i.first == rank) continue;
      peer_reports[i.first] = new ConnectionReport(*i.second);
    }
    peer_reports[rank] = &my_reports;
  }
  void notify_reset() { clear_peer_reports(); }
  void notify_rank_changed(int new_rank) {
    if (new_rank == rank) return;
    delete peer_reports[new_rank];
    peer_reports.erase(new_rank);
    peer_reports.erase(rank);
    peer_reports[new_rank] = &my_reports;
    my_reports.rank = new_rank;
    rank = new_rank;
  }
  
};

WRITE_CLASS_ENCODER(ConnectionTracker2);

#endif
