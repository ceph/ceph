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

#ifndef CEPH_MON_CONNECTIONTRACKER_H
#define CEPH_MON_CONNECTIONTRACKER_H

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

class ConnectionTracker {
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
   * Remove the scores for a given peer. Use this
   * when it gets removed from the cluster.
   */
  // TODO: This isn't used anywhere yet and needs to be
  void forget_peer(int peer);
  
  /**
   * Report a connection to a peer rank has been considered alive for
   * the given time duration. We assume the units_alive is <= the time
   * since the previous reporting call.
   * (Or, more precisely, we assume that the total amount of time
   * passed in is less than or equal to the time which has actually
   * passed -- you can report a 10-second death immediately followed
   * by reporting 5 seconds of liveness if your metrics are delayed.)
   */
  void report_live_connection(int rank, double units_alive);
  /**
   * Report a connection to a peer rank has been considered dead for
   * the given time duration, analogous to that above.
   */
  void report_dead_connection(int rank, double units_dead);
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
  void get_connection_score(int rank, double *rating, bool *alive) const;
  /**
   * Get the total connection score of a rank across
   * all peers, and the count of how many electors think it's alive.
   * For this summation, if a rank reports a peer as down its score is zero.
   */
  void get_total_connection_score(int rank, double *rating, int *live_count) const;

  void notify_reset() {
    conn_tracker = PeerReportTracker();
    peer_reports.clear();
  }
  void notify_rank_changed(int new_rank) {
    conn_tracker.peers.erase(new_rank);
    peer_reports.erase(new_rank);
  }
  
  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator& bl);

 private:
  struct PeerReportTracker {
    struct PeerConnection {
      double score;
      bool alive;
      PeerConnection() : score(1), alive(false) {}
    };
    map<int, PeerConnection> peers;
  };

  epoch_t epoch;
  uint64_t version;
  PeerReportTracker conn_tracker; // track my view of peer reliability
  map<int,ConnectionReport> peer_reports; // keep latest reports of peers
  double half_life;
  RankProvider *owner;
  int get_my_rank() const { return owner->get_my_rank(); }

 public:
  ConnectionTracker(RankProvider *o, double hl) : epoch(0), version(0),
						  half_life(hl), owner(o) {}
};

WRITE_CLASS_ENCODER(ConnectionTracker);

#endif
