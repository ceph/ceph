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

#pragma once
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
  friend std::ostream& operator<<(std::ostream&o, const ConnectionReport& c);

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<ConnectionReport*>& o);
};
WRITE_CLASS_ENCODER(ConnectionReport);

struct DirectedGraph {
  // The set of nodes in the graph
  // works with only non-negative ranks
  // because we only run the algorithm when
  // all monitors have valid ranks.
  std::map<unsigned, std::set<unsigned>> outgoing_edges;
  std::map<unsigned, std::set<unsigned>> incoming_edges;
  CephContext *cct;
  DirectedGraph(CephContext *c) : cct(c) {}
  void add_outgoing_edge(unsigned from, unsigned to);
  void add_incoming_edge(unsigned to, unsigned from);
  bool has_outgoing_edge(unsigned from, unsigned to) const;
  bool has_incoming_edge(unsigned to, unsigned from) const;
};

class RankProvider {
 public:
  /**
   * Get the rank of the running daemon.
   * It can be -1, meaning unknown/invalid, or it
   * can be >1.
   * You should not invoke the function get_total_connection_score()
   * with an unknown rank.
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
  void receive_peer_report(const ConnectionTracker& o);
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
   * Get the total connection score of a rank across
   * all peers, and the count of how many electors think it's alive.
   * For this summation, if a rank reports a peer as down its score is zero.
   */
  void get_total_connection_score(int peer_rank, double *rating,
				  int *live_count) const;
  /**
  * Check if our ranks are clean and make
  * sure there are no extra peer_report lingering.
  * In the future we also want to check the reports
  * current and history of each peer_report.
  */
  bool is_clean(int mon_rank, int monmap_size);
  /**
   * Get the set of monitor pairs that are disconnected
   * due to network partitions.
   * This is a set of pairs (rank1, rank2) where rank1 < rank2.
   */
  std::set<std::pair<unsigned, unsigned>> get_netsplit(std::set<unsigned> &mons_down);
  /**
   * Encode this ConnectionTracker. Useful both for storing on disk
   * and for sending off to peers for decoding and import
   * with receive_peer_report() above.
   */
  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator& bl);
  /**
   * Get a bufferlist containing the ConnectionTracker.
   * This is like encode() but holds a copy so it
   * doesn't re-encode on every invocation.
   */
  const bufferlist& get_encoded_bl();
 private:
  epoch_t epoch;
  uint64_t version;
  std::map<int,ConnectionReport> peer_reports;
  ConnectionReport my_reports;
  double half_life;
  RankProvider *owner;
  int rank;
  int persist_interval;
  bufferlist encoding;
  CephContext *cct;
  int get_my_rank() const { return rank; }
  ConnectionReport *reports(int p);
  const ConnectionReport *reports(int p) const;

  void clear_peer_reports() {
    encoding.clear();
    peer_reports.clear();
    my_reports = ConnectionReport();
    my_reports.rank = rank;
  }

 public:
  ConnectionTracker() : epoch(0), version(0), half_life(12*60*60),
			owner(NULL), rank(-1), persist_interval(10) {
  }
  ConnectionTracker(RankProvider *o, int rank, double hl,
		    int persist_i, CephContext *c) :
    epoch(0), version(0),
    half_life(hl), owner(o), rank(rank), persist_interval(persist_i), cct(c) {
    my_reports.rank = rank;
  }
  ConnectionTracker(const bufferlist& bl, CephContext *c) :
    epoch(0), version(0),
    half_life(0), owner(NULL), rank(-1), persist_interval(10), cct(c)
  {
    auto bi = bl.cbegin();
    decode(bi);
  }
  ConnectionTracker(const ConnectionTracker& o) :
    epoch(o.epoch), version(o.version),
    half_life(o.half_life), owner(o.owner), rank(o.rank),
    persist_interval(o.persist_interval), cct(o.cct)
  {
    peer_reports = o.peer_reports;
    my_reports = o.my_reports;
  }
  void notify_reset() { clear_peer_reports(); }
  void set_rank(int new_rank) {
    rank = new_rank;
    my_reports.rank = rank;
  }

  void notify_rank_changed(int new_rank);
  void notify_rank_removed(int rank_removed, int new_rank);
  friend std::ostream& operator<<(std::ostream& o, const ConnectionTracker& c);
  friend ConnectionReport *get_connection_reports(ConnectionTracker& ct);
  friend std::map<int,ConnectionReport> *get_peer_reports(ConnectionTracker& ct);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<ConnectionTracker*>& o);
};

WRITE_CLASS_ENCODER(ConnectionTracker);
