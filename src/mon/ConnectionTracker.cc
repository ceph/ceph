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

#include "ConnectionTracker.h"

void ConnectionTracker::receive_peer_report(const ConnectionReport& report)
{
  extra_tracker.receive_peer_report(report);
  ConnectionReport& existing = peer_reports[report.rank];
  if (report.epoch > existing.epoch ||
      (report.epoch == existing.epoch && report.epoch_version > existing.epoch_version)) {
    existing = report;
  }
}

bool ConnectionTracker::increase_epoch(epoch_t e)
{
  extra_tracker.increase_epoch(e);
  if (e > epoch) {
    version = 0;
    epoch = e;
    return true;
  }
  return false;
}

void ConnectionTracker::increase_version()
{
  ++version;
  if ((version % 10) == 0 ) { // TODO: make this configurable?
    owner->persist_connectivity_scores();
  }
}

void ConnectionTracker::generate_report_of_peers(ConnectionReport *report) const
{
  assert(report != NULL);
  report->rank = get_my_rank();
  report->epoch = epoch;
  report->epoch_version = version;
  for (const auto i : conn_tracker.peers) {
    get_connection_score(i.first, &report->history[i.first], &report->current[i.first]);
  }

  ConnectionReport dup;
  extra_tracker.generate_report_of_peers(&dup);
  assert(dup == *report);
}

const ConnectionReport *ConnectionTracker::get_peer_view(int peer) const
{
  const ConnectionReport *dup = extra_tracker.get_peer_view(peer);
  if (peer == get_my_rank()) {
    ceph_assert(0);
  }
  const auto& i = peer_reports.find(peer);
  if (i != peer_reports.end()) {
    assert(*dup == i->second);
    return &(i->second);
  }
  assert(dup == NULL);
  return NULL;
}

void ConnectionTracker::forget_peer(int peer)
{
  conn_tracker.peers.erase(peer);
}

void ConnectionTracker::report_live_connection(int rank, double units_alive)
{
  extra_tracker.report_live_connection(rank, units_alive);
  PeerReportTracker::PeerConnection& conn = conn_tracker.peers[rank];
  conn.score = conn.score * ( 1 - units_alive / (2*half_life)) +
    ( units_alive / (2*half_life) );
  conn.score = std::min(conn.score, 1.0);
  conn.alive = true;
  increase_version();
}

void ConnectionTracker::report_dead_connection(int rank, double units_dead)
{
  extra_tracker.report_dead_connection(rank, units_dead);
  PeerReportTracker::PeerConnection& conn = conn_tracker.peers[rank];
  conn.score = conn.score * ( 1 - units_dead / (2*half_life)) -
    ( units_dead / (2*half_life) );
  conn.score = std::max(conn.score, 0.0);
  conn.alive = false;
  increase_version();
}

void ConnectionTracker::get_connection_score(int rank, double *rating, bool *alive) const
{
  *rating = 0;
  *alive = false;
  const auto& i = conn_tracker.peers.find(rank);
  if (i == conn_tracker.peers.end()) {
    return;
  }
  const PeerReportTracker::PeerConnection& conn = i->second;
  *rating = conn.score;
  *alive = conn.alive;

  double dup_rating;
  bool dup_alive;
  extra_tracker.get_connection_score(rank, &dup_rating, &dup_alive);
  assert(dup_rating == *rating && dup_alive == *alive);
}

void ConnectionTracker::get_total_connection_score(int rank, double *rating, int *live_count) const
{
  *rating = 0;
  *live_count = 0;
  double rate = 0;
  int live = 0;

  bool alive;
  get_connection_score(rank, &rate, &alive); // check my scores for the rank
  if (!alive) {
    rate = 0;
  } else {
    ++live;
  }
  for (const auto i : peer_reports) { // and then add everyone else's scores on
    if (i.first == get_my_rank() ||
	i.first == rank) {
      continue;
    }
    const auto& report = i.second;
    auto score_i = report.history.find(rank);
    auto live_i = report.current.find(rank);
    if (score_i != report.history.end()) {
      if (live_i->second) {
	rate += score_i->second;
	++live;
      }
    }
  }
  *rating = rate;
  *live_count = live;


  double dup_rating;
  int dup_live;
  extra_tracker.get_total_connection_score(rank, &dup_rating, &dup_live);
  assert(dup_rating == rate && dup_live == live);
}

void ConnectionTracker::encode(bufferlist &bl) const
{
  map<int,ConnectionReport> peer_copy = peer_reports;
  generate_report_of_peers(&peer_copy[get_my_rank()]);

  ENCODE_START(1, 1, bl);
  encode(epoch, bl);
  encode(version, bl);
  encode(half_life, bl);
  encode(peer_copy, bl);
  encode(extra_tracker, bl);
  ENCODE_FINISH(bl);
}

void ConnectionTracker::decode(bufferlist::const_iterator& bl) {
  peer_reports.clear();
  DECODE_START(1, bl);
  decode(epoch, bl);
  decode(version, bl);
  decode(half_life, bl);
  decode(peer_reports, bl);
  decode(extra_tracker, bl);
  DECODE_FINISH(bl);

  ConnectionReport& my_report = peer_reports[get_my_rank()];
  // TODO should we validate epoch/version matches here?
  for (auto &i : my_report.history) {
    auto& peer_con = conn_tracker.peers[i.first];
    peer_con.alive = my_report.current[i.first];
    peer_con.score = i.second;
  }
}
