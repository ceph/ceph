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

std::ostream& operator<<(std::ostream&o, const ConnectionReport& c) {
  o << "rank=" << c.rank << ",epoch=" << c.epoch << ",version=" << c.epoch_version
    << ", current links: " << c.current << ", history: " << c.history;
  return o;
}

std::ostream& operator<<(std::ostream& o, const ConnectionTracker& c) {
  o << "rank=" << c.rank << ", epoch=" << c.epoch << ", version=" << c.version
    << ", half_life=" << c.half_life << ", reports: " << c.peer_reports;
  return o;
}

ConnectionReport *ConnectionTracker::reports(int p)
{
  auto i = peer_reports.find(p);
  if (i == peer_reports.end()) {
    ceph_assert(p != rank);
    auto[j,k] = peer_reports.insert(std::pair<int,ConnectionReport*>(p,new ConnectionReport()));
    i = j;
  }
  return i->second;
}

const ConnectionReport *ConnectionTracker::reports(int p) const
{
  auto i = peer_reports.find(p);
  if (i == peer_reports.end()) {
    return NULL;
  }
  return i->second;
}

void ConnectionTracker::receive_peer_report(const ConnectionReport& report)
{
  if (report.rank == rank) return;
  ConnectionReport& existing = *reports(report.rank);
  if (report.epoch > existing.epoch ||
      (report.epoch == existing.epoch &&
       report.epoch_version > existing.epoch_version)) {
    existing = report;
  }
}

bool ConnectionTracker::increase_epoch(epoch_t e)
{
  if (e > epoch) {
    my_reports.epoch_version = version = 0;
    my_reports.epoch = epoch = e;
    return true;
  }
  return false;
}

void ConnectionTracker::increase_version()
{
  ++version;
  my_reports->epoch_version = version;
  if ((version % 10) == 0 ) { // TODO: make this configurable?
    owner->persist_connectivity_scores();
  }
}

void ConnectionTracker::generate_report_of_peers(ConnectionReport *report) const
{
  ceph_assert(report != NULL);
  *report = my_reports;
}

const ConnectionReport *ConnectionTracker::get_peer_view(int peer) const
{
  ceph_assert(peer != rank);
  return reports(peer);
}

void ConnectionTracker::report_live_connection(int peer_rank, double units_alive)
{
  // we need to "auto-initialize" to 1, do shenanigans
  auto i = my_reports.history.find(peer_rank);
  if (i == my_reports.history.end()) {
    auto[j,k] = my_reports.history.insert(std::pair<int,double>(peer_rank,1.0));
    i = j;
  }
  double& pscore = i->second;
  pscore = pscore * (1 - units_alive / (2 * half_life)) +
    (units_alive / (2 * half_life));
  pscore = std::min(pscore, 1.0);
  my_reports.current[peer_rank] = true;

  increase_version();
}

void ConnectionTracker::report_dead_connection(int peer_rank, double units_dead)
{
  // we need to "auto-initialize" to 1, do shenanigans
  auto i = my_reports.history.find(peer_rank);
  if (i == my_reports.history.end()) {
    auto[j,k] = my_reports.history.insert(std::pair<int,double>(peer_rank,1.0));
    i = j;
  }
  double& pscore = i->second;
  pscore = pscore * (1 - units_dead / (2 * half_life)) -
    (units_dead / (2*half_life));
  pscore = std::max(pscore, 0.0);
  my_reports.current[peer_rank] = false;
  
  increase_version();
}

void ConnectionTracker::get_connection_score(int peer_rank, double *rating,
					      bool *alive) const
{
  *rating = 0;
  *alive = false;
  const auto& i = my_reports.history.find(peer_rank);
  if (i == my_reports.history.end()) {
    return;
  }
  *rating = i->second;
  *alive = my_reports.current[peer_rank];
}

void ConnectionTracker::get_total_connection_score(int peer_rank, double *rating,
						    int *live_count) const
{
  *rating = 0;
  *live_count = 0;
  double rate = 0;
  int live = 0;

  for (const auto i : peer_reports) { // loop through all the scores
    if (i.first == peer_rank) { // ... except the ones it has for itself, of course!
      continue;
    }
    const auto& report = i.second;
    auto score_i = report->history.find(peer_rank);
    auto live_i = report->current.find(peer_rank);
    if (score_i != report->history.end()) {
      if (live_i->second) {
	rate += score_i->second;
	++live;
      }
    }
  }
  *rating = rate;
  *live_count = live;
}

void ConnectionTracker::encode(bufferlist &bl) const
{
  map<int,ConnectionReport> reports;
  for (const auto& i : peer_reports) {
    reports[i.first] = *i.second;
  }
  ENCODE_START(1, 1, bl);
  encode(rank, bl);
  encode(epoch, bl);
  encode(version, bl);
  encode(half_life, bl);
  encode(reports, bl);
  ENCODE_FINISH(bl);
}

void ConnectionTracker::decode(bufferlist::const_iterator& bl) {
  clear_peer_reports();

  map<int,ConnectionReport> reports;
  DECODE_START(1, bl);
  decode(rank, bl);
  decode(epoch, bl);
  decode(version, bl);
  decode(half_life, bl);
  decode(reports, bl);
  DECODE_FINISH(bl);

  my_reports = reports[rank];
  for (const auto& i : reports) {
    if (i.first == rank) continue;
    peer_reports[i.first] = new ConnectionReport(i.second);
  }
}
