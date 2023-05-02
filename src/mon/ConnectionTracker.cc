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
#include "common/Formatter.h"
#include "common/dout.h"
#include "include/ceph_assert.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, rank, epoch, version)

static std::ostream& _prefix(std::ostream *_dout, int rank, epoch_t epoch, uint64_t version) {
  return *_dout << "rank: " << rank << " version: "<< version << " ConnectionTracker(" << epoch << ") ";
}

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
    auto[j,k] = peer_reports.insert(std::pair<int,ConnectionReport>(p,ConnectionReport()));
    i = j;
  }
  return &i->second;
}

const ConnectionReport *ConnectionTracker::reports(int p) const
{
  auto i = peer_reports.find(p);
  if (i == peer_reports.end()) {
    return NULL;
  }
  return &i->second;
}

void ConnectionTracker::receive_peer_report(const ConnectionTracker& o)
{
  ldout(cct, 30) << __func__ << dendl;
  for (auto& i : o.peer_reports) {
    const ConnectionReport& report = i.second;
    if (i.first == rank || i.first < 0) {
      continue;
    }
    ConnectionReport& existing = *reports(i.first);
    if (report.epoch > existing.epoch ||
	(report.epoch == existing.epoch &&
	 report.epoch_version > existing.epoch_version)) {
      ldout(cct, 30) << " new peer_report is more updated" << dendl;
      ldout(cct, 30) << "existing: " << existing << dendl;
      ldout(cct, 30) << "new: " << report << dendl;
      existing = report;
    }
  }
  encoding.clear();
}

bool ConnectionTracker::increase_epoch(epoch_t e)
{
  ldout(cct, 30) << __func__ << " to " << e << dendl;
  if (e > epoch && rank >= 0) {
    my_reports.epoch_version = version = 0;
    my_reports.epoch = epoch = e;
    peer_reports[rank] = my_reports;
    encoding.clear();
    return true;
  }
  ldout(cct, 10) << "Either got a report from a rank -1 or our epoch is >= to "
    << e << " not increasing our epoch!" << dendl;
  return false;
}

void ConnectionTracker::increase_version()
{
  ldout(cct, 30) << __func__ << " to " << version+1 << dendl;
  if (rank >= 0) {
    encoding.clear();
    ++version;
    my_reports.epoch_version = version;
    peer_reports[rank] = my_reports;
    if ((version % persist_interval) == 0 ) {
      ldout(cct, 30) << version << " % " << persist_interval << " == 0" << dendl;
      owner->persist_connectivity_scores();
    }
  } else {
      ldout(cct, 10) << "Got a report from a rank -1, not increasing our version!" << dendl;
  }
}

void ConnectionTracker::report_live_connection(int peer_rank, double units_alive)
{
  ldout(cct, 30) << __func__ << " peer_rank: " << peer_rank << " units_alive: " << units_alive << dendl;
  ldout(cct, 30) << "my_reports before: " << my_reports << dendl;
  if (peer_rank == rank) {
    lderr(cct) << "Got a report from my own rank, hopefully this is startup weirdness, dropping" << dendl;
    return;
  }
  if (peer_rank < 0) {
    ldout(cct, 10) << "Got a report from a rank -1, not adding that to our report!" << dendl;
    return;
  }  
  // we need to "auto-initialize" to 1, do shenanigans
  auto i = my_reports.history.find(peer_rank);
  if (i == my_reports.history.end()) {
    ldout(cct, 30) << "couldn't find: " << peer_rank
      << " in my_reports.history" << "... inserting: "
      << "(" << peer_rank << ", 1" << dendl;
    auto[j,k] = my_reports.history.insert(std::pair<int,double>(peer_rank,1.0));
    i = j;
  }
  double& pscore = i->second;
  ldout(cct, 30) << "adding new pscore to my_reports" << dendl;
  pscore = pscore * (1 - units_alive / (2 * half_life)) +
    (units_alive / (2 * half_life));
  pscore = std::min(pscore, 1.0);
  my_reports.current[peer_rank] = true;

  increase_version();
  ldout(cct, 30) << "my_reports after: " << my_reports << dendl;
}

void ConnectionTracker::report_dead_connection(int peer_rank, double units_dead)
{
  ldout(cct, 30) << __func__ << " peer_rank: " << peer_rank << " units_dead: " << units_dead << dendl;
  ldout(cct, 30) << "my_reports before: " << my_reports << dendl;
  if (peer_rank == rank) {
    lderr(cct) << "Got a report from my own rank, hopefully this is startup weirdness, dropping" << dendl;
    return;
  }
  if (peer_rank < 0) {
    ldout(cct, 10) << "Got a report from a rank -1, not adding that to our report!" << dendl;
    return;
  }
  // we need to "auto-initialize" to 1, do shenanigans
  auto i = my_reports.history.find(peer_rank);
  if (i == my_reports.history.end()) {
    ldout(cct, 30) << "couldn't find: " << peer_rank
    << " in my_reports.history" << "... inserting: "
    << "(" << peer_rank << ", 1" << dendl;
    auto[j,k] = my_reports.history.insert(std::pair<int,double>(peer_rank,1.0));
    i = j;
  }
  double& pscore = i->second;
  ldout(cct, 30) << "adding new pscore to my_reports" << dendl;
  pscore = pscore * (1 - units_dead / (2 * half_life)) -
    (units_dead / (2*half_life));
  pscore = std::max(pscore, 0.0);
  my_reports.current[peer_rank] = false;
  
  increase_version();
  ldout(cct, 30) << "my_reports after: " << my_reports << dendl;
}

void ConnectionTracker::get_total_connection_score(int peer_rank, double *rating,
						    int *live_count) const
{
  ldout(cct, 30) << __func__ << dendl;
  *rating = 0;
  *live_count = 0;
  double rate = 0;
  int live = 0;

  for (const auto& i : peer_reports) { // loop through all the scores
    if (i.first == peer_rank) { // ... except the ones it has for itself, of course!
      continue;
    }
    const auto& report = i.second;
    auto score_i = report.history.find(peer_rank);
    auto live_i = report.current.find(peer_rank);
    if (score_i != report.history.end()) {
      if (live_i->second) {
	rate += score_i->second;
	++live;
      }
    }
  }
  *rating = rate;
  *live_count = live;
}

void ConnectionTracker::notify_rank_changed(int new_rank)
{
  ldout(cct, 20) << __func__ << " to " << new_rank << dendl;
  if (new_rank == rank) return;
  ldout(cct, 20) << "peer_reports before: " << peer_reports << dendl;
  peer_reports.erase(rank);
  peer_reports.erase(new_rank);
  my_reports.rank = new_rank;
  rank = new_rank;
  encoding.clear();
  ldout(cct, 20) << "peer_reports after: " << peer_reports << dendl;

  increase_version();
}

void ConnectionTracker::notify_rank_removed(int rank_removed, int new_rank)
{
  ldout(cct, 20) << __func__ << " " << rank_removed
    << " new_rank: " << new_rank << dendl;
  ldout(cct, 20) << "my_reports before: " << my_reports << dendl;
  ldout(cct, 20) << "peer_reports before: " << peer_reports << dendl;
  ldout(cct, 20) << "my rank before: " << rank << dendl;

  encoding.clear();
  size_t starting_size_current = my_reports.current.size();
  // Lets adjust everything in my report.
  my_reports.current.erase(rank_removed);
  my_reports.history.erase(rank_removed);
  auto ci = my_reports.current.upper_bound(rank_removed);
  auto hi = my_reports.history.upper_bound(rank_removed);
  while (ci != my_reports.current.end()) {
    ceph_assert(ci->first == hi->first);
    my_reports.current[ci->first - 1] = ci->second;
    my_reports.history[hi->first - 1] = hi->second;
    my_reports.current.erase(ci++);
    my_reports.history.erase(hi++);
  }
  ceph_assert((my_reports.current.size() == starting_size_current) ||
    (my_reports.current.size() + 1 == starting_size_current));

  size_t starting_size = peer_reports.size();
  auto pi = peer_reports.upper_bound(rank_removed);
  // Remove the target rank and adjust everything that comes after.
  // Note that we don't adjust current and history for our peer_reports
  // because it is better to rely on our peers on that information.
  peer_reports.erase(rank_removed);
  while (pi != peer_reports.end()) {
    peer_reports[pi->first - 1] = pi->second; // copy content of next rank to ourself.
    peer_reports.erase(pi++); // destroy our next rank and move on.
  }

  ceph_assert((peer_reports.size() == starting_size) ||
	  (peer_reports.size() + 1 == starting_size));

  if (rank_removed < rank) { // if the rank removed is lower than us, we need to adjust.
    --rank;
    my_reports.rank = rank; // also adjust my_reports.rank.
  }

  ldout(cct, 20) << "my rank after: " << rank << dendl;
  ldout(cct, 20) << "peer_reports after: " << peer_reports << dendl;
  ldout(cct, 20) << "my_reports after: " << my_reports << dendl;

  //check if the new_rank from monmap is equal to our adjusted rank.
  ceph_assert(rank == new_rank);

  increase_version();
}

bool ConnectionTracker::is_clean(int mon_rank, int monmap_size)
{
  ldout(cct, 30) << __func__ << dendl;
  // check consistency between our rank according
  // to monmap and our rank according to our report.
  if (rank != mon_rank ||
    my_reports.rank != mon_rank) {
    return false;
  } else if (!peer_reports.empty()){
    // if peer_report max rank is greater than monmap max rank
    // then there is a problem.
    if (peer_reports.rbegin()->first > monmap_size - 1) return false;
  }
  return true;
}

void ConnectionTracker::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  encode(rank, bl);
  encode(epoch, bl);
  encode(version, bl);
  encode(half_life, bl);
  encode(peer_reports, bl);
  ENCODE_FINISH(bl);
}

void ConnectionTracker::decode(bufferlist::const_iterator& bl) {
  clear_peer_reports();
  encoding.clear();

  DECODE_START(1, bl);
  decode(rank, bl);
  decode(epoch, bl);
  decode(version, bl);
  decode(half_life, bl);
  decode(peer_reports, bl);
  DECODE_FINISH(bl);
  if (rank >=0)
    my_reports = peer_reports[rank];
}

const bufferlist& ConnectionTracker::get_encoded_bl()
{
  if (!encoding.length()) {
    encode(encoding);
  }
  return encoding;
}

void ConnectionReport::dump(ceph::Formatter *f) const
{
  f->dump_int("rank", rank);
  f->dump_int("epoch", epoch);
  f->dump_int("version", epoch_version);
  f->open_object_section("peer_scores");
  for (auto i : history) {
    f->open_object_section("peer");
    f->dump_int("peer_rank", i.first);
    f->dump_float("peer_score", i.second);
    f->dump_bool("peer_alive", current.find(i.first)->second);
    f->close_section();
  }
  f->close_section(); // peer scores
}

void ConnectionReport::generate_test_instances(std::list<ConnectionReport*>& o)
{
  o.push_back(new ConnectionReport);
  o.push_back(new ConnectionReport);
  o.back()->rank = 1;
  o.back()->epoch = 2;
  o.back()->epoch_version = 3;
  o.back()->current[0] = true;
  o.back()->history[0] = .4;
}

void ConnectionTracker::dump(ceph::Formatter *f) const
{
  f->dump_int("rank", rank);
  f->dump_int("epoch", epoch);
  f->dump_int("version", version);
  f->dump_float("half_life", half_life);
  f->dump_int("persist_interval", persist_interval);
  f->open_object_section("reports");
  for (const auto& i : peer_reports) {
    f->open_object_section("report");
    i.second.dump(f);
    f->close_section();
  }
  f->close_section(); // reports
}

void ConnectionTracker::generate_test_instances(std::list<ConnectionTracker*>& o)
{
  o.push_back(new ConnectionTracker);
  o.push_back(new ConnectionTracker);
  ConnectionTracker *e = o.back();
  e->rank = 2;
  e->epoch = 3;
  e->version = 4;
  e->peer_reports[0];
  e->peer_reports[1];
  e->my_reports = e->peer_reports[2];
}
