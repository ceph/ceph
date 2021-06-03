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

#include "Elector.h"
#include "Monitor.h"

#include "common/Timer.h"
#include "MonitorDBStore.h"
#include "messages/MMonElection.h"
#include "messages/MMonPing.h"

#include "common/config.h"
#include "include/ceph_assert.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, get_epoch())
static ostream& _prefix(std::ostream *_dout, Monitor *mon, epoch_t epoch) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").elector(" << epoch << ") ";
}

Elector::Elector(Monitor *m, int strategy) : logic(this, static_cast<ElectionLogic::election_strategy>(strategy),
						   &peer_tracker,
						   m->cct->_conf.get_val<double>("mon_elector_ignore_propose_margin"),
						   m->cct),
					     peer_tracker(this, m->rank,
					    m->cct->_conf.get_val<uint64_t>("mon_con_tracker_score_halflife"),
					    m->cct->_conf.get_val<uint64_t>("mon_con_tracker_persist_interval")),
			       ping_timeout(m->cct->_conf.get_val<double>("mon_elector_ping_timeout")),
			       PING_DIVISOR(m->cct->_conf.get_val<uint64_t>("mon_elector_ping_divisor")),
			       mon(m), elector(this) {
  bufferlist bl;
  mon->store->get(Monitor::MONITOR_NAME, "connectivity_scores", bl);
  if (bl.length()) {
    bufferlist::const_iterator bi = bl.begin();
    peer_tracker.decode(bi);
  }
}


void Elector::persist_epoch(epoch_t e)
{
  auto t(std::make_shared<MonitorDBStore::Transaction>());
  t->put(Monitor::MONITOR_NAME, "election_epoch", e);
  t->put(Monitor::MONITOR_NAME, "connectivity_scores", peer_tracker.get_encoded_bl());
  mon->store->apply_transaction(t);
}

void Elector::persist_connectivity_scores()
{
  auto t(std::make_shared<MonitorDBStore::Transaction>());
  t->put(Monitor::MONITOR_NAME, "connectivity_scores", peer_tracker.get_encoded_bl());
  mon->store->apply_transaction(t);
}

epoch_t Elector::read_persisted_epoch() const
{
  return mon->store->get(Monitor::MONITOR_NAME, "election_epoch");
}

void Elector::validate_store()
{
  auto t(std::make_shared<MonitorDBStore::Transaction>());
  t->put(Monitor::MONITOR_NAME, "election_writeable_test", rand());
  int r = mon->store->apply_transaction(t);
  ceph_assert(r >= 0);
}

bool Elector::is_current_member(int rank) const
{
  return mon->quorum.count(rank);
}

void Elector::trigger_new_election()
{
  mon->start_election();
}

int Elector::get_my_rank() const
{
  return mon->rank;
}

void Elector::reset_election()
{
  mon->bootstrap();
}

bool Elector::ever_participated() const
{
  return mon->has_ever_joined;
}

unsigned Elector::paxos_size() const
{
  return (unsigned)mon->monmap->size();
}

void Elector::shutdown()
{
  cancel_timer();
}

void Elector::notify_bump_epoch()
{
  mon->join_election();
}

void Elector::propose_to_peers(epoch_t e, bufferlist& logic_bl)
{
  // bcast to everyone else
  for (unsigned i=0; i<mon->monmap->size(); ++i) {
    if ((int)i == mon->rank) continue;
    MMonElection *m =
      new MMonElection(MMonElection::OP_PROPOSE, e,
		       peer_tracker.get_encoded_bl(),
		       logic.strategy, mon->monmap);
    m->sharing_bl = logic_bl;
    m->mon_features = ceph::features::mon::get_supported();
    m->mon_release = ceph_release();
    mon->send_mon_message(m, i);
  }  
}

void Elector::_start()
{
  peer_info.clear();
  peer_info[mon->rank].cluster_features = CEPH_FEATURES_ALL;
  peer_info[mon->rank].mon_release = ceph_release();
  peer_info[mon->rank].mon_features = ceph::features::mon::get_supported();
  mon->collect_metadata(&peer_info[mon->rank].metadata);
  reset_timer();
}

void Elector::_defer_to(int who)
{
  MMonElection *m = new MMonElection(MMonElection::OP_ACK, get_epoch(),
				     peer_tracker.get_encoded_bl(),
				     logic.strategy, mon->monmap);
  m->mon_features = ceph::features::mon::get_supported();
  m->mon_release = ceph_release();
  mon->collect_metadata(&m->metadata);

  mon->send_mon_message(m, who);
  
  // set a timer
  reset_timer(1.0);  // give the leader some extra time to declare victory
}


void Elector::reset_timer(double plus)
{
  // set the timer
  cancel_timer();
  /**
   * This class is used as the callback when the expire_event timer fires up.
   *
   * If the expire_event is fired, then it means that we had an election going,
   * either started by us or by some other participant, but it took too long,
   * thus expiring.
   *
   * When the election expires, we will check if we were the ones who won, and
   * if so we will declare victory. If that is not the case, then we assume
   * that the one we defered to didn't declare victory quickly enough (in fact,
   * as far as we know, we may even be dead); so, just propose ourselves as the
   * Leader.
   */
  expire_event = mon->timer.add_event_after(
    g_conf()->mon_election_timeout + plus,
    new C_MonContext(mon, [this](int) {
	logic.end_election_period();
      }));
}


void Elector::cancel_timer()
{
  if (expire_event) {
    mon->timer.cancel_event(expire_event);
    expire_event = 0;
  }
}

void Elector::assimilate_connection_reports(const bufferlist& tbl)
{
  ConnectionTracker pct(tbl);
  peer_tracker.receive_peer_report(pct);
}

void Elector::message_victory(const std::set<int>& quorum)
{
  uint64_t cluster_features = CEPH_FEATURES_ALL;
  mon_feature_t mon_features = ceph::features::mon::get_supported();
  map<int,Metadata> metadata;
  int min_mon_release = -1;
  for (auto id : quorum) {
    auto i = peer_info.find(id);
    ceph_assert(i != peer_info.end());
    auto& info = i->second;
    cluster_features &= info.cluster_features;
    mon_features &= info.mon_features;
    metadata[id] = info.metadata;
    if (min_mon_release < 0 ||
	info.mon_release < min_mon_release) {
      min_mon_release = info.mon_release;
    }
  }

  cancel_timer();
  

  // tell everyone!
  for (set<int>::iterator p = quorum.begin();
       p != quorum.end();
       ++p) {
    if (*p == mon->rank) continue;
    MMonElection *m = new MMonElection(MMonElection::OP_VICTORY, get_epoch(),
				       peer_tracker.get_encoded_bl(),
				       logic.strategy, mon->monmap);
    m->quorum = quorum;
    m->quorum_features = cluster_features;
    m->mon_features = mon_features;
    m->sharing_bl = mon->get_local_commands_bl(mon_features);
    m->mon_release = min_mon_release;
    mon->send_mon_message(m, *p);
  }

  // tell monitor
  mon->win_election(get_epoch(), quorum,
                    cluster_features, mon_features, min_mon_release,
		    metadata);
}


void Elector::handle_propose(MonOpRequestRef op)
{
  op->mark_event("elector:handle_propose");
  MMonElection *m = static_cast<MMonElection*>(op->get_req());
  dout(5) << "handle_propose from " << m->get_source() << dendl;
  int from = m->get_source().num();

  ceph_assert(m->epoch % 2 == 1); // election
  uint64_t required_features = mon->get_required_features();
  mon_feature_t required_mon_features = mon->get_required_mon_features();

  dout(10) << __func__ << " required features " << required_features
           << " " << required_mon_features
           << ", peer features " << m->get_connection()->get_features()
           << " " << m->mon_features
           << dendl;

  if ((required_features ^ m->get_connection()->get_features()) &
      required_features) {
    dout(5) << " ignoring propose from mon" << from
	    << " without required features" << dendl;
    nak_old_peer(op);
    return;
  } else if (mon->monmap->min_mon_release > m->mon_release) {
    dout(5) << " ignoring propose from mon" << from
	    << " release " << (int)m->mon_release
	    << " < min_mon_release " << (int)mon->monmap->min_mon_release
	    << dendl;
    nak_old_peer(op);
    return;
  } else if (!m->mon_features.contains_all(required_mon_features)) {
    // all the features in 'required_mon_features' not in 'm->mon_features'
    mon_feature_t missing = required_mon_features.diff(m->mon_features);
    dout(5) << " ignoring propose from mon." << from
            << " without required mon_features " << missing
            << dendl;
    nak_old_peer(op);
  }
  ConnectionTracker *oct = NULL;
  if (m->sharing_bl.length()) {
    oct = new ConnectionTracker(m->sharing_bl);
  }
  logic.receive_propose(from, m->epoch, oct);
  delete oct;
}

void Elector::handle_ack(MonOpRequestRef op)
{
  op->mark_event("elector:handle_ack");
  MMonElection *m = static_cast<MMonElection*>(op->get_req());
  dout(5) << "handle_ack from " << m->get_source() << dendl;
  int from = m->get_source().num();

  ceph_assert(m->epoch == get_epoch());
  uint64_t required_features = mon->get_required_features();
  if ((required_features ^ m->get_connection()->get_features()) &
      required_features) {
    dout(5) << " ignoring ack from mon" << from
	    << " without required features" << dendl;
    return;
  }

  mon_feature_t required_mon_features = mon->get_required_mon_features();
  if (!m->mon_features.contains_all(required_mon_features)) {
    mon_feature_t missing = required_mon_features.diff(m->mon_features);
    dout(5) << " ignoring ack from mon." << from
            << " without required mon_features " << missing
            << dendl;
    return;
  }

  if (logic.electing_me) {
    // thanks
    peer_info[from].cluster_features = m->get_connection()->get_features();
    peer_info[from].mon_features = m->mon_features;
    peer_info[from].mon_release = m->mon_release;
    peer_info[from].metadata = m->metadata;
    dout(5) << " so far i have {";
    for (auto q = logic.acked_me.begin();
         q != logic.acked_me.end();
         ++q) {
      auto p = peer_info.find(*q);
      ceph_assert(p != peer_info.end());
      if (q != logic.acked_me.begin())
        *_dout << ",";
      *_dout << " mon." << p->first << ":"
             << " features " << p->second.cluster_features
             << " " << p->second.mon_features;
    }
    *_dout << " }" << dendl;
  }

  logic.receive_ack(from, m->epoch);
}

void Elector::handle_victory(MonOpRequestRef op)
{
  op->mark_event("elector:handle_victory");
  MMonElection *m = static_cast<MMonElection*>(op->get_req());
  dout(5) << "handle_victory from " << m->get_source()
          << " quorum_features " << m->quorum_features
          << " " << m->mon_features
          << dendl;
  int from = m->get_source().num();

  bool accept_victory = logic.receive_victory_claim(from, m->epoch);

  if (!accept_victory) {
    return;
  }

  mon->lose_election(get_epoch(), m->quorum, from,
                     m->quorum_features, m->mon_features, m->mon_release);

  // cancel my timer
  cancel_timer();

  // stash leader's commands
  ceph_assert(m->sharing_bl.length());
  vector<MonCommand> new_cmds;
  auto bi = m->sharing_bl.cbegin();
  MonCommand::decode_vector(new_cmds, bi);
  mon->set_leader_commands(new_cmds);
}

void Elector::nak_old_peer(MonOpRequestRef op)
{
  op->mark_event("elector:nak_old_peer");
  MMonElection *m = static_cast<MMonElection*>(op->get_req());
  uint64_t supported_features = m->get_connection()->get_features();
  uint64_t required_features = mon->get_required_features();
  mon_feature_t required_mon_features = mon->get_required_mon_features();
  dout(10) << "sending nak to peer " << m->get_source()
	   << " supports " << supported_features << " " << m->mon_features
	   << ", required " << required_features << " " << required_mon_features
	   << ", release " << (int)m->mon_release
	   << " vs required " << (int)mon->monmap->min_mon_release
	   << dendl;
  MMonElection *reply = new MMonElection(MMonElection::OP_NAK, m->epoch,
                                         peer_tracker.get_encoded_bl(),
					 logic.strategy, mon->monmap);
  reply->quorum_features = required_features;
  reply->mon_features = required_mon_features;
  reply->mon_release = mon->monmap->min_mon_release;
  mon->features.encode(reply->sharing_bl);
  m->get_connection()->send_message(reply);
}

void Elector::handle_nak(MonOpRequestRef op)
{
  op->mark_event("elector:handle_nak");
  MMonElection *m = static_cast<MMonElection*>(op->get_req());
  dout(1) << "handle_nak from " << m->get_source()
	  << " quorum_features " << m->quorum_features
          << " " << m->mon_features
	  << " min_mon_release " << (int)m->mon_release
          << dendl;

  if (m->mon_release > ceph_release()) {
    derr << "Shutting down because I am release " << (int)ceph_release()
	 << " < min_mon_release " << (int)m->mon_release << dendl;
  } else {
    CompatSet other;
    auto bi = m->sharing_bl.cbegin();
    other.decode(bi);
    CompatSet diff = Monitor::get_supported_features().unsupported(other);

    mon_feature_t mon_supported = ceph::features::mon::get_supported();
    // all features in 'm->mon_features' not in 'mon_supported'
    mon_feature_t mon_diff = m->mon_features.diff(mon_supported);

    derr << "Shutting down because I lack required monitor features: { "
	 << diff << " } " << mon_diff << dendl;
  }
  exit(0);
  // the end!
}

void Elector::begin_peer_ping(int peer)
{
  if (live_pinging.count(peer)) {
    return;
  }

  if (!mon->get_quorum_mon_features().contains_all(
				      ceph::features::mon::FEATURE_PINGING)) {
    return;
  }

  dout(5) << __func__ << " against " << peer << dendl;

  peer_tracker.report_live_connection(peer, 0); // init this peer as existing
  live_pinging.insert(peer);
  dead_pinging.erase(peer);
  peer_acked_ping[peer] = ceph_clock_now();
  send_peer_ping(peer);
  mon->timer.add_event_after(ping_timeout / PING_DIVISOR,
			     new C_MonContext{mon, [this, peer](int) {
				 ping_check(peer);
			       }});
}

void Elector::send_peer_ping(int peer, const utime_t *n)
{
  dout(10) << __func__ << " to peer " << peer << dendl;

  utime_t now;
  if (n != NULL) {
    now = *n;
  } else {
    now = ceph_clock_now();
  }
  MMonPing *ping = new MMonPing(MMonPing::PING, now, peer_tracker.get_encoded_bl());
  mon->messenger->send_to_mon(ping, mon->monmap->get_addrs(peer));
  peer_sent_ping[peer] = now;
}

void Elector::ping_check(int peer)
{
  dout(20) << __func__ << " to peer " << peer << dendl;
  if (!live_pinging.count(peer) &&
      !dead_pinging.count(peer)) {
    dout(20) << __func__ << peer << " is no longer marked for pinging" << dendl;
    return;
  }
  utime_t now = ceph_clock_now();
  utime_t& acked_ping = peer_acked_ping[peer];
  utime_t& newest_ping = peer_sent_ping[peer];
  if (!acked_ping.is_zero() && acked_ping < now - ping_timeout) {
    peer_tracker.report_dead_connection(peer, now - acked_ping);
    acked_ping = now;
    begin_dead_ping(peer);
    return;
  }

  if (acked_ping == newest_ping) {
    send_peer_ping(peer, &now);
  }

  mon->timer.add_event_after(ping_timeout / PING_DIVISOR,
			     new C_MonContext{mon, [this, peer](int) {
				 ping_check(peer);
			       }});
}

void Elector::begin_dead_ping(int peer)
{
  if (dead_pinging.count(peer)) {
    return;
  }
  
  live_pinging.erase(peer);
  dead_pinging.insert(peer);
  mon->timer.add_event_after(ping_timeout,
			     new C_MonContext{mon, [this, peer](int) {
				 dead_ping(peer);
			       }});
}

void Elector::dead_ping(int peer)
{
  dout(20) << __func__ << " to peer " << peer << dendl;
  if (!dead_pinging.count(peer)) {
    dout(20) << __func__ << peer << " is no longer marked for dead pinging" << dendl;
    return;
  }
  ceph_assert(!live_pinging.count(peer));

  utime_t now = ceph_clock_now();
  utime_t& acked_ping = peer_acked_ping[peer];

  peer_tracker.report_dead_connection(peer, now - acked_ping);
  acked_ping = now;
  mon->timer.add_event_after(ping_timeout,
			       new C_MonContext{mon, [this, peer](int) {
				   dead_ping(peer);
				 }});
}

void Elector::handle_ping(MonOpRequestRef op)
{
  MMonPing *m = static_cast<MMonPing*>(op->get_req());
  dout(10) << __func__ << " " << *m << dendl;

  int prank = mon->monmap->get_rank(m->get_source_addr());
  begin_peer_ping(prank);
  assimilate_connection_reports(m->tracker_bl);
  switch(m->op) {
  case MMonPing::PING:
    {
      MMonPing *reply = new MMonPing(MMonPing::PING_REPLY, m->stamp, peer_tracker.get_encoded_bl());
      m->get_connection()->send_message(reply);
    }
    break;

  case MMonPing::PING_REPLY:
    const utime_t& previous_acked = peer_acked_ping[prank];
    const utime_t& newest = peer_sent_ping[prank];
    if (m->stamp > newest && !newest.is_zero()) {
      derr << "dropping PING_REPLY stamp " << m->stamp
	   << " as it is newer than newest sent " << newest << dendl;
      return;
    }
    if (m->stamp > previous_acked) {
      peer_tracker.report_live_connection(prank, m->stamp - previous_acked);
      peer_acked_ping[prank] = m->stamp;
    }
    utime_t now = ceph_clock_now();
    if (now - m->stamp > ping_timeout / PING_DIVISOR) {
      send_peer_ping(prank, &now);
    }
    break;
  }
}

void Elector::dispatch(MonOpRequestRef op)
{
  op->mark_event("elector:dispatch");
  ceph_assert(op->is_type_election_or_ping());

  switch (op->get_req()->get_type()) {
    
  case MSG_MON_ELECTION:
    {
      if (!logic.participating) {
        return;
      }
      if (op->get_req()->get_source().num() >= mon->monmap->size()) {
	dout(5) << " ignoring bogus election message with bad mon rank " 
		<< op->get_req()->get_source() << dendl;
	return;
      }

      MMonElection *em = static_cast<MMonElection*>(op->get_req());

      // assume an old message encoding would have matched
      if (em->fsid != mon->monmap->fsid) {
	dout(0) << " ignoring election msg fsid " 
		<< em->fsid << " != " << mon->monmap->fsid << dendl;
	return;
      }

      if (!mon->monmap->contains(em->get_source_addr())) {
	dout(1) << "discarding election message: " << em->get_source_addr()
		<< " not in my monmap " << *mon->monmap << dendl;
	return;
      }

      MonMap peermap;
      peermap.decode(em->monmap_bl);
      if (peermap.epoch > mon->monmap->epoch) {
	dout(0) << em->get_source_inst() << " has newer monmap epoch " << peermap.epoch
		<< " > my epoch " << mon->monmap->epoch 
		<< ", taking it"
		<< dendl;
	mon->monmap->decode(em->monmap_bl);
        auto t(std::make_shared<MonitorDBStore::Transaction>());
        t->put("monmap", mon->monmap->epoch, em->monmap_bl);
        t->put("monmap", "last_committed", mon->monmap->epoch);
        mon->store->apply_transaction(t);
	//mon->monmon()->paxos->stash_latest(mon->monmap->epoch, em->monmap_bl);
	cancel_timer();
	mon->notify_new_monmap(false);
	mon->bootstrap();
	return;
      }
      if (peermap.epoch < mon->monmap->epoch) {
	dout(0) << em->get_source_inst() << " has older monmap epoch " << peermap.epoch
		<< " < my epoch " << mon->monmap->epoch 
		<< dendl;
      }

      if (em->strategy != logic.strategy) {
	dout(5) << __func__ << " somehow got an Election message with different strategy "
		<< em->strategy << " from local " << logic.strategy
		<< "; dropping for now to let race resolve" << dendl;
	return;
      }

      if (em->scoring_bl.length()) {
	assimilate_connection_reports(em->scoring_bl);
      }

      begin_peer_ping(mon->monmap->get_rank(em->get_source_addr()));
      switch (em->op) {
      case MMonElection::OP_PROPOSE:
	handle_propose(op);
	return;
      }

      if (em->epoch < get_epoch()) {
	dout(5) << "old epoch, dropping" << dendl;
	break;
      }

      switch (em->op) {
      case MMonElection::OP_ACK:
	handle_ack(op);
	return;
      case MMonElection::OP_VICTORY:
	handle_victory(op);
	return;
      case MMonElection::OP_NAK:
	handle_nak(op);
	return;
      default:
	ceph_abort();
      }
    }
    break;

  case MSG_MON_PING:
    handle_ping(op);
    break;
    
  default: 
    ceph_abort();
  }
}

void Elector::start_participating()
{
  logic.participating = true;
}

void Elector::notify_clear_peer_state()
{
  peer_tracker.notify_reset();
}

void Elector::notify_rank_changed(int new_rank)
{
  peer_tracker.notify_rank_changed(new_rank);
  live_pinging.erase(new_rank);
  dead_pinging.erase(new_rank);
}

void Elector::notify_rank_removed(int rank_removed)
{
  peer_tracker.notify_rank_removed(rank_removed);
  /* we have to clean up the pinging state, which is annoying
     because it's not indexed anywhere (and adding indexing
     would also be annoying). So what we do is start with the
     remoed rank and examine the state of the surrounding ranks.
     Everybody who remains with larger rank gets a new rank one lower
     than before, and we have to figure out the remaining scheduled
     ping contexts. So, starting one past with the removed rank, we:
     * check if the current rank is alive or dead
     * examine our new rank (one less than before, initially the removed
     rank)
     * * erase it if it's in the wrong set
     * * start pinging it if we're not already
     * check if the next rank is in the same pinging set, and delete
     * ourselves if not.
   */
  for (unsigned i = rank_removed + 1; i <= paxos_size() ; ++i) {
    if (live_pinging.count(i)) {
      dead_pinging.erase(i-1);
      if (!live_pinging.count(i-1)) {
	begin_peer_ping(i-1);
      }
      if (!live_pinging.count(i+1)) {
	live_pinging.erase(i);
      }
    }
    else if (dead_pinging.count(i)) {
      live_pinging.erase(i-1);
      if (!dead_pinging.count(i-1)) {
	begin_dead_ping(i-1);
      }
      if (!dead_pinging.count(i+1)) {
	dead_pinging.erase(i);
      }
    } else {
      // we aren't pinging rank i at all
      if (i-1 == (unsigned)rank_removed) {
	// so we special case to make sure we
	// actually nuke the removed rank
	dead_pinging.erase(rank_removed);
	live_pinging.erase(rank_removed);
      }
    }
  }
}

void Elector::notify_strategy_maybe_changed(int strategy)
{
  logic.set_election_strategy(static_cast<ElectionLogic::election_strategy>(strategy));
}
