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

#include "ElectionLogic.h"

#include "include/ceph_assert.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, epoch, elector)
using std::cerr;
using std::cout;
using std::dec;
using std::hex;
using std::list;
using std::map;
using std::make_pair;
using std::ostream;
using std::ostringstream;
using std::pair;
using std::set;
using std::setfill;
using std::string;
using std::stringstream;
using std::to_string;
using std::vector;
using std::unique_ptr;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;
using ceph::Formatter;
using ceph::JSONFormatter;
using ceph::mono_clock;
using ceph::mono_time;
using ceph::timespan_str;
static ostream& _prefix(std::ostream *_dout, epoch_t epoch, ElectionOwner* elector) {
  return *_dout << "paxos." << elector->get_my_rank()
		<< ").electionLogic(" <<  epoch << ") ";
}
void ElectionLogic::init()
{
  epoch = elector->read_persisted_epoch();
  if (!epoch) {
    ldout(cct, 1) << "init, first boot, initializing epoch at 1 " << dendl;
    epoch = 1;
  } else if (epoch % 2) {
    ldout(cct, 1) << "init, last seen epoch " << epoch
	    << ", mid-election, bumping" << dendl;
    ++epoch;
    elector->persist_epoch(epoch);
  } else {
    ldout(cct, 1) << "init, last seen epoch " << epoch << dendl;
  }
}

void ElectionLogic::bump_epoch(epoch_t e)
{
  ldout(cct, 10) << __func__ << " to "  << e << dendl;
  ceph_assert(epoch <= e);
  epoch = e;
  peer_tracker->increase_epoch(e);
  elector->persist_epoch(epoch);
  // clear up some state
  electing_me = false;
  acked_me.clear();
  elector->notify_bump_epoch();
}

void ElectionLogic::declare_standalone_victory()
{
  assert(elector->paxos_size() == 1 && elector->get_my_rank() == 0);
  init();
  bump_epoch(epoch+1);
}

void ElectionLogic::clear_live_election_state()
{
  leader_acked = -1;
  electing_me = false;
  reset_stable_tracker();
  leader_peer_tracker.reset();
}

void ElectionLogic::reset_stable_tracker()
{
  stable_peer_tracker.reset(new ConnectionTracker(*peer_tracker));
}

void ElectionLogic::connectivity_bump_epoch_in_election(epoch_t mepoch)
{
  ldout(cct, 30) << __func__ << " to " << mepoch << dendl;
  ceph_assert(mepoch > epoch);
  bump_epoch(mepoch);
  reset_stable_tracker();
  double lscore, my_score;
  my_score = connectivity_election_score(elector->get_my_rank());
  lscore = connectivity_election_score(leader_acked);
  if (my_score > lscore) {
    leader_acked = -1;
    leader_peer_tracker.reset();
  }
}

void ElectionLogic::start()
{
  if (!participating) {
    ldout(cct, 0) << "not starting new election -- not participating" << dendl;
    return;
  }
  ldout(cct, 5) << "start -- can i be leader?" << dendl;

  acked_me.clear();
  init();
  
  // start by trying to elect me
  if (epoch % 2 == 0) {
    bump_epoch(epoch+1);  // odd == election cycle
  } else {
    elector->validate_store();
  }
  acked_me.insert(elector->get_my_rank());
  clear_live_election_state();
  reset_stable_tracker();
  electing_me = true;

  bufferlist bl;
  if (strategy == CONNECTIVITY) {
    stable_peer_tracker->encode(bl);
  }
  elector->propose_to_peers(epoch, bl);
  elector->_start();
}

void ElectionLogic::defer(int who)
{
  if (strategy == CLASSIC) {
      ldout(cct, 5) << "defer to " << who << dendl;
      ceph_assert(who < elector->get_my_rank());
  } else {
    ldout(cct, 5) << "defer to " << who << ", disallowed_leaders=" << elector->get_disallowed_leaders() << dendl;
    ceph_assert(!elector->get_disallowed_leaders().count(who));
  }

  if (electing_me) {
    // drop out
    acked_me.clear();
    electing_me = false;
  }

  // ack them
  leader_acked = who;
  elector->_defer_to(who);
}

void ElectionLogic::end_election_period()
{
  ldout(cct, 5) << "election period ended" << dendl;
  
  // did i win?
  if (electing_me &&
      acked_me.size() > (elector->paxos_size() / 2)) {
    // i win
    declare_victory();
  } else {
    // whoever i deferred to didn't declare victory quickly enough.
    if (elector->ever_participated())
      start();
    else
      elector->reset_election();
  }
}


void ElectionLogic::declare_victory()
{
  ldout(cct, 5) << "I win! acked_me=" << acked_me << dendl;
  last_election_winner = elector->get_my_rank();
  last_voted_for = last_election_winner;
  clear_live_election_state();

  set<int> new_quorum;
  new_quorum.swap(acked_me);
  
  ceph_assert(epoch % 2 == 1);  // election
  bump_epoch(epoch+1);     // is over!

  elector->message_victory(new_quorum);
}

bool ElectionLogic::propose_classic_prefix(int from, epoch_t mepoch)
{
  if (mepoch > epoch) {
    bump_epoch(mepoch);
  } else if (mepoch < epoch) {
    // got an "old" propose,
    if (epoch % 2 == 0 &&    // in a non-election cycle
	!elector->is_current_member(from)) {  // from someone outside the quorum
      // a mon just started up, call a new election so they can rejoin!
      ldout(cct, 5) << " got propose from old epoch, "
	      << from << " must have just started" << dendl;
      // we may be active; make sure we reset things in the monitor appropriately.
      elector->trigger_new_election();
    } else {
      ldout(cct, 5) << " ignoring old propose" << dendl;
    }
    return true;
  }
  return false;
}

void ElectionLogic::receive_propose(int from, epoch_t mepoch,
				    const ConnectionTracker *ct)
{
  ldout(cct, 20) << __func__ << " from " << from << dendl;
  if (from == elector->get_my_rank()) {
    lderr(cct) << "I got a propose from my own rank, hopefully this is startup weirdness,dropping" << dendl;
    return;
  }
  switch (strategy) {
  case CLASSIC:
    propose_classic_handler(from, mepoch);
    break;
  case DISALLOW:
    propose_disallow_handler(from, mepoch);
    break;
  case CONNECTIVITY:
    propose_connectivity_handler(from, mepoch, ct);
    break;
  default:
    ceph_assert(0 == "how did election strategy become an invalid value?");
  }
}

void ElectionLogic::propose_disallow_handler(int from, epoch_t mepoch)
{
  if (propose_classic_prefix(from, mepoch)) {
    return;
  }
  const set<int>& disallowed_leaders = elector->get_disallowed_leaders();
  int my_rank = elector->get_my_rank();
  bool me_disallowed = disallowed_leaders.count(my_rank);
  bool from_disallowed = disallowed_leaders.count(from);
  bool my_win = !me_disallowed && // we are allowed to lead
    (my_rank < from || from_disallowed); // we are a better choice than them
  bool their_win = !from_disallowed && // they are allowed to lead
    (my_rank > from || me_disallowed) && // they are a better choice than us
    (leader_acked < 0 || leader_acked >= from); // they are a better choice than our previously-acked choice
    
  
  if (my_win) {
    // i would win over them.
    if (leader_acked >= 0) {        // we already acked someone
      ceph_assert(leader_acked < from || from_disallowed);  // and they still win, of course
      ldout(cct, 5) << "no, we already acked " << leader_acked << dendl;
    } else {
      // wait, i should win!
      if (!electing_me) {
	elector->trigger_new_election();
      }
    }
  } else {
    // they would win over me
    if (their_win) {
      defer(from);
    } else {
      // ignore them!
      ldout(cct, 5) << "no, we already acked " << leader_acked << dendl;
    }
  }
}

void ElectionLogic::propose_classic_handler(int from, epoch_t mepoch)
{
  if (propose_classic_prefix(from, mepoch)) {
    return;
  }
  if (elector->get_my_rank() < from) {
    // i would win over them.
    if (leader_acked >= 0) {        // we already acked someone
      ceph_assert(leader_acked < from);  // and they still win, of course
      ldout(cct, 5) << "no, we already acked " << leader_acked << dendl;
    } else {
      // wait, i should win!
      if (!electing_me) {
	elector->trigger_new_election();
      }
    }
  } else {
    // they would win over me
    if (leader_acked < 0 || // haven't acked anyone yet, or
	leader_acked > from ||   // they would win over who you did ack, or
	leader_acked == from) {  // this is the guy we're already deferring to
      defer(from);
    } else {
      // ignore them!
      ldout(cct, 5) << "no, we already acked " << leader_acked << dendl;
    }
  }
}

double ElectionLogic::connectivity_election_score(int rank)
{
  ldout(cct, 30) << __func__ << " of " << rank << dendl;
  if (elector->get_disallowed_leaders().count(rank)) {
    return -1;
  }
  double score;
  int liveness;
  if (stable_peer_tracker) {
    ldout(cct, 30) << "stable_peer_tracker exists so using that ..." << dendl;
    stable_peer_tracker->get_total_connection_score(rank, &score, &liveness);
  } else {
    ldout(cct, 30) << "stable_peer_tracker does not exists, using peer_tracker ..." << dendl;
    peer_tracker->get_total_connection_score(rank, &score, &liveness);
  }
  return score;
}

void ElectionLogic::propose_connectivity_handler(int from, epoch_t mepoch,
						 const ConnectionTracker *ct)
{
  ldout(cct, 10) << __func__ << " from " << from << " mepoch: "
    << mepoch << " epoch: " << epoch << dendl;
  ldout(cct, 30) << "last_election_winner: " << last_election_winner << dendl;
  if ((epoch % 2 == 0) &&
      last_election_winner != elector->get_my_rank() &&
      !elector->is_current_member(from)) {
    // To prevent election flapping, peons ignore proposals from out-of-quorum
    // peers unless their vote would materially change from the last election
    ldout(cct, 30) << "Lets see if this out-of-quorum peer is worth it " << dendl;
    int best_scorer = 0;
    double best_score = 0;
    double last_voted_for_score = 0;
    ldout(cct, 30) << "elector->paxos_size(): " << elector->paxos_size() << dendl;
    for (unsigned i = 0; i < elector->paxos_size(); ++i) {
      double score = connectivity_election_score(i);
      if (score > best_score) {
	best_scorer = i;
	best_score = score;
      }
      if (last_voted_for >= 0 && i == static_cast<unsigned>(last_voted_for)) {
	last_voted_for_score = score;
      }
    }
    ldout(cct, 30) << "best_scorer: " << best_scorer << " best_score: " << best_score
      << " last_voted_for: " << last_voted_for << " last_voted_for_score: " 
      << last_voted_for_score << dendl;
    if (best_scorer == last_voted_for ||
	(best_score - last_voted_for_score < ignore_propose_margin)) {
      // drop this message; it won't change our vote so we defer to leader
      ldout(cct, 30) << "drop this message; it won't change our vote so we defer to leader " << dendl;
      return;
    }
  }
  if (mepoch > epoch) {
    ldout(cct, 20) << "mepoch > epoch" << dendl;
    connectivity_bump_epoch_in_election(mepoch);
  } else if (mepoch < epoch) {
    // got an "old" propose,
    if (epoch % 2 == 0 &&    // in a non-election cycle
	!elector->is_current_member(from)) {  // from someone outside the quorum
      // a mon just started up, call a new election so they can rejoin!
      ldout(cct, 5) << " got propose from old epoch, "
	      << from << " must have just started" << dendl;
      ldout(cct, 10) << "triggering new election" << dendl;
      // we may be active; make sure we reset things in the monitor appropriately.
      elector->trigger_new_election();
    } else {
      ldout(cct, 5) << " ignoring old propose" << dendl;
    }
    return;
  }

  int my_rank = elector->get_my_rank();
  double my_score = connectivity_election_score(my_rank);
  double from_score = connectivity_election_score(from);
  double leader_score = -1;
  if (leader_acked >= 0) {
    leader_score = connectivity_election_score(leader_acked);
  }

  ldout(cct, 20) << "propose from rank=" << from << ", tracker: "
		 << (stable_peer_tracker ? *stable_peer_tracker : *peer_tracker) << dendl;

  ldout(cct, 10) << "propose from rank=" << from << ",from_score=" << from_score
		 << "; my score=" << my_score
		 << "; currently acked " << leader_acked
		 << ",leader_score=" << leader_score
     << ",disallowed_leaders=" << elector->get_disallowed_leaders() << dendl;

  bool my_win = (my_score >= 0) && // My score is non-zero; I am allowed to lead
    ((my_rank < from && my_score >= from_score) || // We have same scores and I have lower rank, or
     (my_score > from_score)); // my score is higher
  
  bool their_win = (from_score >= 0) && // Their score is non-zero; they're allowed to lead, AND
    ((from < my_rank && from_score >= my_score) || // Either they have lower rank and same score, or
     (from_score > my_score)) && // their score is higher, AND
    ((from <= leader_acked && from_score >= leader_score) || // same conditions compared to leader, or IS leader
     (from_score > leader_score));

  if (my_win) {
    ldout(cct, 10) << " conditionally I win" << dendl;
    // i would win over them.
    if (leader_acked >= 0) {        // we already acked someone
      ceph_assert(leader_score >= from_score);  // and they still win, of course
      ldout(cct, 5) << "no, we already acked " << leader_acked << dendl;
    } else {
      // wait, i should win!
      if (!electing_me) {
      ldout(cct, 10) << " wait, i should win! triggering new election ..." << dendl;
	elector->trigger_new_election();
      }
    }
  } else {
    ldout(cct, 10) << " conditionally they win" << dendl;
    // they would win over me
    if (their_win || from == leader_acked) {
      if (leader_acked >= 0 && from != leader_acked) {
	// we have to make sure our acked leader will ALSO defer to them, or else
	// we can't, to maintain guarantees!
  ldout(cct, 10) << " make sure acked leader defer to: " << from << dendl;
	double leader_from_score;
	int leader_from_liveness;
	leader_peer_tracker->
	  get_total_connection_score(from, &leader_from_score,
				     &leader_from_liveness);
	double leader_leader_score;
	int leader_leader_liveness;
	leader_peer_tracker->
	  get_total_connection_score(leader_acked, &leader_leader_score,
				     &leader_leader_liveness);
	if ((from < leader_acked && leader_from_score >= leader_leader_score) ||
	    (leader_from_score > leader_leader_score)) {
    ldout(cct, 10) << "defering to " << from << dendl;
	  defer(from);
	  leader_peer_tracker.reset(new ConnectionTracker(*ct));
	} else { // we can't defer to them *this* round even though they should win...
	  double cur_leader_score, cur_from_score;
	  int cur_leader_live, cur_from_live;
	  peer_tracker->get_total_connection_score(leader_acked, &cur_leader_score, &cur_leader_live);
	  peer_tracker->get_total_connection_score(from, &cur_from_score, &cur_from_live);
	  if ((from < leader_acked && cur_from_score >= cur_leader_score) ||
	      (cur_from_score > cur_leader_score)) {
	    ldout(cct, 5) << "Bumping epoch and starting new election; acked "
			  << leader_acked << " should defer to " << from
			  << " but there is score disagreement!" << dendl;
	    bump_epoch(epoch+1);
	    start();
	  } else {
	    ldout(cct, 5) << "no, we already acked " << leader_acked
			  << " and it won't defer to " << from
			  << " despite better round scores" << dendl;
	  }
	}
      } else {
  ldout(cct, 10) << "defering to " << from << dendl;
	defer(from);
	leader_peer_tracker.reset(new ConnectionTracker(*ct));
      }
    } else {
      // ignore them!
      ldout(cct, 5) << "no, we already acked " << leader_acked << " with score >=" << from_score << dendl;
    }
  }
}

void ElectionLogic::receive_ack(int from, epoch_t from_epoch)
{
  ceph_assert(from_epoch % 2 == 1); // sender in an election epoch
  if (from_epoch > epoch) {
    ldout(cct, 5) << "woah, that's a newer epoch, i must have rebooted.  bumping and re-starting!" << dendl;
    bump_epoch(from_epoch);
    start();
    return;
  }
  // is that _everyone_?
  if (electing_me) {
    acked_me.insert(from);
    if (acked_me.size() == elector->paxos_size()) {
      // if yes, shortcut to election finish
      declare_victory();
    }
  } else {
    // ignore, i'm deferring already.
    ceph_assert(leader_acked >= 0);
  }
}

bool ElectionLogic::victory_makes_sense(int from)
{
  bool makes_sense = false;
  switch (strategy) {
  case CLASSIC:
    makes_sense = (from < elector->get_my_rank());
    break;
  case DISALLOW:
    makes_sense = (from < elector->get_my_rank()) ||
      elector->get_disallowed_leaders().count(elector->get_my_rank());
    break;
  case CONNECTIVITY:
    double my_score, leader_score;
    my_score = connectivity_election_score(elector->get_my_rank());
    leader_score = connectivity_election_score(from);
    ldout(cct, 5) << "victory from " << from << " makes sense? lscore:"
		  << leader_score
		  << "; my score:" << my_score << dendl;

    makes_sense = (leader_score >= my_score);
    break;
  default:
    ceph_assert(0 == "how did you get a nonsense election strategy assigned?");
  }
  return makes_sense;
}

bool ElectionLogic::receive_victory_claim(int from, epoch_t from_epoch)
{
  bool election_okay = victory_makes_sense(from);

  last_election_winner = from;
  last_voted_for = leader_acked;
  clear_live_election_state();

  if (!election_okay) {
    ceph_assert(strategy == CONNECTIVITY);
    ldout(cct, 1) << "I should have been elected over this leader; bumping and restarting!" << dendl;
    bump_epoch(from_epoch);
    start();
    return false;
  }

  // i should have seen this election if i'm getting the victory.
  if (from_epoch != epoch + 1) { 
    ldout(cct, 5) << "woah, that's a funny epoch, i must have rebooted.  bumping and re-starting!" << dendl;
    bump_epoch(from_epoch);
    start();
    return false;
  }

  bump_epoch(from_epoch);

  // they win
  return true;
}
