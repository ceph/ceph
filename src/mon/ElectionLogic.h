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


#ifndef CEPH_ELECTIONLOGIC_H
#define CEPH_ELECTIONLOGIC_H

#include <map>
#include <memory>
#include <set>
#include "include/types.h"
#include "ConnectionTracker.h"

class ElectionOwner {
public:
  /**
   * Write down the given epoch in persistent storage, such that it
   * can later be retrieved by read_persisted_epoch even across process
   * or machine restarts.
   *
   * @param e The epoch to write
   */
  virtual void persist_epoch(epoch_t e) = 0;
  /**
   * Retrieve the most-previously-persisted epoch.
   *
   * @returns The latest epoch passed to persist_epoch()
   */
  virtual epoch_t read_persisted_epoch() const = 0;
  /**
   * Validate that the persistent store is working by committing
   * to it. (There is no interface for retrieving the value; this
   * tests local functionality before doing things like triggering
   * elections to try and join a quorum.)
   */
  virtual void validate_store() = 0;
  /**
   * Notify the ElectionOwner that ElectionLogic has increased its
   * election epoch. This resets an election (either on local loss or victory,
   * or when trying a new election round) and the ElectionOwner
   * should reset any tracking of its own to match. (The ElectionLogic
   * will further trigger sending election messages if that is
   * appropriate.)
   */
  virtual void notify_bump_epoch() = 0;
  /**
   * Notify the ElectionOwner we must start a new election.
   */
  virtual void trigger_new_election() = 0;
  /**
   * Retrieve this Paxos instance's rank.
   */
  virtual int get_my_rank() const = 0;
  /**
   * Send a PROPOSE message to all our peers. This happens when
   * we have started a new election (which may mean attempting to
   * override a current one).
   *
   * @param e The election epoch of our proposal.
   * @param bl A bufferlist containing data the logic wishes to share
   */
  virtual void propose_to_peers(epoch_t e, bufferlist& bl) = 0;
  /**
   * The election has failed and we aren't sure what the state of the
   * quorum is, so reset the entire system as if from scratch.
   */
  virtual void reset_election() = 0;
  /**
   * Ask the ElectionOwner if we-the-Monitor have ever participated in the
   * quorum (including across process restarts!).
   *
   * @returns true if we have participated, false otherwise
   */
  virtual bool ever_participated() const = 0;
  /**
   * Check if the monitor is the tiebreaker in a stretch cluster.
   *
   * @returns true if the Monitor is the tiebreaker, false otherwise.
   */
  virtual bool is_tiebreaker(int rank) const = 0;
  /**
   * Check if the Monitor is marked down in a stretch cluster.
   *
   * @returns true if the Monitor in a stretch cluster is marked down, false otherwise.
   */
  virtual bool is_stretch_marked_down_mons(int rank) const = 0;
  /**
   * Ask the ElectionOwner for the size of the Paxos set. This includes
   * those monitors which may not be in the current quorum!
   * The value returned by this function can change between elections,
   * but not during them. (In practical terms, it can be updated
   * by making a paxos commit, but not by injecting values while
   * an election is ongoing.)
   */
  virtual unsigned paxos_size() const = 0;
  /**
   * Retrieve a set of ranks which are not allowed to become the leader.
   * Like paxos_size(), This set can change between elections, but not
   * during them.
   */
  virtual const std::set<int>& get_disallowed_leaders() const = 0;
  /**
   * Tell the ElectionOwner we have started a new election.
   *
   * The ElectionOwner is responsible for timing out the election (by invoking
   * end_election_period()) if it takes too long (as defined by the ElectionOwner).
   * This function is the opportunity to do that and to clean up any other external 
   * election state it may be maintaining.
   */
  virtual void _start() = 0;
  /**
   * Tell the ElectionOwner to defer to the identified peer. Tell that peer
   * we have deferred to it.
   *
   * @post  we sent an ack message to @p who
   */
  virtual void _defer_to(int who) = 0;
  /**
   * We have won an election, so have the ElectionOwner message that to
   * our new quorum!
   *
   * @param quorum The ranks of our peers which deferred to us and
   *        must be told of our victory
   */
  virtual void message_victory(const std::set<int>& quorum) = 0;
  /**
   * Query the ElectionOwner about if a given rank is in the
   * currently active quorum.
   * @param rank the Paxos rank whose status we are checking
   * @returns true if the rank is in our current quorum, false otherwise.
   */
  virtual bool is_current_member(int rank) const = 0;
  virtual ~ElectionOwner() {}
};

/**
 * This class maintains local state for running an election
 * between Paxos instances. It receives input requests
 * and calls back out to its ElectionOwner to do persistence
 * and message other entities.
 */

class ElectionLogic {
  ElectionOwner *elector;
  ConnectionTracker *peer_tracker;
  
  CephContext *cct;
  /**
   * Latest epoch we've seen.
   *
   * @remarks if its value is odd, we're electing; if it's even, then we're
   *	      stable.
   */
  epoch_t epoch = 0;
  /**
   * The last rank which won an election we participated in
   */
  int last_election_winner = -1;
  /**
   * Only used in the connectivity handler.
   * The rank we voted for in the last election we voted in.
   */
  int last_voted_for = -1;
  double ignore_propose_margin = 0.0001;
  /**
   * Only used in the connectivity handler.
   * Points at a stable copy of the peer_tracker we use to keep scores
   * throughout an election period.
   */
  std::unique_ptr<ConnectionTracker> stable_peer_tracker;
  std::unique_ptr<ConnectionTracker> leader_peer_tracker;
  /**
   * Indicates who we have acked
   */
  int leader_acked;
  
public:
  enum election_strategy {
			  // Keep in sync with MonMap.h!
    CLASSIC = 1, // the original rank-based one
    DISALLOW = 2, // disallow a set from being leader
    CONNECTIVITY = 3 // includes DISALLOW, extends to prefer stronger connections
  };
  election_strategy strategy;
    
  /**
   * Indicates if we are participating in the quorum.
   *
   * @remarks By default, we are created as participating. We may stop
   *	      participating if something explicitly sets our value
   *	      false, though. If that happens, it will
   *	      have to set participating=true and invoke start() for us to resume
   *	      participating in the quorum.
   */
  bool participating;
  /**
   * Indicates if we are the ones being elected.
   *
   * We always attempt to be the one being elected if we are the ones starting
   * the election. If we are not the ones that started it, we will only attempt
   * to be elected if we think we might have a chance (i.e., the other guy's
   * rank is lower than ours).
   */
  bool electing_me;
  /**
   * Set containing all those that acked our proposal to become the Leader.
   *
   * If we are acked by ElectionOwner::paxos_size() peers, we will declare
   * victory.
   */
  std::set<int> acked_me;

  ElectionLogic(ElectionOwner *e, election_strategy es, ConnectionTracker *t,
		double ipm,
		CephContext *c) : elector(e), peer_tracker(t), cct(c),
				  last_election_winner(-1), last_voted_for(-1),
				  ignore_propose_margin(ipm),
				  stable_peer_tracker(),
				  leader_peer_tracker(),
				  leader_acked(-1),
				  strategy(es),
				  participating(true),
				  electing_me(false) {}
  /**
   * Set the election strategy to use. If this is not consistent across the
   * electing cluster, you're going to have a bad time.
   * Defaults to CLASSIC.
   */
  void set_election_strategy(election_strategy es) {
    strategy = es;
  }
  /**
   * If there are no other peers in this Paxos group, ElectionOwner
   * can simply declare victory and we will make it so.
   *
   * @pre paxos_size() is 1
   * @pre get_my_rank is 0
   */
  void declare_standalone_victory();
  /**
   * Start a new election by proposing ourselves as the new Leader.
   *
   * Basically, send propose messages to all the peers.
   *
   * @pre   participating is true
   * @post  epoch is an odd value
   * @post  electing_me is true
   * @post  We have invoked propose_to_peers() on our ElectionOwner
   * @post  We have invoked _start() on our ElectionOwner
   */
  void start();
  /**
   * ElectionOwner has decided the election has taken too long and expired.
   *
   * This will happen when no one declared victory or started a new election
   * during the allowed time span.
   *
   * When the election expires, we will check if we were the ones who won, and
   * if so we will declare victory. If that is not the case, then we assume
   * that the one we deferred to didn't declare victory quickly enough (in fact,
   * as far as we know, it may even be dead); so, just propose ourselves as the
   * Leader.
   */
  void end_election_period();
  /**
   * Handle a proposal from some other node proposing asking to become
   * the Leader.
   *
   * If the message appears to be old (i.e., its epoch is lower than our epoch),
   * then we may take one of two actions:
   *
   *  @li Ignore it because it's nothing more than an old proposal
   *  @li Start new elections if we verify that it was sent by a monitor from
   *	  outside the quorum; given its old state, it's fair to assume it just
   *	  started, so we should start new elections so it may rejoin. (Some
   *      handlers may choose to ignore even these, if they think it's flapping.)
   *
   * We pass the propose off to a propose_*_handler function based
   * on the election strategy we're using.
   * Only the Connectivity strategy cares about the ConnectionTracker; it should
   * be NULL if other strategies are in use. Otherwise, it will take ownership
   * of the underlying data and delete it as needed.
   *
   * @pre   Message epoch is from the current or a newer epoch
   * @param mepoch The epoch of the proposal
   * @param from The rank proposing itself as leader
   * @param ct Any incoming ConnectionTracker data sent with the message.
   *  Callers are responsible for deleting this -- we will copy it if we want
   *  to keep the data.
   */
  void receive_propose(int from, epoch_t mepoch, const ConnectionTracker *ct);
  /**
   * Handle a message from some other participant Acking us as the Leader.
   *
   * When we receive such a message, one of three thing may be happening:
   *  @li We received a message with a newer epoch, which means we must have
   *	  somehow lost track of what was going on (maybe we rebooted), thus we
   *	  will start a new election
   *  @li We consider ourselves in the run for the Leader (i.e., @p electing_me 
   *	  is true), and we are actually being Acked by someone; thus simply add
   *	  the one acking us to the @p acked_me set. If we do now have acks from
   *	  all the participants, then we can declare victory
   *  @li We already deferred the election to somebody else, so we will just
   *	  ignore this message
   *
   * @pre   Message epoch is from the current or a newer epoch
   * @post  Election is on-going if we deferred to somebody else
   * @post  Election is on-going if we are still waiting for further Acks
   * @post  Election is not on-going if we are victorious
   * @post  Election is not on-going if we must start a new one
   *
   * @param from The rank which acked us
   * @param from_epoch The election epoch the ack belongs to
   */
  void receive_ack(int from, epoch_t from_epoch);
  /**
   * Handle a message from some other participant declaring Victory.
   *
   * We just got a message from someone declaring themselves Victorious, thus
   * the new Leader.
   *
   * However, if the message's epoch happens to be different from our epoch+1,
   * then it means we lost track of something and we must start a new election.
   *
   * If that is not the case, then we will simply update our epoch to the one
   * in the message and invoke start() to reset the quorum.
   *
   * @pre   from_epoch is the current or a newer epoch
   * @post  Election is not on-going
   * @post  Updated @p epoch
   * @post  We are a peon in a new quorum if we lost the election
   *
   * @param from The victory-claiming rank
   * @param from_epoch The election epoch in which they claim victory
   */
  bool receive_victory_claim(int from, epoch_t from_epoch);
  /**
   * Obtain our epoch
   *
   * @returns Our current epoch number
   */
  epoch_t get_epoch() const { return epoch; }
  int get_election_winner() { return last_election_winner; }

private:
  /**
   * Initiate the ElectionLogic class.
   *
   * Basically, we will simply read whatever epoch value we have in our stable
   * storage, or consider it to be 1 if none is read.
   *
   * @post @p epoch is set to 1 or higher.
   */
  void init();
  /**
   * Update our epoch.
   *
   * If we come across a higher epoch, we simply update ours, also making
   * sure we are no longer being elected (even though we could have been,
   * we no longer are since we no longer are on that old epoch).
   *
   * @pre Our epoch is not larger than @p e
   * @post Our epoch equals @p e
   *
   * @param e Epoch to which we will update our epoch
   */
  void bump_epoch(epoch_t e);
  /**
   * If the incoming proposal is newer, bump our own epoch; if
   * it comes from an out-of-quorum peer, trigger a new eleciton.
   * @returns true if you should drop this proposal, false otherwise.
   */
  bool propose_classic_prefix(int from, epoch_t mepoch);
  /**
   * Handle a proposal from another rank using the classic strategy.
   * We will take one of the following actions:
   *
   *  @li Ignore it because we already acked another node with higher rank
   *  @li Ignore it and start a new election because we outrank it
   *  @li Defer to it because it outranks us and the node we previously
   *	  acked, if any
   */
  void propose_classic_handler(int from, epoch_t mepoch);
  /**
   * Handle a proposal from another rank using our disallow strategy.
   * This is the same as the classic strategy except we also disallow
   * certain ranks from becoming the leader.
   */
  void propose_disallow_handler(int from, epoch_t mepoch);
  /**
   * Handle a proposal from another rank using the connectivity strategy.
   * We will choose to defer or not based on the ordered criteria:
   *
   * @li Whether the other monitor (or ourself) is on the disallow list
   * @li Whether the other monitor or ourself has the most connectivity to peers
   * @li Whether the other monitor or ourself has the lower rank
   */
  void propose_connectivity_handler(int from, epoch_t mepoch, const ConnectionTracker *ct);
  /**
   * Helper function for connectivity handler. Combines the disallowed list
   * with ConnectionTracker scores.
   */
  double connectivity_election_score(int rank);
  /**
   * Defer the current election to some other monitor.
   *
   * This means that we will ack some other monitor and drop out from the run
   * to become the Leader. We will only defer an election if the monitor we
   * are deferring to outranks us.
   *
   * @pre   @p who outranks us (i.e., who < our rank)
   * @pre   @p who outranks any other monitor we have deferred to in the past
   * @post  electing_me is false
   * @post  leader_acked equals @p who
   * @post  we triggered ElectionOwner's _defer_to() on @p who
   *
   * @param who Some other monitor's numeric identifier. 
   */
  void defer(int who);
  /**
   * Declare Victory.
   * 
   * We won. Or at least we believe we won, but for all intents and purposes
   * that does not matter. What matters is that we Won.
   *
   * That said, we must now bump our epoch to reflect that the election is over
   * and then we must let everybody in the quorum know we are their brand new
   * Leader.
   *
   * Actually, the quorum will be now defined as the group of monitors that
   * acked us during the election process.
   *
   * @pre   Election is on-going
   * @pre   electing_me is true
   * @post  electing_me is false
   * @post  epoch is bumped up into an even value
   * @post  Election is not on-going
   * @post  We have a quorum, composed of the monitors that acked us
   * @post  We invoked message_victory() on the ElectionOwner
   */
  void declare_victory();
  /**
   * This is just a helper function to validate that the victory claim we
   * get from another rank makes any sense.
   */
  bool victory_makes_sense(int from);
  /**
   * Reset some data members which we only care about while we are in an election
   * or need to be set consistently during stable states.
   */
  void clear_live_election_state();
  void reset_stable_tracker();
  /**
   * Only for the connectivity handler, Bump the epoch
   * when we get a message from a newer one and clear
   * out leader and stable tracker
   * data so that we can switch our allegiance.
   */
  void connectivity_bump_epoch_in_election(epoch_t mepoch);
};

#endif
