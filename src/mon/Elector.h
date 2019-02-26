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


#ifndef CEPH_MON_ELECTOR_H
#define CEPH_MON_ELECTOR_H

#include <map>

#include "include/types.h"
#include "include/Context.h"
#include "mon/MonOpRequest.h"
#include "mon/mon_types.h"

class Monitor;

/**
 * This class is responsible for maintaining the local state when electing
 * a new Leader. We may win or we may lose. If we win, it means we became the
 * Leader; if we lose, it means we are a Peon.
 */
class Elector {
  /**
   * @defgroup Elector_h_class Elector
   * @{
   */
 private:
   /**
   * @defgroup Elector_h_internal_types Internal Types
   * @{
   */
  /**
   * This struct will hold the features from a given peer.
   * Features may both be the cluster's (in the form of a uint64_t), or
   * mon-specific features. Instead of keeping maps to hold them both, or
   * a pair, which would be weird, a struct to keep them seems appropriate.
   */
  struct elector_info_t {
    uint64_t cluster_features = 0;
    mon_feature_t mon_features;
    int mon_release = 0;
    map<string,string> metadata;
  };

  /**
   * @}
   */

  /**
   * The Monitor instance associated with this class.
   */
  Monitor *mon;

  /**
   * Event callback responsible for dealing with an expired election once a
   * timer runs out and fires up.
   */
  Context *expire_event = nullptr;

  /**
   * Resets the expire_event timer, by cancelling any existing one and
   * scheduling a new one.
   *
   * @remarks This function assumes as a default firing value the duration of
   *	      the monitor's lease interval, and adds to it the value specified
   *	      in @e plus
   *
   * @post expire_event is set
   *
   * @param plus The amount of time to be added to the default firing value.
   */
  void reset_timer(double plus=0.0);
  /**
   * Cancel the expire_event timer, if it is defined.
   *
   * @post expire_event is not set
   */
  void cancel_timer();

  /**
   * Latest epoch we've seen.
   *
   * @remarks if its value is odd, we're electing; if it's even, then we're
   *	      stable.
   */
  epoch_t epoch;

  /**
   * Indicates if we are participating in the quorum.
   *
   * @remarks By default, we are created as participating. We may stop
   *	      participating if the Monitor explicitly calls
   *	      Elector::stop_participating though. If that happens, it will
   *	      have to call Elector::start_participating for us to resume
   *	      participating in the quorum.
   */
  bool participating;

  // electing me
  /**
   * @defgroup Elector_h_electing_me_vars We are being elected
   * @{
   */
  /**
   * Indicates if we are the ones being elected.
   *
   * We always attempt to be the one being elected if we are the ones starting
   * the election. If we are not the ones that started it, we will only attempt
   * to be elected if we think we might have a chance (i.e., the other guy's
   * rank is lower than ours).
   */
  bool     electing_me;
  /**
   * Set containing all those that acked our proposal to become the Leader.
   *
   * If we are acked by everyone in the MonMap, we will declare
   * victory.  Also note each peer's feature set.
   */
  map<int, elector_info_t> acked_me;
  /**
   * @}
   */
  /**
   * @defgroup Elector_h_electing_them_vars We are electing another guy
   * @{
   */
  /**
   * Indicates who we have acked
   */
  int	    leader_acked;
  /**
   * @}
   */
 
  /**
   * Update our epoch.
   *
   * If we come across a higher epoch, we simply update ours, also making
   * sure we are no longer being elected (even though we could have been,
   * we no longer are since we no longer are on that old epoch).
   *
   * @pre Our epoch is lower than @p e
   * @post Our epoch equals @p e
   *
   * @param e Epoch to which we will update our epoch
   */
  void bump_epoch(epoch_t e);

  /**
   * Start new elections by proposing ourselves as the new Leader.
   *
   * Basically, send propose messages to all the monitors in the MonMap and
   * then reset the expire_event timer so we can limit the amount of time we 
   * will be going at it.
   *
   * @pre   participating is true
   * @post  epoch is an odd value
   * @post  electing_me is true
   * @post  we sent propose messages to all the monitors in the MonMap
   * @post  we reset the expire_event timer
   */
  void start();
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
   * @post  we sent an ack message to @p who
   * @post  we reset the expire_event timer
   *
   * @param who Some other monitor's numeric identifier. 
   */
  void defer(int who);
  /**
   * The election has taken too long and has expired.
   *
   * This will happen when no one declared victory or started a new election
   * during the time span allowed by the expire_event timer.
   *
   * When the election expires, we will check if we were the ones who won, and
   * if so we will declare victory. If that is not the case, then we assume
   * that the one we deferred to didn't declare victory quickly enough (in fact,
   * as far as we know, we may even be dead); so, just propose ourselves as the
   * Leader.
   */
  void expire();
  /**
   * Declare Victory.
   * 
   * We won. Or at least we believe we won, but for all intentions and purposes
   * that does not matter. What matters is that we Won.
   *
   * That said, we must now bump our epoch to reflect that the election is over
   * and then we must let everybody in the quorum know we are their brand new
   * Leader. And we will also cancel our expire_event timer.
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
   * @post  We sent a message of type OP_VICTORY to each quorum member.
   */
  void victory();

  /**
   * Handle a message from some other node proposing itself to become it
   * the Leader.
   *
   * If the message appears to be old (i.e., its epoch is lower than our epoch),
   * then we may take one of two actions:
   *
   *  @li Ignore it because it's nothing more than an old proposal
   *  @li Start new elections if we verify that it was sent by a monitor from
   *	  outside the quorum; given its old state, it's fair to assume it just
   *	  started, so we should start new elections so it may rejoin
   *
   * If we did not ignore the received message, then we know that this message
   * was sent by some other node proposing itself to become the Leader. So, we
   * will take one of the following actions:
   *
   *  @li Ignore it because we already acked another node with higher rank
   *  @li Ignore it and start a new election because we outrank it
   *  @li Defer to it because it outranks us and the node we previously
   *	  acked, if any
   *
   *
   * @invariant The received message is an operation of type OP_PROPOSE
   *
   * @param m A message sent by another participant in the quorum.
   */
  void handle_propose(MonOpRequestRef op);
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
   * @pre   Election is on-going
   * @post  Election is on-going if we deferred to somebody else
   * @post  Election is on-going if we are still waiting for further Acks
   * @post  Election is not on-going if we are victorious
   * @post  Election is not on-going if we must start a new one
   *
   * @param m A message with an operation type of OP_ACK
   */
  void handle_ack(MonOpRequestRef op);
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
   * in the message, cancel our @p expire_event timer and inform our Monitor
   * that we lost the election and provide it with the new quorum.
   *
   * @pre   Election in on-going
   * @post  Election is not on-going
   * @post  Updated @p epoch
   * @post  We have a new quorum if we lost the election
   *
   * @param m A message with an operation type of OP_VICTORY
   */
  void handle_victory(MonOpRequestRef op);
  /**
   * Send a nak to a peer who's out of date, containing information about why.
   *
   * If we get a message from a peer who can't support the required quorum
   * features, we have to ignore them. This function will at least send
   * them a message about *why* they're being ignored -- if they're new
   * enough to support such a message.
   *
   * @param m A message from a monitor not supporting required features. We
   * take ownership of the reference.
   */
  void nak_old_peer(MonOpRequestRef op);
  /**
   * Handle a message from some other participant declaring
   * we cannot join the quorum.
   *
   * Apparently the quorum requires some feature that we do not implement. Shut
   * down gracefully.
   *
   * @pre Election is on-going.
   * @post We've shut down.
   *
   * @param m A message with an operation type of OP_NAK
   */
  void handle_nak(MonOpRequestRef op);
  
 public:
  /**
   * Create an Elector class
   *
   * @param m A Monitor instance
   */
  explicit Elector(Monitor *m) : mon(m),
			epoch(0),
			participating(true),
			electing_me(false),
			leader_acked(-1) { }

  /**
   * Initiate the Elector class.
   *
   * Basically, we will simply read whatever epoch value we have in our stable
   * storage, or consider it to be 1 if none is read.
   *
   * @post @p epoch is set to 1 or higher.
   */
  void init();
  /**
   * Inform this class it is supposed to shutdown.
   *
   * We will simply cancel the @p expire_event if any exists.
   *
   * @post @p expire_event is cancelled 
   */
  void shutdown();

  /**
   * Obtain our epoch
   *
   * @returns Our current epoch number
   */
  epoch_t get_epoch() { return epoch; }

  /**
   * advance_epoch
   *
   * increase election epoch by 1
   */
  void advance_epoch() {
    bump_epoch(epoch + 1);
  }

  /**
   * Handle received messages.
   *
   * We will ignore all messages that are not of type @p MSG_MON_ELECTION
   * (i.e., messages whose interface is not of type @p MMonElection). All of
   * those that are will then be dispatched to their operation-specific
   * functions.
   *
   * @param m A received message
   */
  void dispatch(MonOpRequestRef op);

  /**
   * Call an election.
   *
   * This function simply calls Elector::start.
   */
  void call_election() {
    start();
  }

  /**
   * Stop participating in subsequent Elections.
   *
   * @post @p participating is false
   */
  void stop_participating() { participating = false; }
  /**
   * Start participating in Elections.
   *
   * If we are already participating (i.e., @p participating is true), then
   * calling this function is moot.
   *
   * However, if we are not participating (i.e., @p participating is false),
   * then we will start participating by setting @p participating to true and
   * we will call for an Election.
   *
   * @post  @p participating is true
   */
  void start_participating();

  /**
   * @}
   */
};

#endif
