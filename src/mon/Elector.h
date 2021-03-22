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
#include "mon/ElectionLogic.h"
#include "mon/ConnectionTracker.h"

class Monitor;


/**
 * This class is responsible for handling messages and maintaining
 * an ElectionLogic which holds the local state when electing
 * a new Leader. We may win or we may lose. If we win, it means we became the
 * Leader; if we lose, it means we are a Peon.
 */
class Elector : public ElectionOwner, RankProvider {
  /**
   * @defgroup Elector_h_class Elector
   * @{
   */
  ElectionLogic logic;
  // connectivity validation and scoring
  ConnectionTracker peer_tracker;
  map<int, utime_t> peer_acked_ping; // rank -> last ping stamp they acked
  map<int, utime_t> peer_sent_ping; // rank -> last ping stamp we sent
  set<int> live_pinging; // ranks which we are currently pinging
  set<int> dead_pinging; // ranks which didn't answer (degrading scores)
  double ping_timeout; // the timeout after which we consider a ping to be dead
  int PING_DIVISOR = 2;  // we time out pings

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

  // electing me
  /**
   * @defgroup Elector_h_electing_me_vars We are being elected
   * @{
   */
  /**
   * Map containing info of all those that acked our proposal to become the Leader.
   * Note each peer's info.
   */
  map<int, elector_info_t> peer_info;
  /**
   * @}
   */
 
  /**
   * Handle a message from some other node proposing itself to become it
   * the Leader.
   *
   * We validate that the sending Monitor is allowed to participate based on
   * its supported features, then pass the request to our ElectionLogic.
   *
   * @invariant The received message is an operation of type OP_PROPOSE
   *
   * @pre   Message epoch is from the current or a newer epoch
   * 
   * @param m A message sent by another participant in the quorum.
   */
  void handle_propose(MonOpRequestRef op);
  /**
   * Handle a message from some other participant Acking us as the Leader.
   *
   * We validate that the sending Monitor is allowed to participate based on
   * its supported features, add it to peer_info, and pass the ack to our
   * ElectionLogic.
   *
   * @pre   Message epoch is from the current or a newer epoch
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
   * We pass the Victory to our ElectionLogic, and if it confirms the
   * victory we lose the election and start following this Leader. Otherwise,
   * drop the message.
   *
   * @pre   Message epoch is from the current or a newer epoch
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
  /**
   * Send a ping to the specified peer.
   * @n optional time that we will use instead of calling ceph_clock_now()
   */
  void send_peer_ping(int peer, const utime_t *n=NULL);
  /**
   * Check the state of pinging the specified peer. This is our
   * "tick" for heartbeating; scheduled by itself and begin_peer_ping().
   */
  void ping_check(int peer);
  /**
   * Move the peer out of live_pinging into dead_pinging set
   * and schedule dead_ping()ing on it.
   */
  void begin_dead_ping(int peer);
  /**
   * Checks that the peer is still marked for dead pinging,
   * and then marks it as dead for the appropriate interval.
   */
  void dead_ping(int peer);
  /**
   * Handle a ping from another monitor and assimilate the data it contains.
   */
  void handle_ping(MonOpRequestRef op);
  /**
   * Update our view of everybody else's connectivity based on the provided
   * tracker bufferlist
   */
  void assimilate_connection_reports(const bufferlist& bl);
  
 public:
  /**
   * @defgroup Elector_h_ElectionOwner Functions from the ElectionOwner interface
   * @{
   */
  /* Commit the given epoch to our MonStore.
   * We also take the opportunity to persist our peer_tracker.
   */
  void persist_epoch(epoch_t e);
  /* Read the epoch out of our MonStore */
  epoch_t read_persisted_epoch() const;
  /* Write a nonsense key "election_writeable_test" to our MonStore */
  void validate_store();
  /* Reset my tracking. Currently, just call Monitor::join_election() */
  void notify_bump_epoch();
  /* Call a new election: Invoke Monitor::start_election() */
  void trigger_new_election();
  /* Retrieve rank from the Monitor */
  int get_my_rank() const;
  /* Send MMonElection OP_PROPOSE to every monitor in the map. */
  void propose_to_peers(epoch_t e, bufferlist &bl);
  /* bootstrap() the Monitor */
  void reset_election();
  /* Retrieve the Monitor::has_ever_joined member */
  bool ever_participated() const;
  /* Retrieve monmap->size() */
  unsigned paxos_size() const;
  /* Right now we don't disallow anybody */
  set<int> disallowed_leaders;
  const set<int>& get_disallowed_leaders() const { return disallowed_leaders; }
  /**
   * Reset the expire_event timer so we can limit the amount of time we 
   * will be electing. Clean up our peer_info.
   *
   * @post  we reset the expire_event timer
   */
  void _start();
  /**
   * Send an MMonElection message deferring to the identified monitor. We
   * also increase the election timeout so the monitor we defer to
   * has some time to gather deferrals and actually win. (FIXME: necessary to protocol?)
   *
   * @post  we sent an ack message to @p who
   * @post  we reset the expire_event timer
   *
   * @param who Some other monitor's numeric identifier. 
   */
  void _defer_to(int who);
  /**
   * Our ElectionLogic told us we won an election! Identify the quorum
   * features, tell our new peons we've won, and invoke Monitor::win_election().
   */
  void message_victory(const std::set<int>& quorum);
  /* Check if rank is in mon->quorum */
  bool is_current_member(int rank) const;
  /*
   * @}
   */
  /**
   * Persist our peer_tracker to disk.
   */
  void persist_connectivity_scores();

  Elector *elector;
  
  /**
   * Create an Elector class
   *
   * @param m A Monitor instance
   * @param strategy The election strategy to use, defined in MonMap/ElectionLogic
   */
  explicit Elector(Monitor *m, int strategy);
  virtual ~Elector() {}

  /**
   * Inform this class it is supposed to shutdown.
   *
   * We will simply cancel the @p expire_event if any exists.
   *
   * @post @p expire_event is cancelled 
   */
  void shutdown();

  /**
   * Obtain our epoch from ElectionLogic.
   *
   * @returns Our current epoch number
   */
  epoch_t get_epoch() { return logic.get_epoch(); }

  /**
   * If the Monitor knows there are no Paxos peers (so
   * we are rank 0 and there are no others) we can declare victory.
   */
  void declare_standalone_victory() {
    logic.declare_standalone_victory();
  }
  /**
   * Tell the Elector to start pinging a given peer.
   * Do this when you discover a peer and it has a rank assigned.
   * We do it ourselves on receipt of pings and when receiving other messages.
   */
  void begin_peer_ping(int peer);
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
   * This function simply calls ElectionLogic::start.
   */
  void call_election() {
    logic.start();
  }

  /**
   * Stop participating in subsequent Elections.
   *
   * @post @p participating is false
   */
  void stop_participating() { logic.participating = false; }
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
   * Forget everything about our peers. :(
   */
  void notify_clear_peer_state();
  /**
   * Notify that our local rank has changed
   * and we may need to update internal data structures.
   */
  void notify_rank_changed(int new_rank);
  /**
   * A peer has been removed so we should clean up state related to it.
   * This is safe to call even if we haven't joined or are currently
   * in a quorum.
   */
  void notify_rank_removed(int rank_removed);
  void notify_strategy_maybe_changed(int strategy);
  /**
   * Set the disallowed leaders.
   *
   * If you call this and the new disallowed set
   * contains your current leader, you are
   * responsible for calling an election!
   *
   * @returns false if the set is unchanged,
   *   true if the set changed
   */
  bool set_disallowed_leaders(const set<int>& dl) {
    if (dl == disallowed_leaders) return false;
    disallowed_leaders = dl;
    return true;
  }
  void dump_connection_scores(Formatter *f) {
    f->open_object_section("connection scores");
    peer_tracker.dump(f);
    f->close_section();
  }
  /**
   * @}
   */
};

#endif
