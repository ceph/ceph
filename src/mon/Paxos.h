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

/*
time---->

cccccccccccccccccca????????????????????????????????????????
cccccccccccccccccca????????????????????????????????????????
cccccccccccccccccca???????????????????????????????????????? leader
cccccccccccccccccc????????????????????????????????????????? 
ccccc?????????????????????????????????????????????????????? 

last_committed

pn_from
pn

a 12v 
b 12v
c 14v
d
e 12v
*/

/**
 * Paxos storage layout and behavior
 *
 * Currently, we use a key/value store to hold all the Paxos-related data, but
 * it can logically be depicted as this:
 *
 *  paxos:
 *    first_committed -> 1
 *     last_committed -> 4
 *		    1 -> value_1
 *		    2 -> value_2
 *		    3 -> value_3
 *		    4 -> value_4
 *
 * Since we are relying on a k/v store supporting atomic transactions, we can
 * guarantee that if 'last_committed' has a value of '4', then we have up to
 * version 4 on the store, and no more than that; the same applies to
 * 'first_committed', which holding '1' will strictly meaning that our lowest
 * version is 1.
 *
 * Each version's value (value_1, value_2, ..., value_n) is a blob of data,
 * incomprehensible to the Paxos. These values are proposed to the Paxos on
 * propose_new_value() and each one is a transaction encoded in a bufferlist.
 *
 * The Paxos will write the value to disk, associating it with its version,
 * but will take a step further: the value shall be decoded, and the operations
 * on that transaction shall be applied during the same transaction that will
 * write the value's encoded bufferlist to disk. This behavior ensures that
 * whatever is being proposed will only be available on the store when it is
 * applied by Paxos, which will then be aware of such new values, guaranteeing
 * the store state is always consistent without requiring shady workarounds.
 *
 * So, let's say that FooMonitor proposes the following transaction, neatly
 * encoded on a bufferlist of course:
 *
 *  Tx_Foo
 *    put(foo, last_committed, 3)
 *    put(foo, 3, foo_value_3)
 *    erase(foo, 2)
 *    erase(foo, 1)
 *    put(foo, first_committed, 3)
 *
 * And knowing that the Paxos is proposed Tx_Foo as a bufferlist, once it is
 * ready to commit, and assuming we are now committing version 5 of the Paxos,
 * we will do something along the lines of:
 *
 *  Tx proposed_tx;
 *  proposed_tx.decode(Tx_foo_bufferlist);
 *
 *  Tx our_tx;
 *  our_tx.put(paxos, last_committed, 5);
 *  our_tx.put(paxos, 5, Tx_foo_bufferlist);
 *  our_tx.append(proposed_tx);
 *
 *  store_apply(our_tx);
 *
 * And the store should look like this after we apply 'our_tx':
 *
 *  paxos:
 *    first_committed -> 1
 *     last_committed -> 5
 *		    1 -> value_1
 *		    2 -> value_2
 *		    3 -> value_3
 *		    4 -> value_4
 *		    5 -> Tx_foo_bufferlist
 *  foo:
 *    first_committed -> 3
 *     last_committed -> 3
 *		    3 -> foo_value_3
 *
 */

#ifndef CEPH_MON_PAXOS_H
#define CEPH_MON_PAXOS_H

#include "include/types.h"
#include "mon_types.h"
#include "include/buffer.h"
#include "messages/PaxosServiceMessage.h"
#include "msg/msg_types.h"

#include "include/Context.h"

#include "common/Timer.h"
#include <errno.h>

#include "MonitorDBStore.h"

class Monitor;
class MMonPaxos;
class Paxos;

// i am one state machine.
/**
 * This libary is based on the Paxos algorithm, but varies in a few key ways:
 *  1- Only a single new value is generated at a time, simplifying the recovery logic.
 *  2- Nodes track "committed" values, and share them generously (and trustingly)
 *  3- A 'leasing' mechism is built-in, allowing nodes to determine when it is 
 *     safe to "read" their copy of the last committed value.
 *
 * This provides a simple replication substrate that services can be built on top of.
 * See PaxosService.h
 */
class Paxos {
  /**
   * @defgroup Paxos_h_class Paxos
   * @{
   */
  /**
   * The Monitor to which this Paxos class is associated with.
   */
  Monitor *mon;

  // my state machine info
  const string paxos_name;

  friend class Monitor;
  friend class PaxosService;

  list<std::string> extra_state_dirs;

  // LEADER+PEON

  // -- generic state --
public:
  /**
   * @defgroup Paxos_h_states States on which the leader/peon may be.
   * @{
   */
  /**
   * Leader/Peon is in Paxos' Recovery state
   */
  const static int STATE_RECOVERING = 0x01;
  /**
   * Leader/Peon is idle, and the Peon may or may not have a valid lease.
   */
  const static int STATE_ACTIVE     = 0x02;
  /**
   * Leader/Peon is updating to a new value.
   */
  const static int STATE_UPDATING   = 0x04;
  /**
   * Leader is about to propose a new value, but hasn't gotten to do it yet.
   */
  const static int STATE_PREPARING  = 0x08;

  const static int STATE_LOCKED     = 0x10;

  /**
   * Obtain state name from constant value.
   *
   * @note This function will raise a fatal error if @p s is not
   *	   a valid state value.
   *
   * @param s State value.
   * @return The state's name.
   */
  static const string get_statename(int s) {
    stringstream ss;
    if (s & STATE_RECOVERING) {
      ss << "recovering";
      assert(!(s & ~(STATE_RECOVERING|STATE_LOCKED)));
    } else if (s & STATE_ACTIVE) {
      ss << "active";
      assert(s == STATE_ACTIVE);
    } else if (s & STATE_UPDATING) {
      ss << "updating";
      assert(!(s & ~(STATE_UPDATING|STATE_LOCKED)));
    } else if (s & STATE_PREPARING) {
      ss << "preparing update";
      assert(!(s & ~(STATE_PREPARING|STATE_LOCKED)));
    } else {
      assert(0 == "We shouldn't have gotten here!");
    }

    if (s & STATE_LOCKED)
      ss << " (locked)";
    return ss.str();
  }

private:
  /**
   * The state we are in.
   */
  int state;
  /**
   * @}
   */

public:
  /**
   * Check if we are recovering.
   *
   * @return 'true' if we are on the Recovering state; 'false' otherwise.
   */
  bool is_recovering() const { return (state & STATE_RECOVERING); }
  /**
   * Check if we are active.
   *
   * @return 'true' if we are on the Active state; 'false' otherwise.
   */
  bool is_active() const { return state == STATE_ACTIVE; }
  /**
   * Check if we are updating.
   *
   * @return 'true' if we are on the Updating state; 'false' otherwise.
   */
  bool is_updating() const { return (state & STATE_UPDATING); }

  bool is_preparing() const { return (state & STATE_PREPARING); }
  bool is_locked() const { return (state & STATE_LOCKED); }

private:
  /**
   * @defgroup Paxos_h_recovery_vars Common recovery-related member variables
   * @note These variables are common to both the Leader and the Peons.
   * @{
   */
  /**
   *
   */
  version_t first_committed;
  /**
   * Last Proposal Number
   *
   * @todo Expand description
   */
  version_t last_pn;
  /**
   * Last committed value's version.
   *
   * On both the Leader and the Peons, this is the last value's version that 
   * was accepted by a given quorum and thus committed, that this instance 
   * knows about.
   *
   * @note It may not be the last committed value's version throughout the
   *	   system. If we are a Peon, we may have not been part of the quorum
   *	   that accepted the value, and for this very same reason we may still
   *	   be a (couple of) version(s) behind, until we learn about the most
   *	   recent version. This should only happen if we are not active (i.e.,
   *	   part of the quorum), which should not happen if we are up, running
   *	   and able to communicate with others -- thus able to be part of the
   *	   monmap and trigger new elections.
   */
  version_t last_committed;
  /**
   * Last committed value's time.
   *
   * When the commit happened.
   */
  utime_t last_commit_time;
  /**
   * The last Proposal Number we have accepted.
   *
   * On the Leader, it will be the Proposal Number picked by the Leader 
   * itself. On the Peon, however, it will be the proposal sent by the Leader
   * and it will only be updated iif its value is higher than the one
   * already known by the Peon.
   */
  version_t accepted_pn;
  /**
   * @todo This has something to do with the last_committed version. Not sure
   *	   about what it entails, tbh.
   */
  version_t accepted_pn_from;
  /**
   * Map holding the first committed version by each quorum member.
   *
   * The versions kept in this map are updated during the collect phase.
   * When the Leader starts the collect phase, each Peon will reply with its
   * first committed version, which will then be kept in this map.
   */
  map<int,version_t> peer_first_committed;
  /**
   * Map holding the last committed version by each quorum member.
   *
   * The versions kept in this map are updated during the collect phase.
   * When the Leader starts the collect phase, each Peon will reply with its
   * last committed version, which will then be kept in this map.
   */
  map<int,version_t> peer_last_committed;
  /**
   * @}
   */

  // active (phase 2)
  /**
   * @defgroup Paxos_h_active_vars Common active-related member variables
   * @{
   */
  /**
   * When does our read lease expires.
   *
   * Instead of performing a full commit each time a read is requested, we
   * keep leases. Each lease will have an expiration date, which may or may
   * not be extended. This member variable will keep when is the lease 
   * expiring.
   */
  utime_t lease_expire;
  /**
   * List of callbacks waiting for our state to change into STATE_ACTIVE.
   */
  list<Context*> waiting_for_active;
  /**
   * List of callbacks waiting for the chance to read a version from us.
   *
   * Each entry on the list may result from an attempt to read a version that
   * wasn't available at the time, or an attempt made during a period during
   * which we could not satisfy the read request. The first case happens if
   * the requested version is greater than our last committed version. The
   * second scenario may happen if we are recovering, or if we don't have a
   * valid lease.
   *
   * The list will be woken up once we change to STATE_ACTIVE with an extended
   * lease -- which can be achieved if we have everyone on the quorum on board
   * with the latest proposal, or if we don't really care about the remaining
   * uncommitted values --, or if we're on a quorum of one.
   */
  list<Context*> waiting_for_readable;
  /**
   * @}
   */

  // -- leader --
  // recovery (paxos phase 1)
  /**
   * @defgroup Paxos_h_leader_recovery Leader-specific Recovery-related vars
   * @{
   */
  /**
   * Number of replies to the collect phase we've received so far.
   *
   * This variable is reset to 1 each time we start a collect phase; it is
   * incremented each time we receive a reply to the collect message, and
   * is used to determine whether or not we have received replies from the
   * whole quorum.
   */
  unsigned   num_last;
  /**
   * Uncommitted value's version.
   *
   * If we have, or end up knowing about, an uncommitted value, then its
   * version will be kept in this variable.
   *
   * @note If this version equals @p last_committed+1 when we reach the final
   *	   steps of recovery, then the algorithm will assume this is a value
   *	   the Leader does not know about, and trustingly the Leader will 
   *	   propose this version's value.
   */
  version_t  uncommitted_v;
  /**
   * Uncommitted value's Proposal Number.
   *
   * We use this variable to assess if the Leader should take into consideration
   * an uncommitted value sent by a Peon. Given that the Peon will send back to
   * the Leader the last Proposal Number he accepted, the Leader will be able
   * to infer if this value is more recent than the one the Leader has, thus
   * more relevant.
   */
  version_t  uncommitted_pn;
  /**
   * Uncommitted Value.
   *
   * If the system fails in-between the accept replies from the Peons and the
   * instruction to commit from the Leader, then we may end up with accepted
   * but yet-uncommitted values. During the Leader's recovery, he will attempt
   * to bring the whole system to the latest state, and that means committing
   * past accepted but uncommitted values.
   *
   * This variable will hold an uncommitted value, which may originate either
   * on the Leader, or learnt by the Leader from a Peon during the collect
   * phase.
   */
  bufferlist uncommitted_value;
  /**
   * Used to specify when an on-going collect phase times out.
   */
  Context    *collect_timeout_event;
  /**
   * @}
   */

  // active
  /**
   * @defgroup Paxos_h_leader_active Leader-specific Active-related vars
   * @{
   */
  /**
   * Set of participants (Leader & Peons) that have acked a lease extension.
   *
   * Each Peon that acknowledges a lease extension will have its place in this
   * set, which will be used to account for all the acks from all the quorum
   * members, guaranteeing that we trigger new elections if some don't ack in
   * the expected timeframe.
   */
  set<int>   acked_lease;
  /**
   * Callback responsible for extending the lease periodically.
   */
  Context    *lease_renew_event;
  /**
   * Callback to trigger new elections once the time for acks is out.
   */
  Context    *lease_ack_timeout_event;
  /**
   * @}
   */
  /**
   * @defgroup Paxos_h_peon_active Peon-specific Active-related vars
   * @{
   */
  /**
   * Callback to trigger new elections when the Peon's lease times out.
   *
   * If the Peon's lease is extended, this callback will be reset (i.e.,
   * we cancel the event and reschedule a new one with starting from the
   * beginning).
   */
  Context    *lease_timeout_event;
  /**
   * @}
   */

  // updating (paxos phase 2)
  /**
   * @defgroup Paxos_h_leader_updating Leader-specific Updating-related vars
   * @{
   */
  /**
   * New Value being proposed to the Peons.
   *
   * This bufferlist holds the value the Leader is proposing to the Peons, and
   * that will be committed if the Peons do accept the proposal.
   */
  bufferlist new_value;
  /**
   * Set of participants (Leader & Peons) that accepted the new proposed value.
   *
   * This set is used to keep track of those who have accepted the proposed
   * value, so the leader may know when to issue a commit (when a majority of
   * participants has accepted the proposal), and when to extend the lease
   * (when all the quorum members have accepted the proposal).
   */
  set<int>   accepted;
  /**
   * Callback to trigger a new election if the proposal is not accepted by the
   * full quorum within a given timeframe.
   *
   * If the full quorum does not accept the proposal, then it means that the
   * Leader may no longer be recognized as the leader, or that the quorum has
   * changed, and the value may have not reached all the participants. Thus,
   * the leader must call new elections, and go through a recovery phase in
   * order to propagate the new value throughout the system.
   *
   * This does not mean that we won't commit. We will commit as soon as we
   * have a majority of acceptances. But if we do not have full acceptance
   * from the quorum, then we cannot extend the lease, as some participants
   * may not have the latest committed value.
   */
  Context    *accept_timeout_event;

  /**
   * List of callbacks waiting for it to be possible to write again.
   *
   * @remarks It is not possible to write if we are not the Leader, or we are
   *	      not on the active state, or if the lease has expired.
   */
  list<Context*> waiting_for_writeable;
  /**
   * List of callbacks waiting for a commit to finish.
   *
   * @remarks This may be used to a) wait for an on-going commit to finish
   *	      before we proceed with, say, a new proposal; or b) wait for the
   *	      next commit to be finished so we are sure that our value was
   *	      fully committed.
   */
  list<Context*> waiting_for_commit;
  /**
   *
   */
  list<Context*> proposals;
  /**
   * @}
   */

  /**
   * @defgroup Paxos_h_sync_warns Synchronization warnings
   * @todo Describe these variables
   * @{
   */
  utime_t last_clock_drift_warn;
  int clock_drift_warned;
  /**
   * @}
   */

  /**
   * Should be true if we have proposed to trim, or are in the middle of
   * trimming; false otherwise.
   */
  bool going_to_trim;
  /**
   * If we have disabled trimming our state, this variable should have a
   * value greater than zero, corresponding to the version we had at the time
   * we disabled the trim.
   */
  version_t trim_disabled_version;

  /**
   * @defgroup Paxos_h_callbacks Callback classes.
   * @{
   */
  /**
   * Callback class responsible for handling a Collect Timeout.
   */
  class C_CollectTimeout : public Context {
    Paxos *paxos;
  public:
    C_CollectTimeout(Paxos *p) : paxos(p) {}
    void finish(int r) {
      if (r == -ECANCELED)
	return;
      paxos->collect_timeout();
    }
  };

  /**
   * Callback class responsible for handling an Accept Timeout.
   */
  class C_AcceptTimeout : public Context {
    Paxos *paxos;
  public:
    C_AcceptTimeout(Paxos *p) : paxos(p) {}
    void finish(int r) {
      if (r == -ECANCELED)
	return;
      paxos->accept_timeout();
    }
  };

  /**
   * Callback class responsible for handling a Lease Ack Timeout.
   */
  class C_LeaseAckTimeout : public Context {
    Paxos *paxos;
  public:
    C_LeaseAckTimeout(Paxos *p) : paxos(p) {}
    void finish(int r) {
      if (r == -ECANCELED)
	return;
      paxos->lease_ack_timeout();
    }
  };

  /**
   * Callback class responsible for handling a Lease Timeout.
   */
  class C_LeaseTimeout : public Context {
    Paxos *paxos;
  public:
    C_LeaseTimeout(Paxos *p) : paxos(p) {}
    void finish(int r) {
      if (r == -ECANCELED)
	return;
      paxos->lease_timeout();
    }
  };

  /**
   * Callback class responsible for handling a Lease Renew Timeout.
   */
  class C_LeaseRenew : public Context {
    Paxos *paxos;
  public:
    C_LeaseRenew(Paxos *p) : paxos(p) {}
    void finish(int r) {
      if (r == -ECANCELED)
	return;
      paxos->lease_renew_timeout();
    }
  };

  class C_Trimmed : public Context {
    Paxos *paxos;
  public:
    C_Trimmed(Paxos *p) : paxos(p) { }
    void finish(int r) {
      paxos->going_to_trim = false;
    }
  };
  /**
   *
   */
public:
  class C_Proposal : public Context {
    Context *proposer_context;
  public:
    bufferlist bl;
    // for debug purposes. Will go away. Soon.
    bool proposed;
    utime_t proposal_time;

    C_Proposal(Context *c, bufferlist& proposal_bl) :
	proposer_context(c),
	bl(proposal_bl),
        proposed(false),
	proposal_time(ceph_clock_now(NULL))
      { }

    void finish(int r) {
      if (proposer_context) {
	proposer_context->complete(r);
	proposer_context = NULL;
      }
    }
  };
  /**
   * @}
   */
private:
  /**
   * @defgroup Paxos_h_election_triggered Steps triggered by an election.
   *
   * @note All these functions play a significant role in the Recovery Phase,
   *	   which is triggered right after an election once someone becomes
   *	   the Leader.
   * @{
   */
  /**
   * Create a new Proposal Number and propose it to the Peons.
   *
   * This function starts the Recovery Phase, which can be directly mapped
   * onto the original Paxos' Prepare phase. Basically, we'll generate a
   * Proposal Number, taking @p oldpn into consideration, and we will send
   * it to a quorum, along with our first and last committed versions. By
   * sending these informations in a message to the quorum, we expect to
   * obtain acceptances from a majority, allowing us to commit, or be
   * informed of a higher Proposal Number known by one or more of the Peons
   * in the quorum.
   *
   * @pre We are the Leader.
   * @post Recovery Phase initiated by sending messages to the quorum.
   *
   * @param oldpn A proposal number taken as the highest known so far, that
   *		  should be taken into consideration when generating a new 
   *		  Proposal Number for the Recovery Phase.
   */
  void collect(version_t oldpn);
  /**
   * Handle the reception of a collect message from the Leader and reply
   * accordingly.
   *
   * Once a Peon receives a collect message from the Leader it will reply
   * with its first and last committed versions, as well as informations so
   * the Leader may know if his Proposal Number was, or was not, accepted by
   * the Peon. The Peon will accept the Leader's Proposal Number iif it is
   * higher than the Peon's currently accepted Proposal Number. The Peon may
   * also inform the Leader of accepted but uncommitted values.
   *
   * @invariant The message is an operation of type OP_COLLECT.
   * @pre We are a Peon.
   * @post Replied to the Leader, accepting or not accepting his PN.
   *
   * @param collect The collect message sent by the Leader to the Peon.
   */
  void handle_collect(MMonPaxos *collect);
  /**
   * Handle a response from a Peon to the Leader's collect phase.
   *
   * The received message will state the Peon's last committed version, as 
   * well as its last proposal number. This will lead to one of the following
   * scenarios: if the replied Proposal Number is equal to the one we proposed,
   * then the Peon has accepted our proposal, and if all the Peons do accept
   * our Proposal Number, then we are allowed to proceed with the commit;
   * however, if a Peon replies with a higher Proposal Number, we assume he
   * knows something we don't and the Leader will have to abort the current
   * proposal in order to retry with the Proposal Number specified by the Peon.
   * It may also occur that the Peon replied with a lower Proposal Number, in
   * which case we assume it is a reply to an an older value and we'll simply
   * drop it.
   * This function will also check if the Peon replied with an accepted but
   * yet uncommitted value. In this case, if its version is higher than our
   * last committed value by one, we assume that the Peon knows a value from a
   * previous proposal that has never been committed, and we should try to
   * commit that value by proposing it next. On the other hand, if that is
   * not the case, we'll assume it is an old, uncommitted value, we do not
   * care about and we'll consider the system active by extending the leases.
   *
   * @invariant The message is an operation of type OP_LAST.
   * @pre We are the Leader.
   * @post We initiate a commit, or we retry with a higher Proposal Number, 
   *	   or we drop the message.
   * @post We move from STATE_RECOVERING to STATE_ACTIVE.
   *
   * @param last The message sent by the Peon to the Leader.
   */
  void handle_last(MMonPaxos *last);
  /**
   * The Recovery Phase timed out, meaning that a significant part of the
   * quorum does not believe we are the Leader, and we thus should trigger new
   * elections.
   *
   * @pre We believe to be the Leader.
   * @post Trigger new elections.
   */
  void collect_timeout();
  /**
   * @}
   */

  /**
   * @defgroup Paxos_h_updating_funcs Functions used during the Updating State
   * 
   * These functions may easily be mapped to the original Paxos Algorithm's 
   * phases. 
   *
   * Taking into account the algorithm can be divided in 4 phases (Prepare,
   * Promise, Accept Request and Accepted), we can easily map Paxos::begin to
   * both the Prepare and Accept Request phases; the Paxos::handle_begin to
   * the Promise phase; and the Paxos::handle_accept to the Accepted phase.
   * @{
   */
  /**
   * Start a new proposal with the intent of committing @p value.
   *
   * If we are alone on the system (i.e., a quorum of one), then we will
   * simply commit the value, but if we are not alone, then we need to propose
   * the value to the quorum.
   *
   * @pre We are the Leader
   * @pre We are on STATE_ACTIVE
   * @post We commit, iif we are alone, or we send a message to each quorum 
   *	   member
   * @post We are on STATE_ACTIVE, iif we are alone, or on 
   *	   STATE_UPDATING otherwise
   *
   * @param value The value being proposed to the quorum
   */
  void begin(bufferlist& value);
  /**
   * Accept or decline (by ignoring) a proposal from the Leader.
   *
   * We will decline the proposal (by ignoring it) if we have promised to
   * accept a higher numbered proposal. If that is not the case, we will
   * accept it and accordingly reply to the Leader.
   *
   * @pre We are a Peon
   * @pre We are on STATE_ACTIVE
   * @post We are on STATE_UPDATING iif we accept the Leader's proposal
   * @post We send a reply message to the Leader iif we accept his proposal
   *
   * @invariant The received message is an operation of type OP_BEGIN
   *
   * @param begin The message sent by the Leader to the Peon during the
   *		  Paxos::begin function
   *
   */
  void handle_begin(MMonPaxos *begin);
  /**
   * Handle an Accept message sent by a Peon.
   *
   * In order to commit, the Leader has to receive accepts from a majority of
   * the quorum. If that does happen, then the Leader may proceed with the
   * commit. However, the Leader needs the accepts from all the quorum members
   * in order to extend the lease and move on to STATE_ACTIVE.
   *
   * This function handles these two situations, accounting for the amount of
   * received accepts.
   *
   * @pre We are the Leader
   * @pre We are on STATE_UPDATING
   * @post We are on STATE_ACTIVE iif we received accepts from the full quorum
   * @post We extended the lease iif we moved on to STATE_ACTIVE
   * @post We are on STATE_UPDATING iif we didn't received accepts from the
   *	   full quorum
   * @post We have committed iif we received accepts from a majority
   *
   * @invariant The received message is an operation of type OP_ACCEPT
   *
   * @param accept The message sent by the Peons to the Leader during the
   *		   Paxos::handle_begin function
   */
  void handle_accept(MMonPaxos *accept);
  /**
   * Trigger a fresh election.
   *
   * During Paxos::begin we set a Callback of type Paxos::C_AcceptTimeout in
   * order to limit the amount of time we spend waiting for Accept replies.
   * This callback will call Paxos::accept_timeout when it is fired.
   *
   * This is essential to the algorithm because there may be the chance that
   * we are no longer the Leader (i.e., others don't believe in us) and we
   * are getting ignored, or we dropped out of the quorum and haven't realised
   * it. So, our only option is to trigger fresh elections.
   *
   * @pre We are the Leader
   * @pre We are on STATE_UPDATING
   * @post Triggered fresh elections
   */
  void accept_timeout();
  /**
   * @}
   */

  /**
   * Commit a value throughout the system.
   *
   * The Leader will cancel the current lease (as it was for the old value),
   * and will store the committed value locally. It will then instruct every
   * quorum member to do so as well.
   *
   * @pre We are the Leader
   * @pre We are on STATE_UPDATING
   * @pre A majority of quorum members accepted our proposal
   * @post Value locally stored
   * @post Quorum members instructed to commit the new value.
   */
  void commit();
  /**
   * Commit the new value to stable storage as being the latest available
   * version.
   *
   * @pre We are a Peon
   * @post The new value is locally stored
   * @post Fire up the callbacks waiting on waiting_for_commit
   *
   * @invariant The received message is an operation of type OP_COMMIT
   *
   * @param commit The message sent by the Leader to the Peon during
   *		   Paxos::commit
   */
  void handle_commit(MMonPaxos *commit);
  /**
   * Extend the system's lease.
   *
   * This means that the Leader considers that it should now safe to read from
   * any node on the system, since every quorum member is now in possession of
   * the latest version. Therefore, the Leader will send a message stating just
   * this to each quorum member, and will impose a limited timeframe during
   * which acks will be accepted. If there aren't as many acks as expected
   * (i.e, if at least one quorum member does not ack the lease) during this
   * timeframe, then we will force fresh elections.
   *
   * @pre We are the Leader
   * @pre We are on STATE_ACTIVE
   * @post A message extending the lease is sent to each quorum member
   * @post A timeout callback is set to limit the amount of time we will wait
   *	   for lease acks.
   * @post A timer is set in order to renew the lease after a certain amount
   *	   of time.
   */
  void extend_lease();
  /**
   * Update the lease on the Peon's side of things.
   *
   * Once a Peon receives a Lease message, it will update its lease_expire
   * variable, reply to the Leader acknowledging the lease update and set a
   * timeout callback to be fired upon the lease's expiration. Finally, the
   * Peon will fire up all the callbacks waiting for it to become active,
   * which it just did, and all those waiting for it to become readable,
   * which should be true if the Peon's lease didn't expire in the mean time.
   *
   * @pre We are a Peon
   * @post We update the lease accordingly
   * @post A lease timeout callback is set
   * @post Move to STATE_ACTIVE
   * @post Fire up all the callbacks waiting for STATE_ACTIVE
   * @post Fire up all the callbacks waiting for readable iif we are readable
   * @post Ack the lease to the Leader
   *
   * @invariant The received message is an operation of type OP_LEASE
   *
   * @param The message sent by the Leader to the Peon during the
   *	    Paxos::extend_lease function
   */
  void handle_lease(MMonPaxos *lease);
  /**
   * Account for all the Lease Acks the Leader receives from the Peons.
   *
   * Once the Leader receives all the Lease Acks from the Peons, it will be
   * able to cancel the Lease Ack timeout callback, thus avoiding calling
   * fresh elections.
   *
   * @pre We are the Leader
   * @post Cancel the Lease Ack timeout callback iif we receive acks from all
   *	   the quorum members
   *
   * @invariant The received message is an operation of type OP_LEASE_ACK
   *
   * @param ack The message sent by a Peon to the Leader during the
   *		Paxos::handle_lease function
   */
  void handle_lease_ack(MMonPaxos *ack);
  /**
   * Call fresh elections because at least one Peon didn't acked our lease.
   *
   * @pre We are the Leader
   * @pre We are on STATE_ACTIVE
   * @post Trigger fresh elections
   */
  void lease_ack_timeout();
  /**
   * Extend lease since we haven't had new committed values meanwhile.
   *
   * @pre We are the Leader
   * @pre We are on STATE_ACTIVE
   * @post Go through with Paxos::extend_lease
   */
  void lease_renew_timeout();
  /**
   * Call fresh elections because the Peon's lease expired without being
   * renewed or receiving a fresh lease.
   *
   * This means that the Peon is no longer assumed as being in the quorum
   * (or there is no Leader to speak of), so just trigger fresh elections
   * to circumvent this issue.
   *
   * @pre We are a Peon
   * @post Trigger fresh elections
   */
  void lease_timeout();        // on peon, if lease isn't extended

  /// restart the lease timeout timer
  void reset_lease_timeout();

  /**
   * Cancel all of Paxos' timeout/renew events. 
   */
  void cancel_events();
  /**
   * Shutdown this Paxos machine
   */
  void shutdown();

  /**
   * Generate a new Proposal Number based on @p gt
   *
   * @todo Check what @p gt actually means and what its usage entails
   * @param gt A hint for the geration of the Proposal Number
   * @return A globally unique, monotonically increasing Proposal Number
   */
  version_t get_new_proposal_number(version_t gt=0);
 
  /**
   * @todo document sync function
   */
  void warn_on_future_time(utime_t t, entity_name_t from);

  /**
   * Queue a new proposal by pushing it at the back of the queue; do not
   * propose it.
   *
   * @param bl The bufferlist to be proposed
   * @param onfinished The callback to be called once the proposal finishes
   */
  void queue_proposal(bufferlist& bl, Context *onfinished);
  /**
   * Begin proposing the Proposal at the front of the proposals queue.
   */
  void propose_queued();
  void finish_queued_proposal();
  void finish_proposal();

public:
  /**
   * @param m A monitor
   * @param mid A machine id
   */
  Paxos(Monitor *m, const string &name) 
		 : mon(m),
		   paxos_name(name),
		   state(STATE_RECOVERING),
		   first_committed(0),
		   last_pn(0),
		   last_committed(0),
		   accepted_pn(0),
		   accepted_pn_from(0),
		   num_last(0),
		   uncommitted_v(0), uncommitted_pn(0),
		   collect_timeout_event(0),
		   lease_renew_event(0),
		   lease_ack_timeout_event(0),
		   lease_timeout_event(0),
		   accept_timeout_event(0),
		   clock_drift_warned(0),
		   going_to_trim(false),
		   trim_disabled_version(0) { }

  const string get_name() const {
    return paxos_name;
  }

  void dispatch(PaxosServiceMessage *m);

  void reapply_all_versions();
  void apply_version(MonitorDBStore::Transaction &tx, version_t v);

  void init();
  /**
   * This function runs basic consistency checks. Importantly, if
   * it is inconsistent and shouldn't be, it asserts out.
   *
   * @return True if consistent, false if not.
   */
  bool is_consistent();

  void restart();
  /**
   * Initiate the Leader after it wins an election.
   *
   * Once an election is won, the Leader will be initiated and there are two
   * possible outcomes of this method: the Leader directly jumps to the active
   * state (STATE_ACTIVE) if it believes to be the only one in the quorum, or
   * will start recovering (STATE_RECOVERING) by initiating the collect phase. 
   *
   * @pre Our monitor is the Leader.
   * @post We are either on STATE_ACTIVE if we're the only one in the quorum,
   *	   or on STATE_RECOVERING otherwise.
   */
  void leader_init();
  /**
   * Initiate a Peon after it loses an election.
   *
   * If we are a Peon, then there must be a Leader and we are not alone in the
   * quorum, thus automatically assume we are on STATE_RECOVERING, which means
   * we will soon be enrolled into the Leader's collect phase.
   *
   * @pre There is a Leader, and he's about to start the collect phase.
   * @post We are on STATE_RECOVERING and will soon receive collect phase's 
   *	   messages.
   */
  void peon_init();

  /**
   * Include an incremental state of values, ranging from peer_first_committed
   * to the last committed value, on the message m
   *
   * @param m A message
   * @param peer_first_committed Lowest version to take into account
   * @param peer_last_committed Highest version to take into account
   */
  void share_state(MMonPaxos *m, version_t peer_first_committed,
		   version_t peer_last_committed);
  /**
   * Store on disk a state that was shared with us
   *
   * Basically, we received a set of version. Or just one. It doesn't matter.
   * What matters is that we have to stash it in the store. So, we will simply
   * write every single bufferlist into their own versions on our side (i.e.,
   * onto paxos-related keys), and then we will decode those same bufferlists
   * we just wrote and apply the transactions they hold. We will also update
   * our first and last committed values to point to the new values, if need
   * be. All all this is done tightly wrapped in a transaction to ensure we
   * enjoy the atomicity guarantees given by our awesome k/v store.
   *
   * @param m A message
   */
  void store_state(MMonPaxos *m);
  /**
   * Helper function to decode a bufferlist into a transaction and append it
   * to another transaction.
   *
   * This function is used during the Leader's commit and during the
   * Paxos::store_state in order to apply the bufferlist's transaction onto
   * the store.
   *
   * @param t The transaction to which we will append the operations
   * @param bl A bufferlist containing an encoded transaction
   */
  void decode_append_transaction(MonitorDBStore::Transaction& t,
				 bufferlist& bl) {
    MonitorDBStore::Transaction vt;
    bufferlist::iterator it = bl.begin();
    vt.decode(it);
    t.append(vt);
  }

  /**
   * @todo This appears to be used only by the OSDMonitor, and I would say
   *	   its objective is to allow a third-party to have a "private"
   *	   state dir. -JL
   */
  void add_extra_state_dir(string s) {
    extra_state_dirs.push_back(s);
  }

  // -- service interface --
  /**
   * Add c to the list of callbacks waiting for us to become active.
   *
   * @param c A callback
   */
  void wait_for_active(Context *c) {
    waiting_for_active.push_back(c);
  }

  /**
   * Erase old states from stable storage.
   *
   * @param first The version we are trimming to
   */
  void trim_to(version_t first);
  /**
   * Erase old states from stable storage.
   *
   * @param t A transaction
   * @param first The version we are trimming to
   */
  void trim_to(MonitorDBStore::Transaction *t, version_t first);
  /**
   * Auxiliary function to erase states in the interval [from, to[ from stable
   * storage.
   *
   * @param t A transaction
   * @param from Bottom limit of the interval of versions to erase
   * @param to Upper limit, not including, of the interval of versions to erase
   */
  void trim_to(MonitorDBStore::Transaction *t, version_t from, version_t to);
  /**
   * Trim the Paxos state as much as we can.
   */
  void trim() {
    assert(should_trim());
    version_t trim_to_version = MIN(get_version() - g_conf->paxos_max_join_drift,
				    get_first_committed() + g_conf->paxos_trim_max);
    trim_to(trim_to_version);
  }
  /**
   * Disable trimming
   *
   * This is required by the Monitor's store synchronization mechanisms
   * to guarantee a consistent store state.
   */
  void trim_disable() {
    if (!trim_disabled_version)
      trim_disabled_version = get_version();
  }
  /**
   * Enable trimming
   */
  void trim_enable();
  /**
   * Check if trimming has been disabled
   *
   * @returns true if trim has been disabled; false otherwise.
   */
  bool is_trim_disabled() { return (trim_disabled_version > 0); }
  /**
   * Check if we should trim.
   *
   * If trimming is disabled, we must take that into consideration and only
   * return true if we are positively sure that we should trim soon.
   *
   * @returns true if we should trim; false otherwise.
   */
  bool should_trim() {
    int available_versions = (get_version() - get_first_committed());
    int maximum_versions =
      (g_conf->paxos_max_join_drift + g_conf->paxos_trim_min);

    if (going_to_trim || (available_versions <= maximum_versions))
      return false;

    if (trim_disabled_version > 0) {
      int disabled_versions = (get_version() - trim_disabled_version);
      if (disabled_versions < g_conf->paxos_trim_disabled_max_versions)
	return false;
    }
    return true;
  }
 
  // read
  /**
   * @defgroup Paxos_h_read_funcs Read-related functions
   * @{
   */
  /**
   * Get latest committed version
   *
   * @return latest committed version
   */
  version_t get_version() { return last_committed; }
  /**
   * Get first committed version
   *
   * @return the first committed version
   */
  version_t get_first_committed() { return first_committed; }
  /**
   * Check if a given version is readable.
   *
   * A version may not be readable for a myriad of reasons:
   *  @li the version @v is higher that the last committed version
   *  @li we are not the Leader nor a Peon (election may be on-going)
   *  @li we do not have a committed value yet
   *  @li we do not have a valid lease
   *
   * @param seen The version we want to check if it is readable.
   * @return 'true' if the version is readable; 'false' otherwise.
   */
  bool is_readable(version_t seen=0);
  /**
   * Read version @v and store its value in @bl
   *
   * @param[in] v The version we want to read
   * @param[out] bl The version's value
   * @return 'true' if we successfully read the value; 'false' otherwise
   */
  bool read(version_t v, bufferlist &bl);
  /**
   * Read the latest committed version
   *
   * @param[out] bl The version's value
   * @return the latest committed version if we successfully read the value;
   *	     or 0 (zero) otherwise.
   */
  version_t read_current(bufferlist &bl);
  /**
   * Add onreadable to the list of callbacks waiting for us to become readable.
   *
   * @param onreadable A callback
   */
  void wait_for_readable(Context *onreadable) {
    assert(!is_readable());
    waiting_for_readable.push_back(onreadable);
  }
  /**
   * @}
   */

  /**
   * Check if we have a valid lease.
   *
   * @returns true if the lease is still valid; false otherwise.
   */
  bool is_lease_valid();
  // write
  /**
   * @defgroup Paxos_h_write_funcs Write-related functions
   * @{
   */
  /**
   * Check if we are writeable.
   *
   * We are writeable if we are alone (i.e., a quorum of one), or if we match
   * all the following conditions:
   *  @li We are the Leader
   *  @li We are on STATE_ACTIVE
   *  @li We have a valid lease
   *
   * @return 'true' if we are writeable; 'false' otherwise.
   */
  bool is_writeable();
  /**
   * Add c to the list of callbacks waiting for us to become writeable.
   *
   * @param c A callback
   */
  void wait_for_writeable(Context *c) {
    assert(!is_writeable());
    waiting_for_writeable.push_back(c);
  }

  /**
   * List all queued proposals
   *
   * @param out[out] Output Stream onto which we will output the list
   *		     of queued proposals.
   */
  void list_proposals(ostream& out);
  /**
   * Propose a new value to the Leader.
   *
   * This function enables the submission of a new value to the Leader, which
   * will trigger a new proposal.
   *
   * @param bl A bufferlist holding the value to be proposed
   * @param onfinish A callback to be fired up once we finish the proposal
   */
  bool propose_new_value(bufferlist& bl, Context *onfinished=0);
  /**
   * Add oncommit to the back of the list of callbacks waiting for us to
   * finish committing.
   *
   * @param oncommit A callback
   */
  void wait_for_commit(Context *oncommit) {
    waiting_for_commit.push_back(oncommit);
  }
  /**
   * Add oncommit to the front of the list of callbacks waiting for us to
   * finish committing.
   *
   * @param oncommit A callback
   */
  void wait_for_commit_front(Context *oncommit) {
    waiting_for_commit.push_front(oncommit);
  }
  /**
   * @}
   */

  /**
   * @}
   */
 protected:
  MonitorDBStore *get_store();
};

inline ostream& operator<<(ostream& out, Paxos::C_Proposal& p)
{
  string proposed = (p.proposed ? "proposed" : "unproposed");
  out << " " << proposed
      << " queued " << (ceph_clock_now(NULL) - p.proposal_time)
      << " tx dump:\n";
  MonitorDBStore::Transaction t;
  bufferlist::iterator p_it = p.bl.begin();
  t.decode(p_it);
  JSONFormatter f(true);
  t.dump(&f);
  f.flush(out);
  return out;
}

#endif

