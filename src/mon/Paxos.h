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
#include "common/perf_counters.h"
#include <errno.h>

#include "MonitorDBStore.h"
#include "mon/MonOpRequest.h"

class Monitor;
class MMonPaxos;
class Paxos;

enum {
  l_paxos_first = 45800,
  l_paxos_start_leader,
  l_paxos_start_peon,
  l_paxos_restart,
  l_paxos_refresh,
  l_paxos_refresh_latency,
  l_paxos_begin,
  l_paxos_begin_keys,
  l_paxos_begin_bytes,
  l_paxos_begin_latency,
  l_paxos_commit,
  l_paxos_commit_keys,
  l_paxos_commit_bytes,
  l_paxos_commit_latency,
  l_paxos_collect,
  l_paxos_collect_keys,
  l_paxos_collect_bytes,
  l_paxos_collect_latency,
  l_paxos_collect_uncommitted,
  l_paxos_collect_timeout,
  l_paxos_accept_timeout,
  l_paxos_lease_ack_timeout,
  l_paxos_lease_timeout,
  l_paxos_store_state,
  l_paxos_store_state_keys,
  l_paxos_store_state_bytes,
  l_paxos_store_state_latency,
  l_paxos_share_state,
  l_paxos_share_state_keys,
  l_paxos_share_state_bytes,
  l_paxos_new_pn,
  l_paxos_new_pn_latency,
  l_paxos_last,
};


// i am one state machine.
/**
 * This libary is based on the Paxos algorithm, but varies in a few key ways:
 *  1- Only a single new value is generated at a time, simplifying the recovery logic.
 *  2- Nodes track "committed" values, and share them generously (and trustingly)
 *  3- A 'leasing' mechanism is built-in, allowing nodes to determine when it is 
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

  /// perf counter for internal instrumentations
  PerfCounters *logger;

  void init_logger();

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
  enum {
    /**
     * Leader/Peon is in Paxos' Recovery state
     */
    STATE_RECOVERING,
    /**
     * Leader/Peon is idle, and the Peon may or may not have a valid lease.
     */
    STATE_ACTIVE,
    /**
     * Leader/Peon is updating to a new value.
     */
    STATE_UPDATING,
    /*
     * Leader proposing an old value
     */
    STATE_UPDATING_PREVIOUS,
    /*
     * Leader/Peon is writing a new commit.  readable, but not
     * writeable.
     */
    STATE_WRITING,
    /*
     * Leader/Peon is writing a new commit from a previous round.
     */
    STATE_WRITING_PREVIOUS,
    // leader: refresh following a commit
    STATE_REFRESH,
  };

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
    switch (s) {
    case STATE_RECOVERING:
      return "recovering";
    case STATE_ACTIVE:
      return "active";
    case STATE_UPDATING:
      return "updating";
    case STATE_UPDATING_PREVIOUS:
      return "updating-previous";
    case STATE_WRITING:
      return "writing";
    case STATE_WRITING_PREVIOUS:
      return "writing-previous";
    case STATE_REFRESH:
      return "refresh";
    default:
      return "UNKNOWN";
    }
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
  bool is_recovering() const { return (state == STATE_RECOVERING); }
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
  bool is_updating() const { return state == STATE_UPDATING; }

  /**
   * Check if we are updating/proposing a previous value from a
   * previous quorum
   */
  bool is_updating_previous() const { return state == STATE_UPDATING_PREVIOUS; }

  /// @return 'true' if we are writing an update to disk
  bool is_writing() const { return state == STATE_WRITING; }

  /// @return 'true' if we are writing an update-previous to disk
  bool is_writing_previous() const { return state == STATE_WRITING_PREVIOUS; }

  /// @return 'true' if we are refreshing an update just committed
  bool is_refresh() const { return state == STATE_REFRESH; }

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
   * When the commit finished.
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
   * The last_committed epoch of the leader at the time we accepted the last pn.
   *
   * This has NO SEMANTIC MEANING, and is there only for the debug output.
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
   * not be extended. 
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
   * the Leader the last Proposal Number it accepted, the Leader will be able
   * to infer if this value is more recent than the one the Leader has, thus
   * more relevant.
   */
  version_t  uncommitted_pn;
  /**
   * Uncommitted Value.
   *
   * If the system fails in-between the accept replies from the Peons and the
   * instruction to commit from the Leader, then we may end up with accepted
   * but yet-uncommitted values. During the Leader's recovery, it will attempt
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
   * Pending proposal transaction
   *
   * This is the transaction that is under construction and pending
   * proposal.  We will add operations to it until we decide it is
   * time to start a paxos round.
   */
  MonitorDBStore::TransactionRef pending_proposal;

  /**
   * Finishers for pending transaction
   *
   * These are waiting for updates in the pending proposal/transaction
   * to be committed.
   */
  list<Context*> pending_finishers;

  /**
   * Finishers for committing transaction
   *
   * When the pending_proposal is submitted, pending_finishers move to
   * this list.  When it commits, these finishers are notified.
   */
  list<Context*> committing_finishers;

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
  bool trimming;

  /**
   * @defgroup Paxos_h_callbacks Callback classes.
   * @{
   */
  /**
   * Callback class responsible for handling a Collect Timeout.
   */
  class C_CollectTimeout;
  /**
   * Callback class responsible for handling an Accept Timeout.
   */
  class C_AcceptTimeout;
  /**
   * Callback class responsible for handling a Lease Ack Timeout.
   */
  class C_LeaseAckTimeout;

  /**
   * Callback class responsible for handling a Lease Timeout.
   */
  class C_LeaseTimeout;

  /**
   * Callback class responsible for handling a Lease Renew Timeout.
   */
  class C_LeaseRenew;

  class C_Trimmed;
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
   * sending these information in a message to the quorum, we expect to
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
   * with its first and last committed versions, as well as information so
   * the Leader may know if its Proposal Number was, or was not, accepted by
   * the Peon. The Peon will accept the Leader's Proposal Number iif it is
   * higher than the Peon's currently accepted Proposal Number. The Peon may
   * also inform the Leader of accepted but uncommitted values.
   *
   * @invariant The message is an operation of type OP_COLLECT.
   * @pre We are a Peon.
   * @post Replied to the Leader, accepting or not accepting its PN.
   *
   * @param collect The collect message sent by the Leader to the Peon.
   */
  void handle_collect(MonOpRequestRef op);
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
  void handle_last(MonOpRequestRef op);
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
   * @post We send a reply message to the Leader iif we accept its proposal
   *
   * @invariant The received message is an operation of type OP_BEGIN
   *
   * @param begin The message sent by the Leader to the Peon during the
   *		  Paxos::begin function
   *
   */
  void handle_begin(MonOpRequestRef op);
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
  void handle_accept(MonOpRequestRef op);
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


  utime_t commit_start_stamp;
  friend struct C_Committed;

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
  void commit_start();
  void commit_finish();   ///< finish a commit after txn becomes durable
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
  void handle_commit(MonOpRequestRef op);
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
   * @param lease The message sent by the Leader to the Peon during the
   *	    Paxos::extend_lease function
   */
  void handle_lease(MonOpRequestRef op);
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
  void handle_lease_ack(MonOpRequestRef op);
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
   * Begin proposing the pending_proposal.
   */
  void propose_pending();

  /**
   * refresh state from store
   *
   * Called when we have new state for the mon to consume.  If we return false,
   * abort (we triggered a bootstrap).
   *
   * @returns true on success, false if we are now bootstrapping
   */
  bool do_refresh();

  void commit_proposal();
  void finish_round();

public:
  /**
   * @param m A monitor
   * @param name A name for the paxos service. It serves as the naming space
   * of the underlying persistent storage for this service.
   */
  Paxos(Monitor *m, const string &name) 
		 : mon(m),
		   logger(NULL),
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
		   trimming(false) { }

  const string get_name() const {
    return paxos_name;
  }

  void dispatch(MonOpRequestRef op);

  void read_and_prepare_transactions(MonitorDBStore::TransactionRef tx,
				     version_t from, version_t last);

  void init();

  /**
   * dump state info to a formatter
   */
  void dump_info(Formatter *f);

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
   * @pre There is a Leader, and it?s about to start the collect phase.
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
   * be. All this is done tightly wrapped in a transaction to ensure we
   * enjoy the atomicity guarantees given by our awesome k/v store.
   *
   * @param m A message
   * @returns true if we stored something new; false otherwise
   */
  bool store_state(MMonPaxos *m);
  void _sanity_check_store();

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
  static void decode_append_transaction(MonitorDBStore::TransactionRef t,
					bufferlist& bl) {
    MonitorDBStore::TransactionRef vt(new MonitorDBStore::Transaction);
    bufferlist::iterator it = bl.begin();
    vt->decode(it);
    t->append(vt);
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
  void wait_for_active(MonOpRequestRef op, Context *c) {
    if (op)
      op->mark_event("paxos:wait_for_active");
    waiting_for_active.push_back(c);
  }
  void wait_for_active(Context *c) {
    MonOpRequestRef o;
    wait_for_active(o, c);
  }

  /**
   * Trim the Paxos state as much as we can.
   */
  void trim();

  /**
   * Check if we should trim.
   *
   * If trimming is disabled, we must take that into consideration and only
   * return true if we are positively sure that we should trim soon.
   *
   * @returns true if we should trim; false otherwise.
   */
  bool should_trim() {
    int available_versions = get_version() - get_first_committed();
    int maximum_versions = g_conf->paxos_min + g_conf->paxos_trim_min;

    if (trimming || (available_versions <= maximum_versions))
      return false;

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
   *  @li the version @e v is higher that the last committed version
   *  @li we are not the Leader nor a Peon (election may be on-going)
   *  @li we do not have a committed value yet
   *  @li we do not have a valid lease
   *
   * @param seen The version we want to check if it is readable.
   * @return 'true' if the version is readable; 'false' otherwise.
   */
  bool is_readable(version_t seen=0);
  /**
   * Read version @e v and store its value in @e bl
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
  void wait_for_readable(MonOpRequestRef op, Context *onreadable) {
    assert(!is_readable());
    if (op)
      op->mark_event("paxos:wait_for_readable");
    waiting_for_readable.push_back(onreadable);
  }
  void wait_for_readable(Context *onreadable) {
    MonOpRequestRef o;
    wait_for_readable(o, onreadable);
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
  void wait_for_writeable(MonOpRequestRef op, Context *c) {
    assert(!is_writeable());
    if (op)
      op->mark_event("paxos:wait_for_writeable");
    waiting_for_writeable.push_back(c);
  }
  void wait_for_writeable(Context *c) {
    MonOpRequestRef o;
    wait_for_writeable(o, c);
  }

  /**
   * Get a transaction to submit operations to propose against
   *
   * Apply operations to this transaction.  It will eventually be proposed
   * to paxos.
   */
  MonitorDBStore::TransactionRef get_pending_transaction();

  /**
   * Queue a completion for the pending proposal
   *
   * This completion will get triggered when the pending proposal
   * transaction commits.
   */
  void queue_pending_finisher(Context *onfinished);

  /**
   * (try to) trigger a proposal
   *
   * Tell paxos that it should submit the pending proposal.  Note that if it
   * is not active (e.g., because it is already in the midst of committing
   * something) that will be deferred (e.g., until the current round finishes).
   */
  bool trigger_propose();

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
  MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
  bufferlist::iterator p_it = p.bl.begin();
  t->decode(p_it);
  JSONFormatter f(true);
  t->dump(&f);
  f.flush(out);
  return out;
}

#endif

