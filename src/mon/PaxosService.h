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

#ifndef CEPH_PAXOSSERVICE_H
#define CEPH_PAXOSSERVICE_H

#include "messages/PaxosServiceMessage.h"
#include "include/Context.h"
#include "include/stringify.h"
#include <errno.h>
#include "Paxos.h"
#include "Monitor.h"
#include "MonitorDBStore.h"

class Monitor;
class Paxos;

/**
 * A Paxos Service is an abstraction that easily allows one to obtain an
 * association between a Monitor and a Paxos class, in order to implement any
 * service.
 */
class PaxosService {
  /**
   * @defgroup PaxosService_h_class Paxos Service
   * @{
   */
 public:
  /**
   * The Monitor to which this class is associated with
   */
  Monitor *mon;
  /**
   * The Paxos instance to which this class is associated with
   */
  Paxos *paxos;
  /**
   * Our name. This will be associated with the class implementing us, and will
   * be used mainly for store-related operations.
   */
  string service_name;
  /**
   * If we are or have queued anything for proposal, this variable will be true
   * until our proposal has been finished.
   */
  bool proposing;

 protected:
  /**
   * Services implementing us used to depend on the Paxos version, back when
   * each service would have a Paxos instance for itself. However, now we only
   * have a single Paxos instance, shared by all the services. Each service now
   * must keep its own version, if so they wish. This variable should be used
   * for that purpose.
   */
  version_t service_version;

 private:
  /**
   * Event callback responsible for proposing our pending value once a timer 
   * runs out and fires.
   */
  Context *proposal_timer;
  /**
   * If the implementation class has anything pending to be proposed to Paxos,
   * then have_pending should be true; otherwise, false.
   */
  bool have_pending; 

protected:

  /**
   * format of our state in leveldb, 0 for default
   */
  version_t format_version;



  /**
   * @defgroup PaxosService_h_callbacks Callback classes
   * @{
   */
  /**
   * Retry dispatching a given service message
   *
   * This callback class is used when we had to wait for some condition to
   * become true while we were dispatching it.
   *
   * For instance, if the message's version isn't readable, according to Paxos,
   * then we must wait for it to become readable. So, we just queue an
   * instance of this class onto the Paxos::wait_for_readable function, and
   * we will retry the whole dispatch again once the callback is fired.
   */
  class C_RetryMessage : public Context {
    PaxosService *svc;
    PaxosServiceMessage *m;
  public:
    C_RetryMessage(PaxosService *s, PaxosServiceMessage *m_) : svc(s), m(m_) {}
    void finish(int r) {
      if (r == -EAGAIN || r >= 0)
	svc->dispatch(m);
      else if (r == -ECANCELED)
	m->put();
      else
	assert(0 == "bad C_RetryMessage return value");
    }
  };

  /**
   * Callback used to make sure we call the PaxosService::_active function
   * whenever a condition is fulfilled.
   *
   * This is used in multiple situations, from waiting for the Paxos to commit
   * our proposed value, to waiting for the Paxos to become active once an
   * election is finished.
   */
  class C_Active : public Context {
    PaxosService *svc;
  public:
    C_Active(PaxosService *s) : svc(s) {}
    void finish(int r) {
      if (r >= 0)
	svc->_active();
    }
  };

  /**
   * Callback class used to propose the pending value once the proposal_timer
   * fires up.
   */
  class C_Propose : public Context {
    PaxosService *ps;
  public:
    C_Propose(PaxosService *p) : ps(p) { }
    void finish(int r) {
      ps->proposal_timer = 0;
      if (r >= 0)
	ps->propose_pending();
      else if (r == -ECANCELED || r == -EAGAIN)
	return;
      else
	assert(0 == "bad return value for C_Propose");
    }
  };

  /**
   * Callback class used to mark us as active once a proposal finishes going
   * through Paxos.
   *
   * We should wake people up *only* *after* we inform the service we
   * just went active. And we should wake people up only once we finish
   * going active. This is why we first go active, avoiding to wake up the
   * wrong people at the wrong time, such as waking up a C_RetryMessage
   * before waking up a C_Active, thus ending up without a pending value.
   */
  class C_Committed : public Context {
    PaxosService *ps;
  public:
    C_Committed(PaxosService *p) : ps(p) { }
    void finish(int r) {
      ps->proposing = false;
      if (r >= 0)
	ps->_active();
      else if (r == -ECANCELED || r == -EAGAIN)
	return;
      else
	assert(0 == "bad return value for C_Committed");
    }
  };
  /**
   * @}
   */
  friend class C_Propose;
  

public:
  /**
   * @param mn A Monitor instance
   * @param p A Paxos instance
   * @parem name Our service's name.
   */
  PaxosService(Monitor *mn, Paxos *p, string name) 
    : mon(mn), paxos(p), service_name(name),
      proposing(false),
      service_version(0), proposal_timer(0), have_pending(false),
      format_version(0),
      last_committed_name("last_committed"),
      first_committed_name("first_committed"),
      full_prefix_name("full"), full_latest_name("latest"),
      cached_first_committed(0), cached_last_committed(0)
  {
  }

  virtual ~PaxosService() {}

  /**
   * Get the service's name.
   *
   * @returns The service's name.
   */
  string get_service_name() { return service_name; }

  /**
   * Get the store prefixes we utilize
   */
  virtual void get_store_prefixes(set<string>& s) {
    s.insert(service_name);
  }
  
  // i implement and you ignore
  /**
   * Informs this instance that it should consider itself restarted.
   *
   * This means that we will cancel our proposal_timer event, if any exists.
   */
  void restart();
  /**
   * Informs this instance that an election has finished.
   *
   * This means that we will invoke a PaxosService::discard_pending while
   * setting have_pending to false (basically, ignore our pending state) and
   * we will then make sure we obtain a new state.
   *
   * Our state shall be updated by PaxosService::_active if the Paxos is
   * active; otherwise, we will wait for it to become active by adding a 
   * PaxosService::C_Active callback to it.
   */
  void election_finished();
  /**
   * Informs this instance that it is supposed to shutdown.
   *
   * Basically, it will instruct Paxos to cancel all events/callbacks and then
   * will cancel the proposal_timer event if any exists.
   */
  void shutdown();

private:
  /**
   * Update our state by updating it from Paxos, and then creating a new
   * pending state if need be.
   *
   * @remarks We only create a pending state we our Monitor is the Leader.
   *
   * @pre Paxos is active
   * @post have_pending is true iif our Monitor is the Leader and Paxos is
   *	   active
   */
  void _active();
  /**
   * Scrub our versions after we convert the store from the old layout to
   * the new k/v store.
   */
  void remove_legacy_versions();

public:
  /**
   * Propose a new value through Paxos.
   *
   * This function should be called by the classes implementing 
   * PaxosService, in order to propose a new value through Paxos.
   *
   * @pre The implementation class implements the encode_pending function.
   * @pre have_pending is true
   * @pre Our monitor is the Leader
   * @pre Paxos is active
   * @post Cancel the proposal timer, if any
   * @post have_pending is false
   * @post propose pending value through Paxos
   *
   * @note This function depends on the implementation of encode_pending on
   *	   the class that is implementing PaxosService
   */
  void propose_pending();

  /**
   * Let others request us to propose.
   *
   * At the moment, this is just a wrapper to propose_pending() with an
   * extra check for is_writeable(), but it's a good practice to dissociate
   * requests for proposals from direct usage of propose_pending() for
   * future use -- we might want to perform additional checks or put a
   * request on hold, for instance.
   */
  void request_proposal() {
    assert(is_writeable());

    propose_pending();
  }
  /**
   * Request service @p other to perform a proposal.
   *
   * We could simply use the function above, requesting @p other directly,
   * but we might eventually want to do something to the request -- say,
   * set a flag stating we're waiting on a cross-proposal to be finished.
   */
  void request_proposal(PaxosService *other) {
    assert(other != NULL);
    assert(other->is_writeable());

    other->request_proposal();
  }

  /**
   * Dispatch a message by passing it to several different functions that are
   * either implemented directly by this service, or that should be implemented
   * by the class implementing this service.
   *
   * @param m A message
   * @returns 'true' on successful dispatch; 'false' otherwise.
   */
  bool dispatch(PaxosServiceMessage *m);

  void refresh(bool *need_bootstrap);

  /**
   * @defgroup PaxosService_h_override_funcs Functions that should be
   *					     overridden.
   *
   * These functions should be overridden at will by the class implementing
   * this service.
   * @{
   */
  /**
   * Create the initial state for your system.
   *
   * In some of ours the state is actually set up elsewhere so this does
   * nothing.
   */
  virtual void create_initial() = 0;

  /**
   * Query the Paxos system for the latest state and apply it if it's newer
   * than the current Monitor state.
   *
   * @returns 'true' on success; 'false' otherwise.
   */
  virtual void update_from_paxos(bool *need_bootstrap) = 0;

  /**
   * Hook called after all services have refreshed their state from paxos
   *
   * This is useful for doing any update work that depends on other
   * service's having up-to-date state.
   */
  virtual void post_paxos_update() {}

  /**
   * Init on startup
   *
   * This is called on mon startup, after all of the PaxosService instances'
   * update_from_paxos() methods have been called
   */
  virtual void init() {}

  /**
   * Create the pending state.
   *
   * @invariant This function is only called on a Leader.
   * @remarks This created state is then modified by incoming messages.
   * @remarks Called at startup and after every Paxos ratification round.
   */
  virtual void create_pending() = 0;

  /**
   * Encode the pending state into a bufferlist for ratification and
   * transmission as the next state.
   *
   * @invariant This function is only called on a Leader.
   *
   * @param t The transaction to hold all changes.
   */
  virtual void encode_pending(MonitorDBStore::TransactionRef t) = 0;

  /**
   * Discard the pending state
   *
   * @invariant This function is only called on a Leader.
   *
   * @remarks This function is NOT overridden in any of our code, but it is
   *	      called in PaxosService::election_finished if have_pending is
   *	      true.
   */
  virtual void discard_pending() { }

  /**
   * Look at the query; if the query can be handled without changing state,
   * do so.
   *
   * @param m A query message
   * @returns 'true' if the query was handled (e.g., was a read that got
   *	      answered, was a state change that has no effect); 'false' 
   *	      otherwise.
   */
  virtual bool preprocess_query(PaxosServiceMessage *m) = 0;

  /**
   * Apply the message to the pending state.
   *
   * @invariant This function is only called on a Leader.
   *
   * @param m An update message
   * @returns 'true' if the update message was handled (e.g., a command that
   *	      went through); 'false' otherwise.
   */
  virtual bool prepare_update(PaxosServiceMessage *m) = 0;
  /**
   * @}
   */

  /**
   * Determine if the Paxos system should vote on pending, and if so how long
   * it should wait to vote.
   *
   * @param[out] delay The wait time, used so we can limit the update traffic
   *		       spamming.
   * @returns 'true' if the Paxos system should propose; 'false' otherwise.
   */
  virtual bool should_propose(double &delay);

  /**
   * @defgroup PaxosService_h_courtesy Courtesy functions
   *
   * Courtesy functions, in case the class implementing this service has
   * anything it wants/needs to do at these times.
   * @{
   */
  /**
   * This is called when the Paxos state goes to active.
   *
   * On the peon, this is after each election.
   * On the leader, this is after each election, *and* after each completed
   * proposal.
   *
   * @note This function may get called twice in certain recovery cases.
   */
  virtual void on_active() { }

  /**
   * This is called when we are shutting down
   */
  virtual void on_shutdown() {}

  /**
   * this is called when activating on the leader
   *
   * it should conditionally upgrade the on-disk format by proposing a transaction
   */
  virtual void upgrade_format() { }

  /**
   * this is called when we detect the store has just upgraded underneath us
   */
  virtual void on_upgrade() {}

  /**
   * Called when the Paxos system enters a Leader election.
   *
   * @remarks It's a courtesy method, in case the class implementing this
   *	      service has anything it wants/needs to do at that time.
   */
  virtual void on_restart() { }
  /**
   * @}
   */

  /**
   * Tick.
   */
  virtual void tick() {}

  /**
   * Get health information
   *
   * @param summary list of summary strings and associated severity
   * @param detail optional list of detailed problem reports; may be NULL
   */
  virtual void get_health(list<pair<health_status_t,string> >& summary,
			  list<pair<health_status_t,string> > *detail) const { }

 private:
  /**
   * @defgroup PaxosService_h_store_keys Set of keys that are usually used on
   *					 all the services implementing this
   *					 class, and, being almost the only keys
   *					 used, should be standardized to avoid
   *					 mistakes.
   * @{
   */
  const string last_committed_name;
  const string first_committed_name;
  const string full_prefix_name;
  const string full_latest_name;
  /**
   * @}
   */

  /**
   * @defgroup PaxosService_h_version_cache Variables holding cached values
   *                                        for the most used versions (first
   *                                        and last committed); we only have
   *                                        to read them when the store is
   *                                        updated, so in-between updates we
   *                                        may very well use cached versions
   *                                        and avoid the overhead.
   * @{
   */
  version_t cached_first_committed;
  version_t cached_last_committed;
  /**
   * @}
   */

  /**
   * Callback list to be used whenever we are running a proposal through
   * Paxos. These callbacks will be awaken whenever the said proposal
   * finishes.
   */
  list<Context*> waiting_for_finished_proposal;

 public:

  /**
   * Check if we are proposing a value through Paxos
   *
   * @returns true if we are proposing; false otherwise.
   */
  bool is_proposing() {
    return proposing;
  }

  /**
   * Check if we are in the Paxos ACTIVE state.
   *
   * @note This function is a wrapper for Paxos::is_active
   *
   * @returns true if in state ACTIVE; false otherwise.
   */
  bool is_active() {
    return
      !is_proposing() &&
      (paxos->is_active() || paxos->is_updating() || paxos->is_writing());
  }

  /**
   * Check if we are readable.
   *
   * This mirrors on the paxos check, except that we also verify that
   *
   *  - the client hasn't seen the future relative to this PaxosService
   *  - this service isn't proposing.
   *  - we have committed our initial state (last_committed > 0)
   *
   * @param ver The version we want to check if is readable
   * @returns true if it is readable; false otherwise
   */
  bool is_readable(version_t ver = 0) {
    if (ver > get_last_committed() ||
	!paxos->is_readable(0) ||
	get_last_committed() == 0)
      return false;
    return true;
  }

  /**
   * Check if we are writeable.
   *
   * We consider to be writeable iff:
   *
   *  - we are not proposing a new version;
   *  - we are ready to be written to -- i.e., we have a pending value.
   *  - paxos is (active or updating or writing or refresh)
   *
   * @returns true if writeable; false otherwise
   */
  bool is_writeable() {
    return
      !is_proposing() &&
      is_write_ready() &&
      (paxos->is_active() || paxos->is_updating() || paxos->is_writing());
  }

  /**
   * Check if we are ready to be written to.  This means we must have a
   * pending value and be active.
   *
   * @returns true if we are ready to be written to; false otherwise.
   */
  bool is_write_ready() {
    return is_active() && have_pending;
  }

  /**
   * Wait for a proposal to finish.
   *
   * Add a callback to be awaken whenever our current proposal finishes being
   * proposed through Paxos.
   *
   * @param c The callback to be awaken once the proposal is finished.
   */
  void wait_for_finished_proposal(Context *c) {
    waiting_for_finished_proposal.push_back(c);
  }

  /**
   * Wait for us to become active
   *
   * @param c The callback to be awaken once we become active.
   */
  void wait_for_active(Context *c) {
    if (!is_proposing()) {
      paxos->wait_for_active(c);
      return;
    }
    wait_for_finished_proposal(c);
  }

  /**
   * Wait for us to become readable
   *
   * @param c The callback to be awaken once we become active.
   * @param ver The version we want to wait on.
   */
  void wait_for_readable(Context *c, version_t ver = 0) {
    /* This is somewhat of a hack. We only do check if a version is readable on
     * PaxosService::dispatch(), but, nonetheless, we must make sure that if that
     * is why we are not readable, then we must wait on PaxosService and not on
     * Paxos; otherwise, we may assert on Paxos::wait_for_readable() if it
     * happens to be readable at that specific point in time.
     */
    if (is_proposing() ||
	ver > get_last_committed() ||
	get_last_committed() == 0)
      wait_for_finished_proposal(c);
    else
      paxos->wait_for_readable(c);
  }

  /**
   * Wait for us to become writeable
   *
   * @param c The callback to be awaken once we become writeable.
   */
  void wait_for_writeable(Context *c) {
    if (is_proposing())
      wait_for_finished_proposal(c);
    else if (!is_write_ready())
      wait_for_active(c);
    else
      paxos->wait_for_writeable(c);
  }

  /**
   * @defgroup PaxosService_h_Trim
   * @{
   */
  /**
   * trim service states if appropriate
   *
   * Called at same interval as tick()
   */
  void maybe_trim();

  /**
   * Auxiliary function to trim our state from version @from to version @to,
   * not including; i.e., the interval [from, to[
   *
   * @param t The transaction to which we will add the trim operations.
   * @param from the lower limit of the interval to be trimmed
   * @param to the upper limit of the interval to be trimmed (not including)
   */
  void trim(MonitorDBStore::TransactionRef t, version_t from, version_t to);

  /**
   * encode service-specific extra bits into trim transaction
   *
   * @param tx transaction
   * @param first new first_committed value
   */
  virtual void encode_trim_extra(MonitorDBStore::TransactionRef tx,
				 version_t first) {}

  /**
   * Get the version we should trim to.
   *
   * Should be overloaded by service if it wants to trim states.
   *
   * @returns the version we should trim to; if we return zero, it should be
   *	      assumed that there's no version to trim to.
   */
  virtual version_t get_trim_to() {
    return 0;
  }

  /**
   * @}
   */
  /**
   * @defgroup PaxosService_h_Stash_Full
   * @{
   */
  virtual bool should_stash_full();
  /**
   * Encode a full version on @p t
   *
   * @note We force every service to implement this function, since we strongly
   *	   desire the encoding of full versions.
   * @note Services that do not trim their state, will be bound to only create
   *	   one full version. Full version stashing is determined/controled by
   *	   trimming: we stash a version each time a trim is bound to erase the
   *	   latest full version.
   *
   * @param t Transaction on which the full version shall be encoded.
   */
  virtual void encode_full(MonitorDBStore::TransactionRef t) = 0;

  /**
   * @}
   */

  /**
   * Cancel events.
   *
   * @note This function is a wrapper for Paxos::cancel_events
   */
  void cancel_events() {
    paxos->cancel_events();
  }

  /**
   * @defgroup PaxosService_h_store_funcs Back storage interface functions
   * @{
   */
  /**
   * @defgroup PaxosService_h_store_modify Wrapper function interface to access
   *					   the back store for modification
   *					   purposes
   * @{
   */
  void put_first_committed(MonitorDBStore::TransactionRef t, version_t ver) {
    t->put(get_service_name(), first_committed_name, ver);
  }
  /**
   * Set the last committed version to @p ver
   *
   * @param t A transaction to which we add this put operation
   * @param ver The last committed version number being put
   */
  void put_last_committed(MonitorDBStore::TransactionRef t, version_t ver) {
    t->put(get_service_name(), last_committed_name, ver);

    /* We only need to do this once, and that is when we are about to make our
     * first proposal. There are some services that rely on first_committed
     * being set -- and it should! -- so we need to guarantee that it is,
     * specially because the services itself do not do it themselves. They do
     * rely on it, but they expect us to deal with it, and so we shall.
     */
    if (!get_first_committed())
      put_first_committed(t, ver);
  }
  /**
   * Put the contents of @p bl into version @p ver
   *
   * @param t A transaction to which we will add this put operation
   * @param ver The version to which we will add the value
   * @param bl A bufferlist containing the version's value
   */
  void put_version(MonitorDBStore::TransactionRef t, version_t ver,
		   bufferlist& bl) {
    t->put(get_service_name(), ver, bl);
  }
  /**
   * Put the contents of @p bl into a full version key for this service, that
   * will be created with @p ver in mind.
   *
   * @param t The transaction to which we will add this put operation
   * @param ver A version number
   * @param bl A bufferlist containing the version's value
   */
  void put_version_full(MonitorDBStore::TransactionRef t,
			version_t ver, bufferlist& bl) {
    string key = mon->store->combine_strings(full_prefix_name, ver);
    t->put(get_service_name(), key, bl);
  }
  /**
   * Put the version number in @p ver into the key pointing to the latest full
   * version of this service.
   *
   * @param t The transaction to which we will add this put operation
   * @param ver A version number
   */
  void put_version_latest_full(MonitorDBStore::TransactionRef t, version_t ver) {
    string key = mon->store->combine_strings(full_prefix_name, full_latest_name);
    t->put(get_service_name(), key, ver);
  }
  /**
   * Put the contents of @p bl into the key @p key.
   *
   * @param t A transaction to which we will add this put operation
   * @param key The key to which we will add the value
   * @param bl A bufferlist containing the value
   */
  void put_value(MonitorDBStore::TransactionRef t,
		 const string& key, bufferlist& bl) {
    t->put(get_service_name(), key, bl);
  }

  /**
   * @}
   */

  /**
   * @defgroup PaxosService_h_store_get Wrapper function interface to access
   *					the back store for reading purposes
   * @{
   */

  /**
   * @defgroup PaxosService_h_version_cache Obtain cached versions for this
   *                                        service.
   * @{
   */
  /**
   * Get the first committed version
   *
   * @returns Our first committed version (that is available)
   */
  version_t get_first_committed() {
    return cached_first_committed;
  }
  /**
   * Get the last committed version
   *
   * @returns Our last committed version
   */
  version_t get_last_committed() {
    return cached_last_committed;
  }

  /**
   * @}
   */

  /**
   * Get the contents of a given version @p ver
   *
   * @param ver The version being obtained
   * @param bl The bufferlist to be populated
   * @return 0 on success; <0 otherwise
   */
  int get_version(version_t ver, bufferlist& bl) {
    return mon->store->get(get_service_name(), ver, bl);
  }
  /**
   * Get the contents of a given full version of this service.
   *
   * @param ver A version number
   * @param bl The bufferlist to be populated
   * @returns 0 on success; <0 otherwise
   */
  int get_version_full(version_t ver, bufferlist& bl) {
    string key = mon->store->combine_strings(full_prefix_name, ver);
    return mon->store->get(get_service_name(), key, bl);
  }
  /**
   * Get the latest full version number
   *
   * @returns A version number
   */
  version_t get_version_latest_full() {
    string key = mon->store->combine_strings(full_prefix_name, full_latest_name);
    return mon->store->get(get_service_name(), key);
  }

  /**
   * Get a value from a given key.
   *
   * @param[in] key The key
   * @param[out] bl The bufferlist to be populated with the value
   */
  int get_value(const string& key, bufferlist& bl) {
    return mon->store->get(get_service_name(), key, bl);
  }
  /**
   * Get an integer value from a given key.
   *
   * @param[in] key The key
   */
  version_t get_value(const string& key) {
    return mon->store->get(get_service_name(), key);
  }

  /**
   * @}
   */
  /**
   * @}
   */
};

#endif

