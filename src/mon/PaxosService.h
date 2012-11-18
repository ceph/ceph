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
#include <errno.h>

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
  
protected:
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
      if (r == -ECANCELED) {
	m->put();
	return;
      }
      svc->dispatch(m);
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
      if (r == -ECANCELED)
	return;
      ps->proposal_timer = 0;
      ps->propose_pending(); 
    }
  };
  /**
   * @}
   */
  friend class C_Propose;
  

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

public:
  /**
   * @param mn A Monitor instance
   * @param p A Paxos instance
   */
  PaxosService(Monitor *mn, Paxos *p) : mon(mn), paxos(p),
					proposal_timer(0),
					have_pending(false) { }
  virtual ~PaxosService() {}

  /**
   * Get the machine name.
   *
   * @returns The machine name.
   */
  const char *get_machine_name();
  
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
   * Dispatch a message by passing it to several different functions that are
   * either implemented directly by this service, or that should be implemented
   * by the class implementing this service.
   *
   * @param m A message
   * @returns 'true' on successful dispatch; 'false' otherwise.
   */
  bool dispatch(PaxosServiceMessage *m);

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
  virtual void update_from_paxos() = 0;

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
   * @param[out] bl A bufferlist containing the encoded pending state
   */
  virtual void encode_pending(bufferlist& bl) = 0;

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
   * @remarks It's a courtesy method, in case the class implementing this 
   *	      service has anything it wants/needs to do at that time.
   *
   * @note This function may get called twice in certain recovery cases.
   */
  virtual void on_active() { }

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

  /**
   * @}
   */
};

#endif

