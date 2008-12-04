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

#ifndef __PAXOSSERVICE_H
#define __PAXOSSERVICE_H

#include "msg/Dispatcher.h"
#include "include/Context.h"

class Monitor;
class Paxos;

class PaxosService : public Dispatcher {
public:
  Monitor *mon;
  Paxos *paxos;
  
protected:
  class C_RetryMessage : public Context {
    PaxosService *svc;
    Message *m;
  public:
    C_RetryMessage(PaxosService *s, Message *m_) : svc(s), m(m_) {}
    void finish(int r) {
      svc->dispatch(m);
    }
  };
  class C_Active : public Context {
    PaxosService *svc;
  public:
    C_Active(PaxosService *s) : svc(s) {}
    void finish(int r) {
      if (r >= 0) 
	svc->_active();
    }
  };
  class C_Commit : public Context {
    PaxosService *svc;
  public:
    C_Commit(PaxosService *s) : svc(s) {}
    void finish(int r) {
      if (r >= 0)
	svc->_commit();
    }
  };
  friend class C_Update;

  class C_Propose : public Context {
    PaxosService *ps;
  public:
    C_Propose(PaxosService *p) : ps(p) { }
    void finish(int r) { 
      ps->proposal_timer = 0;
      ps->propose_pending(); 
    }
  };	
  friend class C_Propose;
  

private:
  Context *proposal_timer;
  bool have_pending;
  bool dispatch_impl(Message *m);

public:
  PaxosService(Monitor *mn, Paxos *p) : mon(mn), paxos(p),
					proposal_timer(0),
					have_pending(false) { }

  const char *get_machine_name();
  
  // i implement and you ignore
  void election_starting();
  void election_finished();
  void shutdown();

private:
  void _active();
  void _commit();

public:
  // i implement and you use
  void propose_pending();     // propose current pending as new paxos state

  // you implement
  virtual void create_initial() = 0;
  virtual bool update_from_paxos() = 0;    // assimilate latest state from paxos
  virtual void create_pending() = 0;       // [leader] create new pending structures
  virtual void encode_pending(bufferlist& bl) = 0; // [leader] finish and encode pending for next paxos state
  virtual void discard_pending() { }       // [leader] discard pending

  virtual bool preprocess_query(Message *m) = 0;  // true if processed (e.g., read-only)
  virtual bool prepare_update(Message *m) = 0;
  virtual bool should_propose(double &delay);

  virtual void committed() = 0;            // [leader] called after a proposed value commits

  virtual void tick() {}
  
};

#endif

