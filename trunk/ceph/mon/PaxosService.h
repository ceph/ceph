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
protected:
  Monitor *mon;
  Paxos *paxos;

  
  class C_RetryMessage : public Context {
    Dispatcher *svc;
    Message *m;
  public:
    C_RetryMessage(Dispatcher *s, Message *m_) : svc(s), m(m_) {}
    void finish(int r) {
      svc->dispatch(m);
    }
  };

public:
  PaxosService(Monitor *mn, Paxos *p) : mon(mn), paxos(p) { }
  
  // i implement
  void dispatch(Message *m);
  void election_finished();

  // you implement
  virtual void create_initial() = 0;
  virtual bool update_from_paxos() = 0;
  virtual void prepare_pending() = 0;
  virtual void propose_pending() = 0;

  virtual bool preprocess_update(Message *m) = 0;  // true if processed.
  virtual void prepare_update(Message *m)= 0;

  virtual void tick() {};  // check state, take actions


};

#endif

