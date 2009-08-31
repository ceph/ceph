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



#ifndef __MESSENGER_H
#define __MESSENGER_H

#include <map>
using namespace std;

#include "Message.h"
#include "Dispatcher.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "include/Context.h"
#include "include/types.h"


class MDS;
class Timer;

class Messenger {
 private:
  Dispatcher          *dispatcher;

protected:
  entity_inst_t _myinst;
  int default_send_priority;

  atomic_t nref;

 public:
  Messenger(entity_name_t w) : dispatcher(0),
			       default_send_priority(CEPH_MSG_PRIO_DEFAULT),
			       nref(1) {
    _myinst.name = w;
  }
  virtual ~Messenger() {
    assert(nref.test() == 0);
  }

  void get() {
    nref.inc();
  }
  void put() {
    if (nref.dec() == 0)
      delete this;
  }
  virtual void destroy() {
    put();
  }
  
  // accessors
  entity_name_t get_myname() { return _myinst.name; }
  const entity_addr_t& get_myaddr() { return _myinst.addr; }
  const entity_inst_t& get_myinst() { return _myinst; }
  
  void _set_myname(entity_name_t m) { _myinst.name = m; }
  virtual void _set_myaddr(entity_addr_t a) { _myinst.addr = a; }
  virtual void reset_myname(entity_name_t m) = 0;

  void set_default_send_priority(int p) { default_send_priority = p; }
  int get_default_send_priority() { return default_send_priority; }
  
  // hrmpf.
  virtual int get_dispatch_queue_len() { return 0; };

  // setup
  void set_dispatcher(Dispatcher *d) { 
    if (!dispatcher) {
      dispatcher = d; 
      ready(); 
    }
  }
  Dispatcher *get_dispatcher() { return dispatcher; }
  virtual void ready() { }
  bool is_ready() { return dispatcher != 0; }

  // dispatch incoming messages
  virtual void dispatch(Message *m) {
    assert(dispatcher);
    dispatcher->ms_deliver_dispatch(m);
  }

  // shutdown
  virtual int shutdown() = 0;
  virtual void suicide() = 0;

  // send message
  virtual void prepare_dest(const entity_inst_t& inst) {}
  virtual int send_message(Message *m, entity_inst_t dest) = 0;
  virtual int forward_message(Message *m, entity_inst_t dest) = 0;
  virtual int lazy_send_message(Message *m, entity_inst_t dest) {
    return send_message(m, dest);
  }
  virtual int send_keepalive(entity_inst_t dest) = 0;

  virtual void mark_down(entity_addr_t a) {}

};





#endif
