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



#ifndef CEPH_MESSENGER_H
#define CEPH_MESSENGER_H

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
  list<Dispatcher*> dispatchers;

protected:
  entity_name_t _my_name;
  int default_send_priority;

  atomic_t nref;

 public:
  Messenger(entity_name_t w) : default_send_priority(CEPH_MSG_PRIO_DEFAULT),
			       nref(1) {
    _my_name = w;
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
  entity_name_t get_myname() { return _my_name; }
  virtual entity_addr_t get_myaddr() = 0;
  entity_inst_t get_myinst() { return entity_inst_t(get_myname(), get_myaddr()); }
  
  void set_myname(const entity_name_t m) { _my_name = m; }

  void set_default_send_priority(int p) { default_send_priority = p; }
  int get_default_send_priority() { return default_send_priority; }
  
  // hrmpf.
  virtual int get_dispatch_queue_len() { return 0; };

  // setup
  void add_dispatcher_head(Dispatcher *d) { 
    bool first = dispatchers.empty();
    dispatchers.push_front(d);
    if (first)
      ready();
  }
  void add_dispatcher_tail(Dispatcher *d) { 
    bool first = dispatchers.empty();
    dispatchers.push_back(d);
    if (first)
      ready();
  }

  virtual void ready() { }
  bool is_ready() { return !dispatchers.empty(); }

  // dispatch incoming messages
  void ms_deliver_dispatch(Message *m) {
    for (list<Dispatcher*>::iterator p = dispatchers.begin();
	 p != dispatchers.end();
	 p++)
      if ((*p)->ms_dispatch(m))
	return;
    generic_dout(0) << "unhandled message " << m << " " << *m
		    << " from " << m->get_source_inst()
		    << dendl;
    assert(0);
  }
  void ms_deliver_handle_connect(Connection *con) {
    for (list<Dispatcher*>::iterator p = dispatchers.begin();
	 p != dispatchers.end();
	 p++)
      (*p)->ms_handle_connect(con);
  }
  void ms_deliver_handle_reset(Connection *con) {
    for (list<Dispatcher*>::iterator p = dispatchers.begin();
	 p != dispatchers.end();
	 p++)
      if ((*p)->ms_handle_reset(con))
	return;
  }
  void ms_deliver_handle_remote_reset(Connection *con) {
    for (list<Dispatcher*>::iterator p = dispatchers.begin();
	 p != dispatchers.end();
	 p++)
      (*p)->ms_handle_remote_reset(con);
  }

  AuthAuthorizer *ms_deliver_get_authorizer(int peer_type, bool force_new) {
    AuthAuthorizer *a = 0;
    for (list<Dispatcher*>::iterator p = dispatchers.begin();
	 p != dispatchers.end();
	 p++)
      if ((*p)->ms_get_authorizer(peer_type, &a, force_new))
	return a;
    return NULL;
  }
  bool ms_deliver_verify_authorizer(Connection *con, int peer_type,
				    int protocol, bufferlist& authorizer, bufferlist& authorizer_reply,
				    bool& isvalid) {
    for (list<Dispatcher*>::iterator p = dispatchers.begin();
	 p != dispatchers.end();
	 p++)
      if ((*p)->ms_verify_authorizer(con, peer_type, protocol, authorizer, authorizer_reply, isvalid))
	return true;
    return false;
  }

  // shutdown
  virtual int shutdown() = 0;
  virtual void suicide() = 0;

  // send message
  virtual void prepare_dest(const entity_inst_t& inst) {}
  virtual int send_message(Message *m, const entity_inst_t& dest) = 0;
  virtual int send_message(Message *m, Connection *con) = 0;
  virtual int lazy_send_message(Message *m, const entity_inst_t& dest) {
    return send_message(m, dest);
  }
  virtual int lazy_send_message(Message *m, Connection *con) = 0;
  virtual int send_keepalive(const entity_inst_t& dest) = 0;

  virtual void mark_down(const entity_addr_t& a) = 0;

  virtual Connection *get_connection(const entity_inst_t& dest) = 0;

  virtual int rebind(int avoid_port) { return -EOPNOTSUPP; }

protected:
  //destruction should be handled via destroy()
  virtual ~Messenger() {
    assert(nref.read() == 0);
  }
};





#endif
