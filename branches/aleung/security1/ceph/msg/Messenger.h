// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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


typedef __uint64_t lamport_t;


class MDS;
class Timer;

class Messenger {
 private:
  Dispatcher          *dispatcher;
  msg_addr_t           _myaddr;


 public:
  Messenger(msg_addr_t w) : dispatcher(0), _myaddr(w) { }
  virtual ~Messenger() { }
  
  void       set_myaddr(msg_addr_t m) { _myaddr = m; }
  msg_addr_t get_myaddr() { return _myaddr; }


  virtual int shutdown() = 0;
  
  // callbacks
  static void do_callbacks();

  void queue_callback(Context *c);
  void queue_callbacks(list<Context*>& ls);
  virtual void callback_kick() = 0;

  virtual int get_dispatch_queue_len() { return 0; };

  // setup
  void set_dispatcher(Dispatcher *d) { dispatcher = d; ready(); }
  Dispatcher *get_dispatcher() { return dispatcher; }
  virtual void ready() { }
  bool is_ready() { return dispatcher != 0; }

  // dispatch incoming messages
  virtual void dispatch(Message *m);

  // send message
  virtual void prepare_dest(const entity_inst_t& inst) {}
  virtual int send_message(Message *m, msg_addr_t dest, int port=0, int fromport=0) = 0;
  virtual int send_message(Message *m, msg_addr_t dest, const entity_inst_t& inst,
			   int port=0, int fromport=0) {
    return send_message(m, dest, port, fromport);   // overload me!
  }


  // make a procedure call
  //virtual Message* sendrecv(Message *m, msg_addr_t dest, int port=0);


  virtual void mark_down(msg_addr_t a, entity_inst_t& i) {}
  virtual void mark_up(msg_addr_t a, entity_inst_t& i) {}
  //virtual void reset(msg_addr_t a) { mark_down(a); mark_up(a); }

};





#endif
