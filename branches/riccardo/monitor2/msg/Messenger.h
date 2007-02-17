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



class MDS;
class Timer;

class Messenger {
 private:
  Dispatcher          *dispatcher;
  entity_name_t           _myname;

 public:
  Messenger(entity_name_t w) : dispatcher(0), _myname(w) { }
  virtual ~Messenger() { }
  
  // accessors
  entity_name_t get_myname() { return _myname; }
  void _set_myname(entity_name_t m) { _myname = m; }

  virtual void reset_myname(entity_name_t m) = 0;

  virtual const entity_addr_t &get_myaddr() = 0;

  entity_inst_t get_myinst() { return entity_inst_t(_myname, get_myaddr()); }
  
  // hrmpf.
  virtual int get_dispatch_queue_len() { return 0; };

  // setup
  void set_dispatcher(Dispatcher *d) { dispatcher = d; ready(); }
  Dispatcher *get_dispatcher() { return dispatcher; }
  virtual void ready() { }
  bool is_ready() { return dispatcher != 0; }

  // dispatch incoming messages
  virtual void dispatch(Message *m) {
    assert(dispatcher);
    dispatcher->dispatch(m);
  }

  // shutdown
  virtual int shutdown() = 0;

  // send message
  virtual void prepare_dest(const entity_addr_t& addr) {}
  virtual int send_message(Message *m, entity_inst_t dest,
			   int port=0, int fromport=0) = 0;

  // make a procedure call
  //virtual Message* sendrecv(Message *m, msg_name_t dest, int port=0);

  virtual void mark_down(entity_addr_t a) {}

};





#endif
