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



#ifndef __FAKEMESSENGER_H
#define __FAKEMESSENGER_H

#include "Messenger.h"
#include "Dispatcher.h"

#include <list>
#include <map>

class Timer;

class FakeMessenger : public Messenger {
 protected:
  class Logger *logger;

  int    qlen;
  list<Message*>       incoming;        // incoming queue

  entity_inst_t _myinst;

 public:
  FakeMessenger(entity_name_t me);
  ~FakeMessenger();

  virtual int shutdown();

  const entity_inst_t& get_myinst() {
    return _myinst;
  };
  const entity_addr_t& get_myaddr() {
    return _myinst.addr;
  }

  void reset_myname(entity_name_t m);

  // msg interface
  virtual int send_message(Message *m, entity_inst_t dest, int port=0, int fromport=0);
  
  // events
  //virtual void trigger_timer(Timer *t);

  int get_dispatch_queue_len() { return qlen; }

  // -- incoming queue --
  // (that nothing uses)
  Message *get_message() {
    if (!incoming.empty()) {
      Message *m = incoming.front();
      incoming.pop_front();
      qlen--;
      return m;
    }
    return NULL;
  }
  bool queue_incoming(Message *m) {
    incoming.push_back(m);
    qlen++;
    return true;
  }
  int num_incoming() {
    //return incoming.size();
    return qlen;
  }

};

int fakemessenger_do_loop();
int fakemessenger_do_loop_2();
void fakemessenger_startthread();
void fakemessenger_stopthread();
void fakemessenger_wait();

#endif
