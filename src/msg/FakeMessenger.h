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



#ifndef CEPH_FAKEMESSENGER_H
#define CEPH_FAKEMESSENGER_H

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

 public:
  bool failed;

  FakeMessenger(entity_name_t me);
  ~FakeMessenger();

  virtual int shutdown();

  void reset_myname(entity_name_t m);

  // msg interface
  int send_message(Message *m, entity_inst_t dest);
  int forward_message(Message *m, entity_inst_t dest);
  int submit_message(Message *m, entity_inst_t dest);
  
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

  void suicide() {
    if (!failed) {
      failed = true;
    }
    shutdown();
  }

};

int fakemessenger_do_loop();
int fakemessenger_do_loop_2();
void fakemessenger_startthread();
void fakemessenger_stopthread();
void fakemessenger_wait();

#endif
