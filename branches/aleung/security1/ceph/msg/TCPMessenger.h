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


#ifndef __TCPMESSENGER_H
#define __TCPMESSENGER_H

#include "Messenger.h"
#include "Dispatcher.h"
#include "common/Thread.h"

#include "tcp.h"

class Timer;


class TCPMessenger : public Messenger {
 protected:

  //class Logger *logger;  // for logging
  
  bool           incoming_stop;
  Mutex          incoming_lock;
  list<Message*> incoming;
  Cond           incoming_cond;

  class DispatchThread : public Thread {
    TCPMessenger *m;
  public:
    DispatchThread(TCPMessenger *_m) : m(_m) {}
    void *entry() {
      m->dispatch_entry();
      return 0;
    }
  } dispatch_thread;

  void dispatch_entry();

public:
  void dispatch_start() {
    incoming_stop = false;
    dispatch_thread.create();
  }
  /*  void dispatch_kick() {
    incoming_lock.Lock();
    incoming_cond.Signal();
    incoming_lock.Unlock();
    }*/
  void dispatch_stop() {
    incoming_lock.Lock();
    incoming_stop = true;
    incoming_cond.Signal();
    incoming_lock.Unlock();
    dispatch_thread.join();
  }
  void dispatch_queue(Message *m) {
    incoming_lock.Lock();
    incoming.push_back(m);
    incoming_cond.Signal();
    incoming_lock.Unlock();
  }

 public:
  TCPMessenger(entity_name_t myaddr);
  ~TCPMessenger();

  void ready();

  tcpaddr_t& get_tcpaddr();
  void map_entity_rank(entity_name_t e, int r);
  void map_rank_addr(int r, tcpaddr_t a);

  int get_dispatch_queue_len();

  void callback_kick();

  // init, shutdown MPI and associated event loop thread.
  virtual int shutdown();

  // message interface
  virtual int send_message(Message *m, entity_name_t dest, int port=0, int fromport=0);
};

/**
 * these are all ONE per process.
 */

extern int tcpmessenger_lookup(char *str, tcpaddr_t& ta);

extern int tcpmessenger_findns(tcpaddr_t &nsa);

extern int tcpmessenger_init();
extern int tcpmessenger_start();   // start thread
extern void tcpmessenger_wait();    // wait for thread to finish.
extern int tcpmessenger_shutdown();   // finalize MPI

extern void tcpmessenger_start_nameserver(tcpaddr_t& ta);  // on rank 0
extern void tcpmessenger_stop_nameserver();   // on rank 0
extern void tcpmessenger_start_rankserver(tcpaddr_t& ta);  // on all ranks
extern void tcpmessenger_stop_rankserver();   // on all ranks

extern int tcpmessenger_get_rank();


#endif
