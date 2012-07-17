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

#ifndef CEPH_DISPATCHQUEUE_H
#define CEPH_DISPATCHQUEUE_H

#include <map>
#include "include/xlist.h"
#include "include/atomic.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "common/RefCountedObj.h"

class CephContext;
class DispatchQueue;
class Pipe;
class SimpleMessenger;
class Message;
class Connection;

struct IncomingQueue : public RefCountedObject {
  CephContext *cct;
  DispatchQueue *dq;
  SimpleMessenger *msgr;
  void *parent;
  Mutex lock;
  map<int, list<Message*> > in_q; // and inbound ones
  int in_qlen;
  map<int, xlist<IncomingQueue *>::item* > queue_items; // protected by pipe_lock AND q.lock
  bool halt;

  void queue(Message *m, int priority, bool hold_dq_lock=false);
  void discard_queue();
  void restart_queue();

private:
  friend class DispatchQueue;
  IncomingQueue(CephContext *cct, DispatchQueue *dq, SimpleMessenger *msgr, void *parent)
    : cct(cct),
      dq(dq),
      msgr(msgr),
      parent(parent),
      lock("SimpleMessenger::IncomingQueue::lock"),
      in_qlen(0),
      halt(false)
  {
  }
  ~IncomingQueue() {
    for (map<int, xlist<IncomingQueue *>::item* >::iterator i = queue_items.begin();
	 i != queue_items.end();
	 ++i) {
      assert(!i->second->is_on_list());
      delete i->second;
    }
  }
};


/**
 * The DispatchQueue contains all the Pipes which have Messages
 * they want to be dispatched, carefully organized by Message priority
 * and permitted to deliver in a round-robin fashion.
 * See SimpleMessenger::dispatch_entry for details.
 */
struct DispatchQueue {
  CephContext *cct;
  SimpleMessenger *msgr;
  Mutex lock;
  Cond cond;
  bool stop;

  map<int, xlist<IncomingQueue *>* > queued_pipes;
  map<int, xlist<IncomingQueue *>::iterator> queued_pipe_iters;
  atomic_t qlen;
    
  enum { D_CONNECT = 1, D_ACCEPT, D_BAD_REMOTE_RESET, D_BAD_RESET, D_NUM_CODES };
  list<Connection*> con_q;

  IncomingQueue local_queue;

  /**
   * The DispatchThread runs dispatch_entry to empty out the dispatch_queue.
   */
  class DispatchThread : public Thread {
    DispatchQueue *dq;
  public:
    DispatchThread(DispatchQueue *dq) : dq(dq) {}
    void *entry() {
      dq->entry();
      return 0;
    }
  } dispatch_thread;

  void local_delivery(Message *m, int priority);

  IncomingQueue *create_queue(Pipe *parent) {
    return new IncomingQueue(cct, this, msgr, parent);
  }

  int get_queue_len() {
    return qlen.read();
  }
    
  void queue_connect(Connection *con) {
    lock.Lock();
    if (stop) {
      lock.Unlock();
      return;
    }
    con_q.push_back(con->get());
    local_queue.queue((Message*)D_CONNECT, CEPH_MSG_PRIO_HIGHEST, true);
    lock.Unlock();
  }
  void queue_accept(Connection *con) {
    lock.Lock();
    if (stop) {
      lock.Unlock();
      return;
    }
    con_q.push_back(con->get());
    local_queue.queue((Message*)D_ACCEPT, CEPH_MSG_PRIO_HIGHEST, true);
    lock.Unlock();
  }
  void queue_remote_reset(Connection *con) {
    lock.Lock();
    if (stop) {
      lock.Unlock();
      return;
    }
    con_q.push_back(con->get());
    local_queue.queue((Message*)D_BAD_REMOTE_RESET, CEPH_MSG_PRIO_HIGHEST, true);
    lock.Unlock();
  }
  void queue_reset(Connection *con) {
    lock.Lock();
    if (stop) {
      lock.Unlock();
      return;
    }
    con_q.push_back(con->get());
    local_queue.queue((Message*)D_BAD_RESET, CEPH_MSG_PRIO_HIGHEST, true);
    lock.Unlock();
  }

  void start();
  void entry();
  void wait();
  void shutdown();

  DispatchQueue(CephContext *cct, SimpleMessenger *msgr)
    : cct(cct), msgr(msgr),
      lock("SimpleMessenger::DispatchQeueu::lock"), 
      stop(false),
      qlen(0),
      local_queue(cct, this, msgr, NULL),
      dispatch_thread(this)
  {}
  ~DispatchQueue() {
    for (map< int, xlist<IncomingQueue *>* >::iterator i = queued_pipes.begin();
	 i != queued_pipes.end();
	 ++i) {
      i->second->clear();
      delete i->second;
    }
  }
};

#endif
