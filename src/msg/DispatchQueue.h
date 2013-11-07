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
#include <boost/intrusive_ptr.hpp>
#include "include/assert.h"
#include "include/xlist.h"
#include "include/atomic.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "common/RefCountedObj.h"
#include "common/PrioritizedQueue.h"

class CephContext;
class DispatchQueue;
class Pipe;
class SimpleMessenger;
class Message;
struct Connection;

/**
 * The DispatchQueue contains all the Pipes which have Messages
 * they want to be dispatched, carefully organized by Message priority
 * and permitted to deliver in a round-robin fashion.
 * See SimpleMessenger::dispatch_entry for details.
 */
class DispatchQueue {
  class QueueItem {
    int type;
    ConnectionRef con;
    MessageRef m;
  public:
    QueueItem(Message *m) : type(-1), con(0), m(m) {}
    QueueItem(int type, Connection *con) : type(type), con(con), m(0) {}
    bool is_code() const {
      return type != -1;
    }
    int get_code () {
      assert(is_code());
      return type;
    }
    Message *get_message() {
      assert(!is_code());
      return m.get();
    }
    Connection *get_connection() {
      assert(is_code());
      return con.get();
    }
  };
    
  CephContext *cct;
  SimpleMessenger *msgr;
  Mutex lock;
  Cond cond;

  PrioritizedQueue<QueueItem, uint64_t> mqueue;

  set<pair<double, Message*> > marrival;
  map<Message *, set<pair<double, Message*> >::iterator> marrival_map;
  void add_arrival(Message *m) {
    marrival_map.insert(
      make_pair(
	m,
	marrival.insert(make_pair(m->get_recv_stamp(), m)).first
	)
      );
  }
  void remove_arrival(Message *m) {
    map<Message *, set<pair<double, Message*> >::iterator>::iterator i =
      marrival_map.find(m);
    assert(i != marrival_map.end());
    marrival.erase(i->second);
    marrival_map.erase(i);
  }

  uint64_t next_pipe_id;
    
  enum { D_CONNECT = 1, D_ACCEPT, D_BAD_REMOTE_RESET, D_BAD_RESET, D_NUM_CODES };

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

  uint64_t pre_dispatch(Message *m);
  void post_dispatch(Message *m, uint64_t msize);

  public:
  bool stop;
  void local_delivery(Message *m, int priority);

  double get_max_age(utime_t now);

  int get_queue_len() {
    Mutex::Locker l(lock);
    return mqueue.length();
  }
    
  void queue_connect(Connection *con) {
    Mutex::Locker l(lock);
    if (stop)
      return;
    mqueue.enqueue_strict(
      0,
      CEPH_MSG_PRIO_HIGHEST,
      QueueItem(D_CONNECT, con));
    cond.Signal();
  }
  void queue_accept(Connection *con) {
    Mutex::Locker l(lock);
    if (stop)
      return;
    mqueue.enqueue_strict(
      0,
      CEPH_MSG_PRIO_HIGHEST,
      QueueItem(D_ACCEPT, con));
    cond.Signal();
  }
  void queue_remote_reset(Connection *con) {
    Mutex::Locker l(lock);
    if (stop)
      return;
    mqueue.enqueue_strict(
      0,
      CEPH_MSG_PRIO_HIGHEST,
      QueueItem(D_BAD_REMOTE_RESET, con));
    cond.Signal();
  }
  void queue_reset(Connection *con) {
    Mutex::Locker l(lock);
    if (stop)
      return;
    mqueue.enqueue_strict(
      0,
      CEPH_MSG_PRIO_HIGHEST,
      QueueItem(D_BAD_RESET, con));
    cond.Signal();
  }

  void enqueue(Message *m, int priority, uint64_t id);
  void discard_queue(uint64_t id);
  uint64_t get_id() {
    Mutex::Locker l(lock);
    return next_pipe_id++;
  }
  void start();
  void entry();
  void wait();
  void shutdown();

  DispatchQueue(CephContext *cct, SimpleMessenger *msgr)
    : cct(cct), msgr(msgr),
      lock("SimpleMessenger::DispatchQeueu::lock"), 
      mqueue(cct->_conf->ms_pq_max_tokens_per_priority,
	     cct->_conf->ms_pq_min_cost),
      next_pipe_id(1),
      dispatch_thread(this),
      stop(false)
    {}
};

#endif
