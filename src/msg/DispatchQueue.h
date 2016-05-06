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
#include "common/PrioritizedQueue.h"

class CephContext;
class DispatchQueue;
class Pipe;
class Messenger;
class Message;
struct Connection;

/**
 * The DispatchQueue contains all the Pipes which have Messages
 * they want to be dispatched, carefully organized by Message priority
 * and permitted to deliver in a round-robin fashion.
 * See Messenger::dispatch_entry for details.
 */
class DispatchQueue {
  class QueueItem {
    int type;
    ConnectionRef con;
    MessageRef m;
  public:
    explicit QueueItem(Message *m) : type(-1), con(0), m(m) {}
    QueueItem(int type, Connection *con) : type(type), con(con), m(0) {}
    bool is_code() const {
      return type != -1;
    }
    int get_code () const {
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
  Messenger *msgr;
  mutable Mutex lock;
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
    explicit DispatchThread(DispatchQueue *dq) : dq(dq) {}
    void *entry() {
      dq->entry();
      return 0;
    }
  } dispatch_thread;

  Mutex local_delivery_lock;
  Cond local_delivery_cond;
  bool stop_local_delivery;
  list<pair<Message *, int> > local_messages;
  class LocalDeliveryThread : public Thread {
    DispatchQueue *dq;
  public:
    explicit LocalDeliveryThread(DispatchQueue *dq) : dq(dq) {}
    void *entry() {
      dq->run_local_delivery();
      return 0;
    }
  } local_delivery_thread;

  uint64_t pre_dispatch(Message *m);
  void post_dispatch(Message *m, uint64_t msize);

 public:

  /// Throttle preventing us from building up a big backlog waiting for dispatch
  Throttle dispatch_throttler;

  bool stop;
  void local_delivery(Message *m, int priority);
  void run_local_delivery();

  double get_max_age(utime_t now) const;

  int get_queue_len() const {
    Mutex::Locker l(lock);
    return mqueue.length();
  }

  /**
   * Release memory accounting back to the dispatch throttler.
   *
   * @param msize The amount of memory to release.
   */
  void dispatch_throttle_release(uint64_t msize);

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

  bool can_fast_dispatch(Message *m) const;
  void fast_dispatch(Message *m);
  void fast_preprocess(Message *m);
  void enqueue(Message *m, int priority, uint64_t id);
  void discard_queue(uint64_t id);
  void discard_local();
  uint64_t get_id() {
    Mutex::Locker l(lock);
    return next_pipe_id++;
  }
  void start();
  void entry();
  void wait();
  void shutdown();
  bool is_started() const {return dispatch_thread.is_started();}

  DispatchQueue(CephContext *cct, Messenger *msgr, string &name)
    : cct(cct), msgr(msgr),
      lock("Messenger::DispatchQueue::lock" + name),
      mqueue(cct->_conf->ms_pq_max_tokens_per_priority,
	     cct->_conf->ms_pq_min_cost),
      next_pipe_id(1),
      dispatch_thread(this),
      local_delivery_lock("Messenger::DispatchQueue::local_delivery_lock" + name),
      stop_local_delivery(false),
      local_delivery_thread(this),
      dispatch_throttler(cct, string("msgr_dispatch_throttler-") + name,
                         cct->_conf->ms_dispatch_throttle_bytes),
      stop(false)
    {}
};

#endif
