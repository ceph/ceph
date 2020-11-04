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

#include <atomic>
#include <map>
#include <queue>
#include <boost/intrusive_ptr.hpp>
#include "include/ceph_assert.h"
#include "include/common_fwd.h"
#include "common/Throttle.h"
#include "common/ceph_mutex.h"
#include "common/Thread.h"
#include "common/PrioritizedQueue.h"

#include "Message.h"

class Messenger;
struct Connection;

/**
 * The DispatchQueue contains all the connections which have Messages
 * they want to be dispatched, carefully organized by Message priority
 * and permitted to deliver in a round-robin fashion.
 * See Messenger::dispatch_entry for details.
 */
class DispatchQueue {
  class QueueItem {
    int type;
    ConnectionRef con;
    ceph::ref_t<Message> m;
  public:
    explicit QueueItem(const ceph::ref_t<Message>& m) : type(-1), con(0), m(m) {}
    QueueItem(int type, Connection *con) : type(type), con(con), m(0) {}
    bool is_code() const {
      return type != -1;
    }
    int get_code () const {
      ceph_assert(is_code());
      return type;
    }
    const ceph::ref_t<Message>& get_message() {
      ceph_assert(!is_code());
      return m;
    }
    Connection *get_connection() {
      ceph_assert(is_code());
      return con.get();
    }
  };

  CephContext *cct;
  Messenger *msgr;
  mutable ceph::mutex lock;
  ceph::condition_variable cond;

  PrioritizedQueue<QueueItem, uint64_t> mqueue;

  std::set<std::pair<double, ceph::ref_t<Message>>> marrival;
  std::map<ceph::ref_t<Message>, decltype(marrival)::iterator> marrival_map;
  void add_arrival(const ceph::ref_t<Message>& m) {
    marrival_map.insert(
      make_pair(
	m,
	marrival.insert(std::make_pair(m->get_recv_stamp(), m)).first
	)
      );
  }
  void remove_arrival(const ceph::ref_t<Message>& m) {
    auto it = marrival_map.find(m);
    ceph_assert(it != marrival_map.end());
    marrival.erase(it->second);
    marrival_map.erase(it);
  }

  std::atomic<uint64_t> next_id;

  enum { D_CONNECT = 1, D_ACCEPT, D_BAD_REMOTE_RESET, D_BAD_RESET, D_CONN_REFUSED, D_NUM_CODES };

  /**
   * The DispatchThread runs dispatch_entry to empty out the dispatch_queue.
   */
  class DispatchThread : public Thread {
    DispatchQueue *dq;
  public:
    explicit DispatchThread(DispatchQueue *dq) : dq(dq) {}
    void *entry() override {
      dq->entry();
      return 0;
    }
  } dispatch_thread;

  ceph::mutex local_delivery_lock;
  ceph::condition_variable local_delivery_cond;
  bool stop_local_delivery;
  std::queue<std::pair<ceph::ref_t<Message>, int>> local_messages;
  class LocalDeliveryThread : public Thread {
    DispatchQueue *dq;
  public:
    explicit LocalDeliveryThread(DispatchQueue *dq) : dq(dq) {}
    void *entry() override {
      dq->run_local_delivery();
      return 0;
    }
  } local_delivery_thread;

  uint64_t pre_dispatch(const ceph::ref_t<Message>& m);
  void post_dispatch(const ceph::ref_t<Message>& m, uint64_t msize);

 public:

  /// Throttle preventing us from building up a big backlog waiting for dispatch
  Throttle dispatch_throttler;

  bool stop;
  void local_delivery(const ceph::ref_t<Message>& m, int priority);
  void local_delivery(Message* m, int priority) {
    return local_delivery(ceph::ref_t<Message>(m, false), priority); /* consume ref */
  }
  void run_local_delivery();

  double get_max_age(utime_t now) const;

  int get_queue_len() const {
    std::lock_guard l{lock};
    return mqueue.length();
  }

  /**
   * Release memory accounting back to the dispatch throttler.
   *
   * @param msize The amount of memory to release.
   */
  void dispatch_throttle_release(uint64_t msize);

  void queue_connect(Connection *con) {
    std::lock_guard l{lock};
    if (stop)
      return;
    mqueue.enqueue_strict(
      0,
      CEPH_MSG_PRIO_HIGHEST,
      QueueItem(D_CONNECT, con));
    cond.notify_all();
  }
  void queue_accept(Connection *con) {
    std::lock_guard l{lock};
    if (stop)
      return;
    mqueue.enqueue_strict(
      0,
      CEPH_MSG_PRIO_HIGHEST,
      QueueItem(D_ACCEPT, con));
    cond.notify_all();
  }
  void queue_remote_reset(Connection *con) {
    std::lock_guard l{lock};
    if (stop)
      return;
    mqueue.enqueue_strict(
      0,
      CEPH_MSG_PRIO_HIGHEST,
      QueueItem(D_BAD_REMOTE_RESET, con));
    cond.notify_all();
  }
  void queue_reset(Connection *con) {
    std::lock_guard l{lock};
    if (stop)
      return;
    mqueue.enqueue_strict(
      0,
      CEPH_MSG_PRIO_HIGHEST,
      QueueItem(D_BAD_RESET, con));
    cond.notify_all();
  }
  void queue_refused(Connection *con) {
    std::lock_guard l{lock};
    if (stop)
      return;
    mqueue.enqueue_strict(
      0,
      CEPH_MSG_PRIO_HIGHEST,
      QueueItem(D_CONN_REFUSED, con));
    cond.notify_all();
  }

  bool can_fast_dispatch(const ceph::cref_t<Message> &m) const;
  void fast_dispatch(const ceph::ref_t<Message>& m);
  void fast_dispatch(Message* m) {
    return fast_dispatch(ceph::ref_t<Message>(m, false)); /* consume ref */
  }
  void fast_preprocess(const ceph::ref_t<Message>& m);
  void enqueue(const ceph::ref_t<Message>& m, int priority, uint64_t id);
  void enqueue(Message* m, int priority, uint64_t id) {
    return enqueue(ceph::ref_t<Message>(m, false), priority, id); /* consume ref */
  }
  void discard_queue(uint64_t id);
  void discard_local();
  uint64_t get_id() {
    return next_id++;
  }

  Messenger* get_messenger() const {
    return msgr;
  }

  void start();
  void entry();
  void wait();
  void shutdown();
  bool is_started() const {return dispatch_thread.is_started();}

  DispatchQueue(CephContext *cct, Messenger *msgr, std::string &name)
    : cct(cct), msgr(msgr),
      lock(ceph::make_mutex("Messenger::DispatchQueue::lock" + name)),
      mqueue(cct->_conf->ms_pq_max_tokens_per_priority,
	     cct->_conf->ms_pq_min_cost),
      next_id(1),
      dispatch_thread(this),
      local_delivery_lock(ceph::make_mutex("Messenger::DispatchQueue::local_delivery_lock" + name)),
      stop_local_delivery(false),
      local_delivery_thread(this),
      dispatch_throttler(cct, std::string("msgr_dispatch_throttler-") + name,
                         cct->_conf->ms_dispatch_throttle_bytes),
      stop(false)
    {}
  ~DispatchQueue() {
    ceph_assert(mqueue.empty());
    ceph_assert(marrival.empty());
    ceph_assert(local_messages.empty());
  }
};

#endif
