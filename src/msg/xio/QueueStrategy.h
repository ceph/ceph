// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef QUEUE_STRATEGY_H
#define QUEUE_STRATEGY_H

#include <mutex>
#include <condition_variable>
#include <boost/intrusive/list.hpp>
#include "DispatchStrategy.h"
#include "msg/Messenger.h"

namespace bi = boost::intrusive;

class QueueStrategy : public DispatchStrategy {
  std::mutex lock;
  typedef std::lock_guard<std::mutex> lock_guard;
  typedef std::unique_lock<std::mutex> unique_lock;
  int n_threads;
  bool stop;

  Message::Queue mqueue;

  class QSThread : public Thread {
  public:
    bi::list_member_hook<> thread_q;
    QueueStrategy *dq;
    std::condition_variable cond;
    QSThread(QueueStrategy *dq) : thread_q(), dq(dq), cond() {}
    void* entry() {
      dq->entry(this);
      delete(this);
      return NULL;
    }

    typedef bi::list< QSThread,
		      bi::member_hook< QSThread,
				       bi::list_member_hook<>,
				       &QSThread::thread_q > > Queue;
  };

  QSThread::Queue disp_threads;

public:
  QueueStrategy(int n_threads);
  virtual void ds_dispatch(Message *m);
  virtual void shutdown();
  virtual void start();
  virtual void wait();
  void entry(QSThread *thrd);
  virtual ~QueueStrategy() {}
};
#endif /* QUEUE_STRATEGY_H */
