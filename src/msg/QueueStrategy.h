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

#include <vector>
#include <memory>
#include <boost/intrusive/list.hpp>
#include "DispatchStrategy.h"
#include "msg/Messenger.h"

namespace bi = boost::intrusive;

class QueueStrategy : public DispatchStrategy {
  Mutex lock;
  const int n_threads;
  bool stop;

  Message::Queue mqueue;

  class QSThread : public Thread {
  public:
    bi::list_member_hook<> thread_q;
    QueueStrategy *dq;
    Cond cond;
    explicit QSThread(QueueStrategy *dq) : thread_q(), dq(dq), cond() {}
    void* entry() {
      dq->entry(this);
      return NULL;
    }

    typedef bi::list< QSThread,
		      bi::member_hook< QSThread,
				       bi::list_member_hook<>,
				       &QSThread::thread_q > > Queue;
  };

  std::vector<std::unique_ptr<QSThread>> threads; //< all threads
  QSThread::Queue disp_threads; //< waiting threads

public:
  explicit QueueStrategy(int n_threads);
  void ds_dispatch(Message *m) override;
  void shutdown() override;
  void start() override;
  void wait() override;
  void entry(QSThread *thrd);
  virtual ~QueueStrategy() {}
};
#endif /* QUEUE_STRATEGY_H */
