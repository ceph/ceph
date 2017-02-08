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
#include "QueueStrategy.h"
#define dout_subsys ceph_subsys_ms
#include "common/debug.h"

QueueStrategy::QueueStrategy(int _n_threads)
  : n_threads(_n_threads),
    stop(false),
    mqueue(),
    disp_threads()
{
}

void QueueStrategy::ds_dispatch(Message *m) {
  QSThread *thrd;
  std::lock_guard<std::mutex> l(lock);
//  m->trace.event("QueueStrategy::ds_dispatch");
  mqueue.push_back(*m);
  if (disp_threads.size()) {
    if (! disp_threads.empty()) {
      thrd = &(disp_threads.front());
      disp_threads.pop_front();
      thrd->cond.notify_all();
    }
  }
}

void QueueStrategy::entry(QSThread *thrd)
{
  Message *m = NULL;
  for (;;) {
    std::unique_lock<std::mutex> l(lock);
    for (;;) {
      if (! mqueue.empty()) {
	m = &(mqueue.front());
	mqueue.pop_front();
	break;
      }
      m = NULL;
      if (stop)
	break;
      disp_threads.push_front(*thrd);
      thrd->cond.wait(l);
    }
    l.unlock();
    if (stop) {
	if (!m) break;
	m->put();
	continue;
    }
    get_messenger()->ms_deliver_dispatch(m);
  }
}

void QueueStrategy::shutdown()
{
  QSThread *thrd;
  std::lock_guard<std::mutex> l(lock);
  stop = true;
  while (disp_threads.size()) {
    thrd = &(disp_threads.front());
    disp_threads.pop_front();
    thrd->cond.notify_all();
  }
}

void QueueStrategy::wait()
{
  QSThread *thrd;
  std::unique_lock<std::mutex> l(lock);
  assert(stop);
  while (disp_threads.size()) {
    thrd = &(disp_threads.front());
    disp_threads.pop_front();
    l.unlock();

    // join outside of lock
    thrd->join();

    l.lock();
  }
  l.unlock();
}

void QueueStrategy::start()
{
  QSThread *thrd;
  assert(!stop);
  std::lock_guard<std::mutex> l(lock);
  for (int ix = 0; ix < n_threads; ++ix) {
    thrd = new QSThread(this);
    thrd->create("qs");
  }
}
