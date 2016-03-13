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
#include <string>
#include "QueueStrategy.h"
#define dout_subsys ceph_subsys_ms
#include "common/debug.h"

QueueStrategy::QueueStrategy(int _n_threads)
  : lock("QueueStrategy::lock"),
    n_threads(_n_threads),
    stop(false),
    mqueue(),
    disp_threads()
{
}

void QueueStrategy::ds_dispatch(Message *m) {
  msgr->ms_fast_preprocess(m);
  if (msgr->ms_can_fast_dispatch(m)) {
    msgr->ms_fast_dispatch(m);
    return;
  }
  lock.Lock();
  mqueue.push_back(*m);
  if (disp_threads.size()) {
    if (! disp_threads.empty()) {
      QSThread *thrd = &disp_threads.front();
      disp_threads.pop_front();
      thrd->cond.Signal();
    }
  }
  lock.Unlock();
}

void QueueStrategy::entry(QSThread *thrd)
{
  Message *m = NULL;
  for (;;) {
    lock.Lock();
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
      thrd->cond.Wait(lock);
    }
    lock.Unlock();
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
  lock.Lock();
  stop = true;
  while (disp_threads.size()) {
    thrd = &(disp_threads.front());
    disp_threads.pop_front();
    thrd->cond.Signal();
  }
  lock.Unlock();
}

void QueueStrategy::wait()
{
  QSThread *thrd;
  lock.Lock();
  assert(stop);
  while (disp_threads.size()) {
    thrd = &(disp_threads.front());
    disp_threads.pop_front();
    lock.Unlock();

    // join outside of lock
    thrd->join();

    lock.Lock();
  }
  lock.Unlock();
}

void QueueStrategy::start()
{
  QSThread *thrd;
  assert(!stop);
  lock.Lock();
  for (int ix = 0; ix < n_threads; ++ix) {
    string thread_name = "ms_xio_qs_";
    thread_name.append(std::to_string(ix));
    thrd = new QSThread(this);
    thrd->create(thread_name.c_str());
  }
  lock.Unlock();
}
