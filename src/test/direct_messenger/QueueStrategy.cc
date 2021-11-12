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
  : n_threads(_n_threads),
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
  std::lock_guard l{lock};
  mqueue.push_back(*m);
  if (disp_threads.size()) {
    if (! disp_threads.empty()) {
      QSThread *thrd = &disp_threads.front();
      disp_threads.pop_front();
      thrd->cond.notify_all();
    }
  }
}

void QueueStrategy::entry(QSThread *thrd)
{
  for (;;) {
    ceph::ref_t<Message> m;
    std::unique_lock l{lock};
    for (;;) {
      if (! mqueue.empty()) {
	m = ceph::ref_t<Message>(&mqueue.front(), false);
	mqueue.pop_front();
	break;
      }
      if (stop)
	break;
      disp_threads.push_front(*thrd);
      thrd->cond.wait(l);
    }
    l.unlock();
    if (stop) {
	if (!m) break;
	continue;
    }
    get_messenger()->ms_deliver_dispatch(m);
  }
}

void QueueStrategy::shutdown()
{
  QSThread *thrd;
  std::lock_guard l{lock};
  stop = true;
  while (disp_threads.size()) {
    thrd = &(disp_threads.front());
    disp_threads.pop_front();
    thrd->cond.notify_all();
  }
}

void QueueStrategy::wait()
{
  std::unique_lock l{lock};
  ceph_assert(stop);
  for (auto& thread : threads) {
    l.unlock();

    // join outside of lock
    thread->join();

    l.lock();
  }
}

void QueueStrategy::start()
{
  ceph_assert(!stop);
  std::lock_guard l{lock};
  threads.reserve(n_threads);
  for (int ix = 0; ix < n_threads; ++ix) {
    std::string thread_name = "ms_qs_";
    thread_name.append(std::to_string(ix));
    auto thrd = std::make_unique<QSThread>(this);
    thrd->create(thread_name.c_str());
    threads.emplace_back(std::move(thrd));
  }
}
