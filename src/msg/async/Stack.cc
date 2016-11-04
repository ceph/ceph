// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/Cond.h"
#include "common/errno.h"
#include "PosixStack.h"
#ifdef HAVE_RDMA
#include "rdma/RDMAStack.h"
#endif

#include "common/dout.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "stack "

void NetworkStack::add_thread(unsigned i, std::function<void ()> &thread)
{
  Worker *w = workers[i];
  thread = std::move(
    [this, w]() {
      const uint64_t EventMaxWaitUs = 30000000;
      w->center.set_owner();
      ldout(cct, 10) << __func__ << " starting" << dendl;
      w->initialize();
      w->init_done();
      while (!w->done) {
        ldout(cct, 30) << __func__ << " calling event process" << dendl;

        int r = w->center.process_events(EventMaxWaitUs);
        if (r < 0) {
          ldout(cct, 20) << __func__ << " process events failed: "
                         << cpp_strerror(errno) << dendl;
          // TODO do something?
        }
      }
      w->reset();
    }
  );
}

std::shared_ptr<NetworkStack> NetworkStack::create(CephContext *c, const string &t)
{
  if (t == "posix")
    return std::make_shared<PosixNetworkStack>(c, t);
#ifdef HAVE_RDMA
  else if (t == "rdma")
    return std::make_shared<RDMAStack>(c, t);
#endif

  return nullptr;
}

Worker* NetworkStack::create_worker(CephContext *c, const string &type, unsigned i)
{
  if (type == "posix")
    return new PosixWorker(c, i);
#ifdef HAVE_RDMA
  else if (type == "rdma")
    return new RDMAWorker(c, i);
#endif
  return nullptr;
}

NetworkStack::NetworkStack(CephContext *c, const string &t): type(t), started(false), cct(c)
{
  const uint64_t InitEventNumber = 5000;
  num_workers = cct->_conf->ms_async_op_threads;
  if (num_workers >= EventCenter::MAX_EVENTCENTER) {
    ldout(cct, 0) << __func__ << " max thread limit is "
                  << EventCenter::MAX_EVENTCENTER << ", switching to this now. "
                  << "Higher thread values are unnecessary and currently unsupported."
                  << dendl;
    num_workers = EventCenter::MAX_EVENTCENTER;
  }

  for (unsigned i = 0; i < num_workers; ++i) {
    Worker *w = create_worker(cct, type, i);
    w->center.init(InitEventNumber, i);
    workers.push_back(w);
  }
  cct->register_fork_watcher(this);
}

void NetworkStack::start()
{
  pool_spin.lock();
  if (started) {
    pool_spin.unlock();
    return ;
  }

  for (unsigned i = 0; i < num_workers; ++i) {
    if (workers[i]->is_init())
      continue;
    std::function<void ()> thread;
    add_thread(i, thread);
    spawn_worker(i, std::move(thread));
  }
  started = true;
  pool_spin.unlock();

  for (unsigned i = 0; i < num_workers; ++i)
    workers[i]->wait_for_init();
}

Worker* NetworkStack::get_worker()
{
  ldout(cct, 10) << __func__ << dendl;

   // start with some reasonably large number
  unsigned min_load = std::numeric_limits<int>::max();
  Worker* current_best = nullptr;

  pool_spin.lock();
  // find worker with least references
  // tempting case is returning on references == 0, but in reality
  // this will happen so rarely that there's no need for special case.
  for (unsigned i = 0; i < num_workers; ++i) {
    unsigned worker_load = workers[i]->references.load();
    if (worker_load < min_load) {
      current_best = workers[i];
      min_load = worker_load;
    }
  }

  pool_spin.unlock();
  assert(current_best);
  ++current_best->references;
  return current_best;
}

void NetworkStack::stop()
{
  Spinlock::Locker l(pool_spin);
  for (unsigned i = 0; i < num_workers; ++i) {
    workers[i]->done = true;
    workers[i]->center.wakeup();
    join_worker(i);
  }
  started = false;
}

class C_drain : public EventCallback {
  Mutex drain_lock;
  Cond drain_cond;
  std::atomic<unsigned> drain_count;

 public:
  explicit C_drain(size_t c)
      : drain_lock("C_drain::drain_lock"),
        drain_count(c) {}
  void do_request(int id) {
    Mutex::Locker l(drain_lock);
    drain_count--;
    drain_cond.Signal();
  }
  void wait() {
    Mutex::Locker l(drain_lock);
    while (drain_count.load())
      drain_cond.Wait(drain_lock);
  }
};

void NetworkStack::drain()
{
  ldout(cct, 10) << __func__ << " started." << dendl;
  pthread_t cur = pthread_self();
  pool_spin.lock();
  C_drain drain(num_workers);
  for (unsigned i = 0; i < num_workers; ++i) {
    assert(cur != workers[i]->center.get_owner());
    workers[i]->center.dispatch_event_external(EventCallbackRef(&drain));
  }
  pool_spin.unlock();
  drain.wait();
  ldout(cct, 10) << __func__ << " end." << dendl;
}
