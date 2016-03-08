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

#include "PosixStack.h"
#ifdef HAVE_DPDK
#include "msg/async/dpdk/DPDKStack.h"
#endif

#include "common/errno.h"
#include "common/dout.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_dpdk
#undef dout_prefix
#define dout_prefix *_dout << "stack "

void Worker::stop()
{
  ldout(cct, 10) << __func__ << dendl;
  done = true;
  center.wakeup();
}

std::shared_ptr<NetworkStack> NetworkStack::create(CephContext *c, const string &t)
{
  if (t == "posix")
    return std::shared_ptr<NetworkStack>(new PosixNetworkStack(c, t));
#ifdef HAVE_DPDK
  else if (t == "dpdk")
    return std::shared_ptr<NetworkStack>(new DPDKStack(c, t));
#endif

  return nullptr;
}

Worker* NetworkStack::create_worker(CephContext *c, const string &type, unsigned i)
{
  if (type == "posix")
    return new PosixWorker(c, i);
  else if (type == "dpdk")
    return new DPDKWorker(c, i);
  return nullptr;
}

NetworkStack::NetworkStack(CephContext *c, const string &t): type(t), cct(c)
{
  assert(cct->_conf->ms_async_op_threads > 0);
  num_workers = cct->_conf->ms_async_op_threads;
  for (unsigned i = 0; i < num_workers; ++i) {
    Worker *w = create_worker(cct, type, i);
    workers.push_back(w);
  }
}

void NetworkStack::start()
{
  static std::vector<std::function<void ()>> threads;
  threads.clear();
  for (auto &&w : workers) {
    threads.emplace_back(
      [this, w]() {
        const uint64_t InitEventNumber = 5000;
        const uint64_t EventMaxWaitUs = 30000000;
        w->center.init(InitEventNumber, w->id);
        ldout(cct, 10) << __func__ << " starting" << dendl;
        w->initialize();
        w->init_done = true;
        while (!w->done) {
          ldout(cct, 30) << __func__ << " calling event process" << dendl;

          int r = w->center.process_events(EventMaxWaitUs);
          if (r < 0) {
            ldout(cct, 20) << __func__ << " process events failed: "
                           << cpp_strerror(errno) << dendl;
            // TODO do something?
          }
        }
      }
    );
  }
  spawn_workers(threads);

  for (auto &&w : workers) {
    while (!w->init_done)
      usleep(100);
  }
}

void NetworkStack::stop()
{
  for (auto &&w : workers)
    w->stop();
  join_workers();
}

class C_barrier : public EventCallback {
  Mutex barrier_lock;
  Cond barrier_cond;
  atomic_t barrier_count;

 public:
  explicit C_barrier(size_t c)
      : barrier_lock("C_barrier::barrier_lock"),
        barrier_count(c) {}
  void do_request(int id) {
    Mutex::Locker l(barrier_lock);
    barrier_count.dec();
    barrier_cond.Signal();
  }
  void wait() {
    Mutex::Locker l(barrier_lock);
    while (barrier_count.read())
      barrier_cond.Wait(barrier_lock);
  }
};

void NetworkStack::barrier()
{
  ldout(cct, 10) << __func__ << " started." << dendl;
  pthread_t cur = pthread_self();
  C_barrier barrier(workers.size());
  for (auto &&worker : workers) {
    assert(cur != worker->center.get_id());
    worker->center.dispatch_event_external(EventCallbackRef(&barrier));
  }
  barrier.wait();
  ldout(cct, 10) << __func__ << " end." << dendl;
}
