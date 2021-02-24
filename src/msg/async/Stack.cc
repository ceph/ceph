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

#include <mutex>

#include "include/compat.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "PosixStack.h"
#ifdef HAVE_RDMA
#include "rdma/RDMAStack.h"
#endif
#ifdef HAVE_DPDK
#include "dpdk/DPDKStack.h"
#endif

#include "common/dout.h"
#include "include/ceph_assert.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "stack "

std::function<void ()> NetworkStack::add_thread(Worker* w)
{
  return [this, w]() {
      char tp_name[16];
      sprintf(tp_name, "msgr-worker-%u", w->id);
      ceph_pthread_setname(pthread_self(), tp_name);
      const unsigned EventMaxWaitUs = 30000000;
      w->center.set_owner();
      ldout(cct, 10) << __func__ << " starting" << dendl;
      w->initialize();
      w->init_done();
      while (!w->done) {
        ldout(cct, 30) << __func__ << " calling event process" << dendl;

        ceph::timespan dur;
        int r = w->center.process_events(EventMaxWaitUs, &dur);
        if (r < 0) {
          ldout(cct, 20) << __func__ << " process events failed: "
                         << cpp_strerror(errno) << dendl;
          // TODO do something?
        }
        w->perf_logger->tinc(l_msgr_running_total_time, dur);
      }
      w->reset();
      w->destroy();
  };
}

std::shared_ptr<NetworkStack> NetworkStack::create(CephContext *c,
						   const std::string &t)
{
  std::shared_ptr<NetworkStack> stack = nullptr;

  if (t == "posix")
    stack.reset(new PosixNetworkStack(c));
#ifdef HAVE_RDMA
  else if (t == "rdma")
    stack.reset(new RDMAStack(c));
#endif
#ifdef HAVE_DPDK
  else if (t == "dpdk")
    stack.reset(new DPDKStack(c));
#endif

  if (stack == nullptr) {
    lderr(c) << __func__ << " ms_async_transport_type " << t <<
    " is not supported! " << dendl;
    ceph_abort();
    return nullptr;
  }
  
  unsigned num_workers = c->_conf->ms_async_op_threads;
  ceph_assert(num_workers > 0);
  if (num_workers >= EventCenter::MAX_EVENTCENTER) {
    ldout(c, 0) << __func__ << " max thread limit is "
                  << EventCenter::MAX_EVENTCENTER << ", switching to this now. "
                  << "Higher thread values are unnecessary and currently unsupported."
                  << dendl;
    num_workers = EventCenter::MAX_EVENTCENTER;
  }
  const int InitEventNumber = 5000;
  for (unsigned worker_id = 0; worker_id < num_workers; ++worker_id) {
    Worker *w = stack->create_worker(c, worker_id);
    int ret = w->center.init(InitEventNumber, worker_id, t);
    if (ret)
      throw std::system_error(-ret, std::generic_category());
    stack->workers.push_back(w);
  }

  return stack;
}

NetworkStack::NetworkStack(CephContext *c)
  : cct(c)
{}

void NetworkStack::start()
{
  std::unique_lock<decltype(pool_spin)> lk(pool_spin);

  if (started) {
    return ;
  }

  for (Worker* worker : workers) {
    if (worker->is_init())
      continue;
    spawn_worker(add_thread(worker));
  }
  started = true;
  lk.unlock();

  for (Worker* worker : workers) {
    worker->wait_for_init();
  }
}

Worker* NetworkStack::get_worker()
{
  ldout(cct, 30) << __func__ << dendl;

   // start with some reasonably large number
  unsigned min_load = std::numeric_limits<int>::max();
  Worker* current_best = nullptr;

  pool_spin.lock();
  // find worker with least references
  // tempting case is returning on references == 0, but in reality
  // this will happen so rarely that there's no need for special case.
  for (Worker* worker : workers) {
    unsigned worker_load = worker->references.load();
    if (worker_load < min_load) {
      current_best = worker;
      min_load = worker_load;
    }
  }

  pool_spin.unlock();
  ceph_assert(current_best);
  ++current_best->references;
  return current_best;
}

void NetworkStack::stop()
{
  std::lock_guard lk(pool_spin);
  unsigned i = 0;
  for (Worker* worker : workers) {
    worker->done = true;
    worker->center.wakeup();
    join_worker(i++);
  }
  started = false;
}

class C_drain : public EventCallback {
  ceph::mutex drain_lock = ceph::make_mutex("C_drain::drain_lock");
  ceph::condition_variable drain_cond;
  unsigned drain_count;

 public:
  explicit C_drain(size_t c)
      : drain_count(c) {}
  void do_request(uint64_t id) override {
    std::lock_guard l{drain_lock};
    drain_count--;
    if (drain_count == 0) drain_cond.notify_all();
  }
  void wait() {
    std::unique_lock l{drain_lock};
    drain_cond.wait(l, [this] { return drain_count == 0; });
  }
};

void NetworkStack::drain()
{
  ldout(cct, 30) << __func__ << " started." << dendl;
  pthread_t cur = pthread_self();
  pool_spin.lock();
  C_drain drain(get_num_worker());
  for (Worker* worker : workers) {
    ceph_assert(cur != worker->center.get_owner());
    worker->center.dispatch_event_external(EventCallbackRef(&drain));
  }
  pool_spin.unlock();
  drain.wait();
  ldout(cct, 30) << __func__ << " end." << dendl;
}
