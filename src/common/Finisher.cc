// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Finisher.h"

#define dout_subsys ceph_subsys_finisher
#undef dout_prefix
#define dout_prefix *_dout << "finisher(" << this << ") "

void Finisher::start()
{
  ldout(cct, 10) << __func__ << dendl;
  finisher_thread.create(thread_name.c_str());
}

void Finisher::stop()
{
  ldout(cct, 10) << __func__ << dendl;
  finisher_lock.Lock();
  finisher_stop = true;
  // we don't have any new work to do, but we want the worker to wake up anyway
  // to process the stop condition.
  finisher_cond.Signal();
  finisher_lock.Unlock();
  finisher_thread.join(); // wait until the worker exits completely
  ldout(cct, 10) << __func__ << " finish" << dendl;
}

void Finisher::wait_for_empty()
{
  finisher_lock.Lock();
  while (!finisher_queue.empty() || finisher_running) {
    ldout(cct, 10) << "wait_for_empty waiting" << dendl;
    finisher_empty_wait = true;
    finisher_empty_cond.Wait(finisher_lock);
  }
  ldout(cct, 10) << "wait_for_empty empty" << dendl;
  finisher_empty_wait = false;
  finisher_lock.Unlock();
}

void *Finisher::finisher_thread_entry()
{
  finisher_lock.Lock();
  ldout(cct, 10) << "finisher_thread start" << dendl;

  utime_t start;
  uint64_t count = 0;
  while (!finisher_stop) {
    /// Every time we are woken up, we process the queue until it is empty.
    while (!finisher_queue.empty()) {
      // To reduce lock contention, we swap out the queue to process.
      // This way other threads can submit new contexts to complete
      // while we are working.
      vector<pair<Context*,int>> ls;
      ls.swap(finisher_queue);
      finisher_running = true;
      finisher_lock.Unlock();
      ldout(cct, 10) << "finisher_thread doing " << ls << dendl;

      if (logger) {
	start = ceph_clock_now();
	count = ls.size();
      }

      // Now actually process the contexts.
      for (auto p : ls) {
	p.first->complete(p.second);
      }
      ldout(cct, 10) << "finisher_thread done with " << ls << dendl;
      ls.clear();
      if (logger) {
	logger->dec(l_finisher_queue_len, count);
	logger->tinc(l_finisher_complete_lat, ceph_clock_now() - start);
      }

      finisher_lock.Lock();
      finisher_running = false;
    }
    ldout(cct, 10) << "finisher_thread empty" << dendl;
    if (unlikely(finisher_empty_wait))
      finisher_empty_cond.Signal();
    if (finisher_stop)
      break;
    
    ldout(cct, 10) << "finisher_thread sleeping" << dendl;
    finisher_cond.Wait(finisher_lock);
  }
  // If we are exiting, we signal the thread waiting in stop(),
  // otherwise it would never unblock
  finisher_empty_cond.Signal();

  ldout(cct, 10) << "finisher_thread stop" << dendl;
  finisher_stop = false;
  finisher_lock.Unlock();
  return 0;
}

