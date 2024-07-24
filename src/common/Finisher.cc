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
  finisher_lock.lock();
  finisher_stop = true;
  // we don't have any new work to do, but we want the worker to wake up anyway
  // to process the stop condition.
  finisher_cond.notify_all();
  finisher_lock.unlock();
  finisher_thread.join(); // wait until the worker exits completely
  ldout(cct, 10) << __func__ << " finish" << dendl;
}

void Finisher::wait_for_empty()
{
  std::unique_lock ul(finisher_lock);
  while (!finisher_queue.empty() || finisher_running) {
    ldout(cct, 10) << "wait_for_empty waiting" << dendl;
    finisher_empty_wait = true;
    finisher_empty_cond.wait(ul);
  }
  ldout(cct, 10) << "wait_for_empty empty" << dendl;
  finisher_empty_wait = false;
}

bool Finisher::is_empty()
{
  std::unique_lock ul(finisher_lock);
  return finisher_queue.empty();
}

void *Finisher::finisher_thread_entry()
{
  std::unique_lock ul(finisher_lock);
  ldout(cct, 10) << "finisher_thread start" << dendl;

  utime_t start;
  uint64_t count = 0;
  while (!finisher_stop) {
    /// Every time we are woken up, we process the queue until it is empty.
    while (!finisher_queue.empty()) {
      // To reduce lock contention, we swap out the queue to process.
      // This way other threads can submit new contexts to complete
      // while we are working.
      in_progress_queue.swap(finisher_queue);
      finisher_running = true;
      ul.unlock();
      ldout(cct, 10) << "finisher_thread doing " << in_progress_queue << dendl;

      if (logger) {
	start = ceph_clock_now();
	count = in_progress_queue.size();
      }

      // Now actually process the contexts.
      for (auto p : in_progress_queue) {
	p.first->complete(p.second);
      }
      ldout(cct, 10) << "finisher_thread done with " << in_progress_queue
                     << dendl;
      in_progress_queue.clear();
      if (logger) {
	logger->dec(l_finisher_queue_len, count);
	logger->tinc(l_finisher_complete_lat, ceph_clock_now() - start);
      }

      ul.lock();
      finisher_running = false;
    }
    ldout(cct, 10) << "finisher_thread empty" << dendl;
    if (unlikely(finisher_empty_wait))
      finisher_empty_cond.notify_all();
    if (finisher_stop)
      break;
    
    ldout(cct, 10) << "finisher_thread sleeping" << dendl;
    finisher_cond.wait(ul);
  }
  // If we are exiting, we signal the thread waiting in stop(),
  // otherwise it would never unblock
  finisher_empty_cond.notify_all();

  ldout(cct, 10) << "finisher_thread stop" << dendl;
  finisher_stop = false;
  return 0;
}

