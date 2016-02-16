// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/config.h"
#include "common/ceph_time.h"

#include "Finisher.h"

#include "common/debug.h"
#define dout_subsys ceph_subsys_finisher
#undef dout_prefix
#define dout_prefix *_dout << "finisher(" << this << ") "

void Finisher::start()
{
  ldout(cct, 10) << __func__ << dendl;
  finisher_thread = make_named_thread(thread_name,
				      &Finisher::finisher_thread_entry, this);
}

void Finisher::stop()
{
  ldout(cct, 10) << __func__ << dendl;
  {
    lock_guard l(finisher_lock);
    finisher_stop = true;
    // we don't have any new work to do, but we want the worker to
    // wake up anyway to process the stop condition.
    finisher_cond.notify_one();
  }
  finisher_thread.join(); // wait until the worker exits completely
  ldout(cct, 10) << __func__ << " finish" << dendl;
}

void Finisher::wait_for_empty()
{
  unique_lock l(finisher_lock);
  ldout(cct, 10) << "wait_for_empty waiting" << dendl;
  finisher_empty_cond.wait(l, [this] {
      return finisher_queue.empty() && !finisher_running;
    });
  ldout(cct, 10) << "wait_for_empty empty" << dendl;
}

void Finisher::finisher_thread_entry()
{
  unique_lock l(finisher_lock);
  ldout(cct, 10) << "finisher_thread start" << dendl;

  ceph::coarse_mono_time start;
  while (!finisher_stop) {
    /// Every time we are woken up, we process the queue until it is empty.
    while (!finisher_queue.empty()) {
      if (logger)
	start = ceph::coarse_mono_clock::now();
      // To reduce lock contention, we swap out the queue to process.
      // This way other threads can submit new contexts to complete
      // while we are working.
      vector<Context*> ls;
      list<pair<Context*,int> > ls_rval;
      ls.swap(finisher_queue);
      ls_rval.swap(finisher_queue_rval);
      finisher_running = true;
      l.unlock();
      ldout(cct, 10) << "finisher_thread doing " << ls << dendl;

      // Now actually process the contexts.
      for (vector<Context*>::iterator p = ls.begin();
	   p != ls.end();
	   ++p) {
	if (*p) {
	  (*p)->complete(0);
	} else {
	  // When an item is NULL in the finisher_queue, it means
	  // we should instead process an item from finisher_queue_rval,
	  // which has a parameter for complete() other than zero.
	  // This preserves the order while saving some storage.
	  assert(!ls_rval.empty());
	  Context *c = ls_rval.front().first;
	  c->complete(ls_rval.front().second);
	  ls_rval.pop_front();
	}
	if (logger) {
	  logger->dec(l_finisher_queue_len);
	  logger->tinc(l_finisher_complete_lat,
		       ceph::coarse_mono_clock::now() - start);
	}
      }
      ldout(cct, 10) << "finisher_thread done with " << ls << dendl;
      ls.clear();

      l.lock();
      finisher_running = false;
    }
    ldout(cct, 10) << "finisher_thread empty" << dendl;
    finisher_empty_cond.notify_all();
    if (finisher_stop)
      break;

    ldout(cct, 10) << "finisher_thread sleeping" << dendl;
    finisher_cond.wait(l);
  }
  // If we are exiting, we signal the thread waiting in stop(),
  // otherwise it would never unblock
  finisher_empty_cond.notify_all();

  ldout(cct, 10) << "finisher_thread stop" << dendl;
  finisher_stop = false;
}

