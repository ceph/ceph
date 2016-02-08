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

void Finisher::wait_for_empty() noexcept
{
  unique_lock l(finisher_lock);
  ldout(cct, 10) << "wait_for_empty waiting" << dendl;
  // Since we can't capture const this
  finisher_empty_cond.wait(l, [this] {
      return finisher_queue.empty() && !finisher_running;
    });
  ldout(cct, 10) << "wait_for_empty empty" << dendl;
}

// We declare this noexcept specifically to get around a limitation in
// libstdc++. To handle thread cancellation, they catch all exceptions
// at thread start and call terminate on any but the specific
// __thread_cancel exception.

// With the noexcept specification here, our stack shouldn't be
// unwound before terminate() is called.

void Finisher::finisher_thread_entry() noexcept
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
      decltype(finisher_queue) ls = std::move(finisher_queue);
      finisher_queue.clear();
      finisher_running = true;
      l.unlock();
      // We don't have jobs identified by addresses any more and
      // there's no really good way of printing a function, generally.
      ldout(cct, 10) << "finisher_thread doing stuff" << dendl;

      // Now actually process the contexts.
      for (auto&& p : ls) {
	std::move(p)();
	if (logger) {
	  logger->dec(l_finisher_queue_len);
	  logger->tinc(l_finisher_complete_lat,
		       ceph::coarse_mono_clock::now() - start);
	}
      }
      ldout(cct, 10) << "finisher_thread done with stuff" << dendl;
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

