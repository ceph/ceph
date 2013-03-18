// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/config.h"
#include "Finisher.h"

#include "common/debug.h"
#define dout_subsys ceph_subsys_finisher
#undef dout_prefix
#define dout_prefix *_dout << "finisher(" << this << ") "

void Finisher::start()
{
  finisher_thread.create();
}

void Finisher::stop()
{
  finisher_lock.Lock();
  finisher_stop = true;
  finisher_cond.Signal();
  finisher_lock.Unlock();
  finisher_thread.join();
}

void Finisher::wait_for_empty()
{
  finisher_lock.Lock();
  while (!finisher_queue.empty() || finisher_running) {
    ldout(cct, 10) << "wait_for_empty waiting" << dendl;
    finisher_empty_cond.Wait(finisher_lock);
  }
  ldout(cct, 10) << "wait_for_empty empty" << dendl;
  finisher_lock.Unlock();
}

void *Finisher::finisher_thread_entry()
{
  finisher_lock.Lock();
  ldout(cct, 10) << "finisher_thread start" << dendl;

  while (!finisher_stop) {
    while (!finisher_queue.empty()) {
      vector<Context*> ls;
      list<pair<Context*,int> > ls_rval;
      ls.swap(finisher_queue);
      ls_rval.swap(finisher_queue_rval);
      finisher_running = true;
      finisher_lock.Unlock();
      ldout(cct, 10) << "finisher_thread doing " << ls << dendl;

      for (vector<Context*>::iterator p = ls.begin();
	   p != ls.end();
	   ++p) {
	if (*p) {
	  (*p)->finish(0);
	  delete *p;
	} else {
	  assert(!ls_rval.empty());
	  Context *c = ls_rval.front().first;
	  c->finish(ls_rval.front().second);
	  delete c;
	  ls_rval.pop_front();
	}
	if (logger)
	  logger->dec(l_finisher_queue_len);
      }
      ldout(cct, 10) << "finisher_thread done with " << ls << dendl;
      ls.clear();

      finisher_lock.Lock();
      finisher_running = false;
    }
    ldout(cct, 10) << "finisher_thread empty" << dendl;
    finisher_empty_cond.Signal();
    if (finisher_stop)
      break;
    
    ldout(cct, 10) << "finisher_thread sleeping" << dendl;
    finisher_cond.Wait(finisher_lock);
  }
  finisher_empty_cond.Signal();

  ldout(cct, 10) << "finisher_thread stop" << dendl;
  finisher_lock.Unlock();
  return 0;
}

