// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <sstream>

#include "include/types.h"
#include "include/utime.h"
#include "WorkQueue.h"

#include "common/config.h"
#include "common/HeartbeatMap.h"

#define dout_subsys ceph_subsys_tp
#undef dout_prefix
#define dout_prefix *_dout << name << " "


void ThreadPool::worker()
{
  _lock.Lock();
  ldout(cct,10) << "worker start" << dendl;
  
  std::stringstream ss;
  ss << name << " thread " << (void*)pthread_self();
  heartbeat_handle_d *hb = cct->get_heartbeat_map()->add_worker(ss.str());

  while (!_stop) {
    if (!_pause && work_queues.size()) {
      WorkQueue_* wq;
      int tries = work_queues.size();
      bool did = false;
      while (tries--) {
	last_work_queue++;
	last_work_queue %= work_queues.size();
	wq = work_queues[last_work_queue];
	
	void *item = wq->_void_dequeue();
	if (item) {
	  processing++;
	  ldout(cct,15) << "worker wq " << wq->name << " start processing " << item
			<< " (" << processing << " active)" << dendl;
	  _lock.Unlock();
	  cct->get_heartbeat_map()->reset_timeout(hb, wq->timeout_interval, wq->suicide_interval);
	  wq->_void_process(item);
	  _lock.Lock();
	  wq->_void_process_finish(item);
	  processing--;
	  ldout(cct,15) << "worker wq " << wq->name << " done processing " << item
			<< " (" << processing << " active)" << dendl;
	  if (_pause || _draining)
	    _wait_cond.Signal();
	  did = true;
	  break;
	}
      }
      if (did)
	continue;
    }

    ldout(cct,20) << "worker waiting" << dendl;
    cct->get_heartbeat_map()->reset_timeout(hb, 4, 0);
    _cond.WaitInterval(cct, _lock, utime_t(2, 0));
  }
  ldout(cct,1) << "worker finish" << dendl;

  cct->get_heartbeat_map()->remove_worker(hb);

  _lock.Unlock();
}

void ThreadPool::start()
{
  ldout(cct,10) << "start" << dendl;
  for (set<WorkThread*>::iterator p = _threads.begin();
       p != _threads.end();
       p++)
    (*p)->create();
  ldout(cct,15) << "started" << dendl;
}
void ThreadPool::stop(bool clear_after)
{
  ldout(cct,10) << "stop" << dendl;
  _lock.Lock();
  _stop = true;
  _cond.Signal();
  _lock.Unlock();
  for (set<WorkThread*>::iterator p = _threads.begin();
       p != _threads.end();
       p++)
    (*p)->join();
  _lock.Lock();
  for (unsigned i=0; i<work_queues.size(); i++)
    work_queues[i]->_clear();
  _lock.Unlock();    
  ldout(cct,15) << "stopped" << dendl;
}

void ThreadPool::pause()
{
  ldout(cct,10) << "pause" << dendl;
  _lock.Lock();
  _pause++;
  while (processing)
    _wait_cond.Wait(_lock);
  _lock.Unlock();
  ldout(cct,15) << "paused" << dendl;
}

void ThreadPool::pause_new()
{
  ldout(cct,10) << "pause_new" << dendl;
  _lock.Lock();
  _pause++;
  _lock.Unlock();
}

void ThreadPool::unpause()
{
  ldout(cct,10) << "unpause" << dendl;
  _lock.Lock();
  assert(_pause > 0);
  _pause--;
  _cond.Signal();
  _lock.Unlock();
}

void ThreadPool::drain(WorkQueue_* wq)
{
  ldout(cct,10) << "drain" << dendl;
  _lock.Lock();
  _draining++;
  while (processing || (wq != NULL && !wq->_empty()))
    _wait_cond.Wait(_lock);
  _draining--;
  _lock.Unlock();
}

