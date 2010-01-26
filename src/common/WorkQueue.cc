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

#include "include/types.h"
#include "WorkQueue.h"

#include "config.h"

#define DOUT_SUBSYS tp
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << std::hex << pthread_self() << std::dec << " " << name << " "


void ThreadPool::worker()
{
  _lock.Lock();
  dout(10) << "worker start" << dendl;
  while (!_stop) {
    if (!_pause && work_queues.size()) {
      _WorkQueue *wq;
      int tries = work_queues.size();
      bool did = false;
      while (tries--) {
	last_work_queue++;
	last_work_queue %= work_queues.size();
	wq = work_queues[last_work_queue];
	
	void *item = wq->_void_dequeue();
	if (item) {
	  processing++;
	  dout(12) << "worker wq " << wq->name << " start processing " << item << dendl;
	  _lock.Unlock();
	  wq->_void_process(item);
	  _lock.Lock();
	  dout(15) << "worker wq " << wq->name << " done processing " << item << dendl;
	  processing--;
	  if (_pause || _draining)
	    _wait_cond.Signal();
	  did = true;
	  break;
	}
      }
      if (did)
	continue;
    }
    dout(15) << "worker waiting" << dendl;
    _cond.Wait(_lock);
  }
  dout(0) << "worker finish" << dendl;
  _lock.Unlock();
}

void ThreadPool::start()
{
  dout(10) << "start" << dendl;
  for (set<WorkThread*>::iterator p = _threads.begin();
       p != _threads.end();
       p++)
    (*p)->create();
  dout(15) << "started" << dendl;
}
void ThreadPool::stop(bool clear_after)
{
  dout(10) << "stop" << dendl;
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
  dout(15) << "stopped" << dendl;
}


void ThreadPool::pause()
{
  dout(10) << "pause" << dendl;
  _lock.Lock();
  assert(!_pause);
  _pause = true;
  while (processing)
    _wait_cond.Wait(_lock);
  _lock.Unlock();
  dout(15) << "paused" << dendl;
}

void ThreadPool::pause_new()
{
  dout(10) << "pause_new" << dendl;
  _lock.Lock();
  assert(!_pause);
  _pause = true;
  _lock.Unlock();
}

void ThreadPool::unpause()
{
  dout(10) << "unpause" << dendl;
  _lock.Lock();
  assert(_pause);
  _pause = false;
  _cond.Signal();
  _lock.Unlock();
}

void ThreadPool::drain()
{
  dout(10) << "drain" << dendl;
  _lock.Lock();
  _draining = true;
  while (processing)
    _wait_cond.Wait(_lock);
  _draining = false;
  _lock.Unlock();
}

