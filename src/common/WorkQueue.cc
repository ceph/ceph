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
#include "common/errno.h"
#include "WorkQueue.h"

#include "common/config.h"
#include "common/HeartbeatMap.h"

#define dout_subsys ceph_subsys_tp
#undef dout_prefix
#define dout_prefix *_dout << name << " "


ThreadPool::ThreadPool(CephContext *cct_, string nm, int n, const char *option)
  : cct(cct_), name(nm),
    lockname(nm + "::lock"),
    _lock(lockname.c_str()),  // this should be safe due to declaration order
    _stop(false),
    _pause(0),
    _draining(0),
    ioprio_class(-1),
    ioprio_priority(-1),
    _num_threads(n),
    last_work_queue(0),
    processing(0)
{
  if (option) {
    _thread_num_option = option;
    // set up conf_keys
    _conf_keys = new const char*[2];
    _conf_keys[0] = _thread_num_option.c_str();
    _conf_keys[1] = NULL;
  } else {
    _conf_keys = new const char*[1];
    _conf_keys[0] = NULL;
  }
}

void ThreadPool::TPHandle::suspend_tp_timeout()
{
  cct->get_heartbeat_map()->clear_timeout(hb);
}

void ThreadPool::TPHandle::reset_tp_timeout()
{
  cct->get_heartbeat_map()->reset_timeout(
    hb, grace, suicide_grace);
}

ThreadPool::~ThreadPool()
{
  assert(_threads.empty());
  delete[] _conf_keys;
}

void ThreadPool::handle_conf_change(const struct md_config_t *conf,
				    const std::set <std::string> &changed)
{
  if (changed.count(_thread_num_option)) {
    char *buf;
    int r = conf->get_val(_thread_num_option.c_str(), &buf, -1);
    assert(r >= 0);
    int v = atoi(buf);
    free(buf);
    if (v > 0) {
      _lock.Lock();
      _num_threads = v;
      start_threads();
      _cond.SignalAll();
      _lock.Unlock();
    }
  }
}

void ThreadPool::worker(WorkThread *wt)
{
  _lock.Lock();
  ldout(cct,10) << "worker start" << dendl;
  
  std::stringstream ss;
  ss << name << " thread " << (void*)pthread_self();
  heartbeat_handle_d *hb = cct->get_heartbeat_map()->add_worker(ss.str());

  while (!_stop) {

    // manage dynamic thread pool
    join_old_threads();
    if (_threads.size() > _num_threads) {
      ldout(cct,1) << " worker shutting down; too many threads (" << _threads.size() << " > " << _num_threads << ")" << dendl;
      _threads.erase(wt);
      _old_threads.push_back(wt);
      break;
    }

    if (!_pause && !work_queues.empty()) {
      WorkQueue_* wq;
      int tries = work_queues.size();
      bool did = false;
      while (tries--) {
	last_work_queue++;
	last_work_queue %= work_queues.size();
	wq = work_queues[last_work_queue];
	//先从队列拿出来
	void *item = wq->_void_dequeue();
	if (item) {
	  processing++;
	  ldout(cct,12) << "worker wq " << wq->name << " start processing " << item
			<< " (" << processing << " active)" << dendl;
	  TPHandle tp_handle(cct, hb, wq->timeout_interval, wq->suicide_interval);
	  tp_handle.reset_tp_timeout();
	  _lock.Unlock();
	  wq->_void_process(item, tp_handle);
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
    cct->get_heartbeat_map()->reset_timeout(
      hb,
      cct->_conf->threadpool_default_timeout,
      0);
    _cond.WaitInterval(cct, _lock,
      utime_t(
	cct->_conf->threadpool_empty_queue_max_wait, 0));
  }
  ldout(cct,1) << "worker finish" << dendl;

  cct->get_heartbeat_map()->remove_worker(hb);

  _lock.Unlock();
}

void ThreadPool::start_threads()
{
  assert(_lock.is_locked());
  while (_threads.size() < _num_threads) {
    WorkThread *wt = new WorkThread(this);
    ldout(cct, 10) << "start_threads creating and starting " << wt << dendl;
    _threads.insert(wt);

    int r = wt->set_ioprio(ioprio_class, ioprio_priority);
    if (r < 0)
      lderr(cct) << " set_ioprio got " << cpp_strerror(r) << dendl;

    wt->create();
  }
}

void ThreadPool::join_old_threads()
{
  assert(_lock.is_locked());
  while (!_old_threads.empty()) {
    ldout(cct, 10) << "join_old_threads joining and deleting " << _old_threads.front() << dendl;
    _old_threads.front()->join();
    delete _old_threads.front();
    _old_threads.pop_front();
  }
}

void ThreadPool::start()
{
  ldout(cct,10) << "start" << dendl;

  if (_thread_num_option.length()) {
    ldout(cct, 10) << " registering config observer on " << _thread_num_option << dendl;
    cct->_conf->add_observer(this);
  }

  _lock.Lock();
  start_threads();
  _lock.Unlock();
  ldout(cct,15) << "started" << dendl;
}

void ThreadPool::stop(bool clear_after)
{
  ldout(cct,10) << "stop" << dendl;

  if (_thread_num_option.length()) {
    ldout(cct, 10) << " unregistering config observer on " << _thread_num_option << dendl;
    cct->_conf->remove_observer(this);
  }

  _lock.Lock();
  _stop = true;
  _cond.Signal();
  join_old_threads();
  _lock.Unlock();
  for (set<WorkThread*>::iterator p = _threads.begin();
       p != _threads.end();
       ++p) {
    (*p)->join();
    delete *p;
  }
  _threads.clear();
  _lock.Lock();
  for (unsigned i=0; i<work_queues.size(); i++)
    work_queues[i]->_clear();
  _stop = false;
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

void ThreadPool::set_ioprio(int cls, int priority)
{
  Mutex::Locker l(_lock);
  ioprio_class = cls;
  ioprio_priority = priority;
  for (set<WorkThread*>::iterator p = _threads.begin();
       p != _threads.end();
       ++p) {
    ldout(cct,10) << __func__ 
		  << " class " << cls << " priority " << priority
		  << " pid " << (*p)->get_pid()
		  << dendl;
    int r = (*p)->set_ioprio(cls, priority);
    if (r < 0)
      lderr(cct) << " set_ioprio got " << cpp_strerror(r) << dendl;
  }
}

ShardedThreadPool::ShardedThreadPool(CephContext *pcct_, string nm, 
  uint32_t pnum_threads): cct(pcct_),name(nm),lockname(nm + "::lock"), 
  shardedpool_lock(lockname.c_str()),num_threads(pnum_threads),stop_threads(0), 
  pause_threads(0),drain_threads(0), num_paused(0), num_drained(0), wq(NULL) {}

void ShardedThreadPool::shardedthreadpool_worker(uint32_t thread_index)
{
  assert(wq != NULL);
  ldout(cct,10) << "worker start" << dendl;

  std::stringstream ss;
  ss << name << " thread " << (void*)pthread_self();
  heartbeat_handle_d *hb = cct->get_heartbeat_map()->add_worker(ss.str());

  while (!stop_threads.read()) {
    if(pause_threads.read()) {
      shardedpool_lock.Lock();
      ++num_paused;
      wait_cond.Signal();
      while(pause_threads.read()) {
       cct->get_heartbeat_map()->reset_timeout(
	 hb,
	 wq->timeout_interval, wq->suicide_interval);
       shardedpool_cond.WaitInterval(cct, shardedpool_lock,
	 utime_t(
	   cct->_conf->threadpool_empty_queue_max_wait, 0));
      }
      --num_paused;
      shardedpool_lock.Unlock();
    }
    if (drain_threads.read()) {
      shardedpool_lock.Lock();
      if (wq->is_shard_empty(thread_index)) {
        ++num_drained;
        wait_cond.Signal();
        while (drain_threads.read()) {
	  cct->get_heartbeat_map()->reset_timeout(
	    hb,
	    wq->timeout_interval, wq->suicide_interval);
          shardedpool_cond.WaitInterval(cct, shardedpool_lock,
	    utime_t(
	      cct->_conf->threadpool_empty_queue_max_wait, 0));
        }
        --num_drained;
      }
      shardedpool_lock.Unlock();
    }

    cct->get_heartbeat_map()->reset_timeout(
      hb,
      wq->timeout_interval, wq->suicide_interval);
    wq->_process(thread_index, hb);

  }

  ldout(cct,10) << "sharded worker finish" << dendl;

  cct->get_heartbeat_map()->remove_worker(hb);

}

void ShardedThreadPool::start_threads()
{
  assert(shardedpool_lock.is_locked());
  int32_t thread_index = 0;
  while (threads_shardedpool.size() < num_threads) {

    WorkThreadSharded *wt = new WorkThreadSharded(this, thread_index);
    ldout(cct, 10) << "start_threads creating and starting " << wt << dendl;
    threads_shardedpool.push_back(wt);
    wt->create();
    thread_index++;
  }
}

void ShardedThreadPool::start()
{
  ldout(cct,10) << "start" << dendl;

  shardedpool_lock.Lock();
  start_threads();
  shardedpool_lock.Unlock();
  ldout(cct,15) << "started" << dendl;
}

void ShardedThreadPool::stop()
{
  ldout(cct,10) << "stop" << dendl;
  stop_threads.set(1);
  assert(wq != NULL);
  wq->return_waiting_threads();
  for (vector<WorkThreadSharded*>::iterator p = threads_shardedpool.begin();
       p != threads_shardedpool.end();
       ++p) {
    (*p)->join();
    delete *p;
  }
  threads_shardedpool.clear();
  ldout(cct,15) << "stopped" << dendl;
}

void ShardedThreadPool::pause()
{
  ldout(cct,10) << "pause" << dendl;
  shardedpool_lock.Lock();
  pause_threads.set(1);
  assert(wq != NULL);
  wq->return_waiting_threads();
  while (num_threads != num_paused){
    wait_cond.Wait(shardedpool_lock);
  }
  shardedpool_lock.Unlock();
  ldout(cct,10) << "paused" << dendl; 
}

void ShardedThreadPool::pause_new()
{
  ldout(cct,10) << "pause_new" << dendl;
  shardedpool_lock.Lock();
  pause_threads.set(1);
  assert(wq != NULL);
  wq->return_waiting_threads();
  shardedpool_lock.Unlock();
  ldout(cct,10) << "paused_new" << dendl;
}

void ShardedThreadPool::unpause()
{
  ldout(cct,10) << "unpause" << dendl;
  shardedpool_lock.Lock();
  pause_threads.set(0);
  shardedpool_cond.Signal();
  shardedpool_lock.Unlock();
  ldout(cct,10) << "unpaused" << dendl;
}

void ShardedThreadPool::drain()
{
  ldout(cct,10) << "drain" << dendl;
  shardedpool_lock.Lock();
  drain_threads.set(1);
  assert(wq != NULL);
  wq->return_waiting_threads();
  while (num_threads != num_drained) {
    wait_cond.Wait(shardedpool_lock);
  }
  drain_threads.set(0);
  shardedpool_cond.Signal();
  shardedpool_lock.Unlock();
  ldout(cct,10) << "drained" << dendl;
}

