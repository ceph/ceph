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

#include "WorkQueue.h"
#include "include/compat.h"
#include "common/errno.h"

#define dout_subsys ceph_subsys_tp
#undef dout_prefix
#define dout_prefix *_dout << name << " "


ThreadPool::ThreadPool(CephContext *cct_, string nm, string tn, int n, const char *option)
  : cct(cct_), name(std::move(nm)), thread_name(std::move(tn)),
    lockname(name + "::lock"),
    _lock(ceph::make_mutex(lockname)),  // this should be safe due to declaration order
    _stop(false),
    _pause(0),
    _draining(0),
    _num_threads(n),
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
  ceph_assert(_threads.empty());
  delete[] _conf_keys;
}

void ThreadPool::handle_conf_change(const ConfigProxy& conf,
				    const std::set <std::string> &changed)
{
  if (changed.count(_thread_num_option)) {
    char *buf;
    int r = conf.get_val(_thread_num_option.c_str(), &buf, -1);
    ceph_assert(r >= 0);
    int v = atoi(buf);
    free(buf);
    if (v >= 0) {
      _lock.lock();
      _num_threads = v;
      start_threads();
      _cond.notify_all();
      _lock.unlock();
    }
  }
}

void ThreadPool::worker(WorkThread *wt)
{
  std::unique_lock ul(_lock);
  ldout(cct,10) << "worker start" << dendl;
  
  std::stringstream ss;
  ss << name << " thread " << (void *)pthread_self();
  heartbeat_handle_d *hb = cct->get_heartbeat_map()->add_worker(ss.str(), pthread_self());

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
      int tries = 2 * work_queues.size();
      bool did = false;
      while (tries--) {
	next_work_queue %= work_queues.size();
	wq = work_queues[next_work_queue++];
	
	void *item = wq->_void_dequeue();
	if (item) {
	  processing++;
	  ldout(cct,12) << "worker wq " << wq->name << " start processing " << item
			<< " (" << processing << " active)" << dendl;
	  TPHandle tp_handle(cct, hb, wq->timeout_interval, wq->suicide_interval);
	  tp_handle.reset_tp_timeout();
	  ul.unlock();
	  wq->_void_process(item, tp_handle);
	  ul.lock();
	  wq->_void_process_finish(item);
	  processing--;
	  ldout(cct,15) << "worker wq " << wq->name << " done processing " << item
			<< " (" << processing << " active)" << dendl;
	  if (_pause || _draining)
	    _wait_cond.notify_all();
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
    auto wait = std::chrono::seconds(
      cct->_conf->threadpool_empty_queue_max_wait);
    _cond.wait_for(ul, wait);
  }
  ldout(cct,1) << "worker finish" << dendl;

  cct->get_heartbeat_map()->remove_worker(hb);
}

void ThreadPool::start_threads()
{
  ceph_assert(ceph_mutex_is_locked(_lock));
  while (_threads.size() < _num_threads) {
    WorkThread *wt = new WorkThread(this);
    ldout(cct, 10) << "start_threads creating and starting " << wt << dendl;
    _threads.insert(wt);

    wt->create(thread_name.c_str());
  }
}

void ThreadPool::join_old_threads()
{
  ceph_assert(ceph_mutex_is_locked(_lock));
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
    cct->_conf.add_observer(this);
  }

  _lock.lock();
  start_threads();
  _lock.unlock();
  ldout(cct,15) << "started" << dendl;
}

void ThreadPool::stop(bool clear_after)
{
  ldout(cct,10) << "stop" << dendl;

  if (_thread_num_option.length()) {
    ldout(cct, 10) << " unregistering config observer on " << _thread_num_option << dendl;
    cct->_conf.remove_observer(this);
  }

  _lock.lock();
  _stop = true;
  _cond.notify_all();
  join_old_threads();
  _lock.unlock();
  for (set<WorkThread*>::iterator p = _threads.begin();
       p != _threads.end();
       ++p) {
    (*p)->join();
    delete *p;
  }
  _threads.clear();
  _lock.lock();
  for (unsigned i=0; i<work_queues.size(); i++)
    work_queues[i]->_clear();
  _stop = false;
  _lock.unlock();
  ldout(cct,15) << "stopped" << dendl;
}

void ThreadPool::pause()
{
  std::unique_lock ul(_lock);
  ldout(cct,10) << "pause" << dendl;
  _pause++;
  while (processing) {
    _wait_cond.wait(ul);
  }
  ldout(cct,15) << "paused" << dendl;
}

void ThreadPool::pause_new()
{
  ldout(cct,10) << "pause_new" << dendl;
  _lock.lock();
  _pause++;
  _lock.unlock();
}

void ThreadPool::unpause()
{
  ldout(cct,10) << "unpause" << dendl;
  _lock.lock();
  ceph_assert(_pause > 0);
  _pause--;
  _cond.notify_all();
  _lock.unlock();
}

void ThreadPool::drain(WorkQueue_* wq)
{
  std::unique_lock ul(_lock);
  ldout(cct,10) << "drain" << dendl;
  _draining++;
  while (processing || (wq != NULL && !wq->_empty())) {
    _wait_cond.wait(ul);
  }
  _draining--;
}

ShardedThreadPool::ShardedThreadPool(CephContext *pcct_, string nm, string tn,
  uint32_t pnum_threads):
  cct(pcct_),
  name(std::move(nm)),
  thread_name(std::move(tn)),
  lockname(name + "::lock"),
  shardedpool_lock(ceph::make_mutex(lockname)),
  num_threads(pnum_threads),
  num_paused(0),
  num_drained(0),
  wq(NULL) {}

void ShardedThreadPool::shardedthreadpool_worker(uint32_t thread_index)
{
  ceph_assert(wq != NULL);
  ldout(cct,10) << "worker start" << dendl;

  std::stringstream ss;
  ss << name << " thread " << (void *)pthread_self();
  heartbeat_handle_d *hb = cct->get_heartbeat_map()->add_worker(ss.str(), pthread_self());

  while (!stop_threads) {
    if (pause_threads) {
      std::unique_lock ul(shardedpool_lock);
      ++num_paused;
      wait_cond.notify_all();
      while (pause_threads) {
       cct->get_heartbeat_map()->reset_timeout(
	        hb,
	        wq->timeout_interval, wq->suicide_interval);
       shardedpool_cond.wait_for(
	 ul,
	 std::chrono::seconds(cct->_conf->threadpool_empty_queue_max_wait));
      }
      --num_paused;
    }
    if (drain_threads) {
      std::unique_lock ul(shardedpool_lock);
      if (wq->is_shard_empty(thread_index)) {
        ++num_drained;
        wait_cond.notify_all();
        while (drain_threads) {
	  cct->get_heartbeat_map()->reset_timeout(
	    hb,
	    wq->timeout_interval, wq->suicide_interval);
          shardedpool_cond.wait_for(
	    ul,
	    std::chrono::seconds(cct->_conf->threadpool_empty_queue_max_wait));
        }
        --num_drained;
      }
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
  ceph_assert(ceph_mutex_is_locked(shardedpool_lock));
  int32_t thread_index = 0;
  while (threads_shardedpool.size() < num_threads) {

    WorkThreadSharded *wt = new WorkThreadSharded(this, thread_index);
    ldout(cct, 10) << "start_threads creating and starting " << wt << dendl;
    threads_shardedpool.push_back(wt);
    wt->create(thread_name.c_str());
    thread_index++;
  }
}

void ShardedThreadPool::start()
{
  ldout(cct,10) << "start" << dendl;

  shardedpool_lock.lock();
  start_threads();
  shardedpool_lock.unlock();
  ldout(cct,15) << "started" << dendl;
}

void ShardedThreadPool::stop()
{
  ldout(cct,10) << "stop" << dendl;
  stop_threads = true;
  ceph_assert(wq != NULL);
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
  std::unique_lock ul(shardedpool_lock);
  ldout(cct,10) << "pause" << dendl;
  pause_threads = true;
  ceph_assert(wq != NULL);
  wq->return_waiting_threads();
  while (num_threads != num_paused){
    wait_cond.wait(ul);
  }
  ldout(cct,10) << "paused" << dendl; 
}

void ShardedThreadPool::pause_new()
{
  ldout(cct,10) << "pause_new" << dendl;
  shardedpool_lock.lock();
  pause_threads = true;
  ceph_assert(wq != NULL);
  wq->return_waiting_threads();
  shardedpool_lock.unlock();
  ldout(cct,10) << "paused_new" << dendl;
}

void ShardedThreadPool::unpause()
{
  ldout(cct,10) << "unpause" << dendl;
  shardedpool_lock.lock();
  pause_threads = false;
  wq->stop_return_waiting_threads();
  shardedpool_cond.notify_all();
  shardedpool_lock.unlock();
  ldout(cct,10) << "unpaused" << dendl;
}

void ShardedThreadPool::drain()
{
  std::unique_lock ul(shardedpool_lock);
  ldout(cct,10) << "drain" << dendl;
  drain_threads = true;
  ceph_assert(wq != NULL);
  wq->return_waiting_threads();
  while (num_threads != num_drained) {
    wait_cond.wait(ul);
  }
  drain_threads = false;
  wq->stop_return_waiting_threads();
  shardedpool_cond.notify_all();
  ldout(cct,10) << "drained" << dendl;
}

