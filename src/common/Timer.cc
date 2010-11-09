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

#include "Cond.h"
#include "Mutex.h"
#include "Thread.h"
#include "Timer.h"

#include "config.h"
#include "include/Context.h"

#define DOUT_SUBSYS timer
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << " TIMER "

#define DBL 10

#include <sstream>
#include <signal.h>
#include <sys/time.h>
#include <math.h>

typedef std::multimap < utime_t, Context *> scheduled_map_t;
typedef std::map < Context*, scheduled_map_t::iterator > event_lookup_map_t;

class TimerThread : public Thread {
public:
  TimerThread(Timer &parent_)
    : p(parent_)
  {
  }

  void *entry()
  {
    p.lock.Lock();
    while (true) {
      /* Wait for a cond_signal */
      scheduled_map_t::iterator s = p.scheduled.begin();
      if (s == p.scheduled.end())
	p.cond.Wait(p.lock);
      else
	p.cond.WaitUntil(p.lock, s->first);

      if (p.exiting) {
	dout(DBL) << "exiting TimerThread" << dendl;
	p.lock.Unlock();
	return NULL;
      }

      // Find out what callbacks we have to do
      list <Context*> running;
      utime_t now = g_clock.now();
      p.pop_running(running, now);

      if (running.empty()) {
	dout(DBL) << "TimerThread: nothing to do." << dendl;
	continue;
      }

      p.lock.Unlock();

      lock_event_lock();
      // p.running is protected by the event lock.
      p.running.swap(running);
      while (true) {
	list <Context*>::const_iterator cit = p.running.begin();
	if (cit == p.running.end())
	  break;
	Context *ctx = *cit;
	p.running.pop_front();
	dout(DBL) << "start callback " << ctx << dendl;
	ctx->finish(0);
	dout(DBL) << "deleting callback " << ctx << dendl;
	delete ctx;
	unlock_event_lock();
	// Release the event_lock here to give other waiters a chance.
	dout(DBL) << "finished callback " << ctx << dendl;
	lock_event_lock();
      }
      unlock_event_lock();
      p.lock.Lock();
    }
  }

private:
  inline void lock_event_lock()
  {
    if (p.event_lock)
      p.event_lock->Lock();
  }

  inline void unlock_event_lock()
  {
    if (p.event_lock)
      p.event_lock->Unlock();
  }

  Timer &p;
};

Timer::Timer()
  : lock("Timer::lock"),
    event_lock(NULL),
    cond(),
    thread(NULL),
    exiting(false)
{
  if (init()) {
    assert(0);
  }
}

Timer::Timer(Mutex *event_lock_)
  : lock("Timer::lock"),
    event_lock(event_lock_),
    cond(),
    thread(NULL),
    exiting(false)
{
  if (init()) {
    assert(0);
  }
}

Timer::~Timer()
{
  shutdown();
}

void Timer::shutdown()
{
  lock.Lock();
  if (!thread) {
    lock.Unlock();
    return;
  }
  exiting = true;
  cancel_all_events_impl(false);
  cond.Signal();
  lock.Unlock();

  /* Block until the thread has exited.
   * Only then do we know that no events are in progress. */
  thread->join();

  delete thread;
  thread = NULL;
}

void Timer::add_event_after(double seconds,
                            Context *callback)
{
  utime_t when = g_clock.now();
  when += seconds;
  Timer::add_event_at(when, callback);
}

void Timer::add_event_at(utime_t when, Context *callback)
{
  lock.Lock();

  /* Don't start using the timer until it's initialized */
  assert(thread);

  dout(DBL) << "add_event " << callback << " at " << when << dendl;

  scheduled_map_t::value_type s_val(when, callback);
  scheduled_map_t::iterator i = scheduled.insert(s_val);

  event_lookup_map_t::value_type e_val(callback, i);
  pair < event_lookup_map_t::iterator, bool > rval(events.insert(e_val));
  dout(DBL) << "inserted events entry for " << callback << dendl;

  /* If you hit this, you tried to insert the same Context* twice. */
  assert(rval.second);

  /* If the event we have just inserted comes before everything else, we need to
   * adjust our timeout. */
  if (i == scheduled.begin())
    cond.Signal();

  dout(19) << show_all_events(__func__) << dendl;
  lock.Unlock();
}

bool Timer::cancel_event(Context *callback)
{
  dout(DBL) << __PRETTY_FUNCTION__ << ": " << callback << dendl;

  lock.Lock();
  bool ret = cancel_event_impl(callback, false);
  lock.Unlock();
  return ret;
}

void Timer::cancel_all_events(void)
{
  dout(DBL) << __PRETTY_FUNCTION__ << dendl;

  dout(19) << show_all_events(__func__) << dendl;

  lock.Lock();
  cancel_all_events_impl(false);
  lock.Unlock();
}

int Timer::init()
{
  int ret = 0;
  lock.Lock();
  assert(exiting == false);
  assert(!thread);

  dout(DBL) << "Timer::init: starting thread" << dendl;
  thread = new TimerThread(*this);
  ret = thread->create();
  lock.Unlock();
  return ret;
}

bool Timer::cancel_event_impl(Context *callback, bool cancel_running)
{
  dout(19) << show_all_events(__func__) << dendl;

  event_lookup_map_t::iterator e = events.find(callback);
  if (e != events.end()) {
    // Erase the item out of the scheduled map.
    scheduled.erase(e->second);
    events.erase(e);

    delete callback;
    return true;
  }

  // If we can't peek at the running list, we have to give up.
  if (!cancel_running)
    return false;

  // Ok, we will check the running list. It's safe, because we're holding the
  // event_lock.
  list <Context*>::iterator cit =
    std::find(running.begin(), running.end(), callback);
  if (cit == running.end())
    return false;
  running.erase(cit);
  delete callback;
  return true;
}

void Timer::cancel_all_events_impl(bool clear_running)
{
  while (1) {
    scheduled_map_t::iterator s = scheduled.begin();
    if (s == scheduled.end())
      break;
    delete s->second;
    scheduled.erase(s);
  }
  events.clear();

  if (clear_running) {
    running.clear();
  }
}

void Timer::pop_running(list <Context*> &running_, const utime_t &now)
{
  while (true) {
    std::multimap < utime_t, Context* >::iterator s = scheduled.begin();
    if (s == scheduled.end())
      return;
    const utime_t &utime(s->first);
    Context *cit(s->second);
    if (utime > now)
      return;
    running_.push_back(cit);
    event_lookup_map_t::iterator e = events.find(cit);
    assert(e != events.end());
    events.erase(e);
    scheduled.erase(s);
  }
}

std::string Timer::show_all_events(const char *caller) const
{
  ostringstream oss;
  string sep;
  oss << "show_all_events: from " << caller << ": scheduled [";
  for (scheduled_map_t::const_iterator s = scheduled.begin();
       s != scheduled.end();
       ++s)
  {
    oss << sep << s->first << "->" << s->second;
    sep = ",";
  }
  oss << "] ";

  oss << "events [";
  string sep2;
  for (event_lookup_map_t::const_iterator e = events.begin();
       e != events.end();
       ++e)
  {
    oss << sep2 << e->first << "->" << e->second->first;
    sep2 = ",";
  }
  oss << "]";
  return oss.str();
}

/******************************************************************/
SafeTimer::SafeTimer(Mutex &event_lock)
  : t(&event_lock)
{
}

SafeTimer::~SafeTimer()
{
  dout(DBL) << __PRETTY_FUNCTION__ << dendl;

  t.shutdown();
}

void SafeTimer::shutdown()
{
  t.shutdown();
}

void SafeTimer::add_event_after(double seconds, Context *callback)
{
  assert(t.event_lock->is_locked());

  t.add_event_after(seconds, callback);
}

void SafeTimer::add_event_at(utime_t when, Context *callback)
{
  assert(t.event_lock->is_locked());

  t.add_event_at(when, callback);
}

bool SafeTimer::cancel_event(Context *callback)
{
  dout(DBL) << __PRETTY_FUNCTION__ << ": " << callback << dendl;

  assert(t.event_lock->is_locked());

  t.lock.Lock();
  bool ret = t.cancel_event_impl(callback, true);
  t.lock.Unlock();
  return ret;
}

void SafeTimer::cancel_all_events()
{
  dout(DBL) << __PRETTY_FUNCTION__ << dendl;

  assert(t.event_lock->is_locked());

  t.lock.Lock();
  t.cancel_all_events_impl(true);
  t.lock.Unlock();
}
