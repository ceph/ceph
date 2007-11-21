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




#include "Timer.h"
#include "Cond.h"

#include "config.h"
#include "include/Context.h"

#define dout(x)  if (x <= g_conf.debug_timer) *_dout << dbeginl << g_clock.now() << " TIMER "
#define derr(x)  if (x <= g_conf.debug_timer) *_derr << dbeginl << g_clock.now() << " TIMER "

#define DBL 10

#include <signal.h>
#include <sys/time.h>
#include <math.h>

// single global instance
Timer      g_timer;



/**** thread solution *****/

bool Timer::get_next_due(utime_t& when)
{
  if (scheduled.empty()) {
    dout(10) << "get_next_due - nothing scheduled" << dendl;
    return false;
  } else {
    map< utime_t, set<Context*> >::iterator it = scheduled.begin();
    when = it->first;
    dout(10) << "get_next_due - " << when << dendl;
    return true;
  }
}


void Timer::timer_entry()
{
  lock.Lock();
  
  while (!thread_stop) {
    
    // now
    utime_t now = g_clock.now();

    // any events due?
    utime_t next;
    bool next_due = get_next_due(next);
    
    if (next_due && now >= next) {
      // move to pending list
      list<Context*> pending;

      map< utime_t, set<Context*> >::iterator it = scheduled.begin();
      while (it != scheduled.end()) {
        if (it->first > now) break;

        utime_t t = it->first;
        dout(DBL) << "queueing event(s) scheduled at " << t << dendl;

        for (set<Context*>::iterator cit = it->second.begin();
             cit != it->second.end();
             cit++) {
          pending.push_back(*cit);
          event_times.erase(*cit);
          num_event--;
        }

        map< utime_t, set<Context*> >::iterator previt = it;
        it++;
        scheduled.erase(previt);
      }

      if (!pending.empty()) {
        sleeping = false;
        lock.Unlock();
        { 
	  // make sure we're not holding any locks while we do callbacks
          // make the callbacks myself.
          for (list<Context*>::iterator cit = pending.begin();
               cit != pending.end();
               cit++) {
            dout(DBL) << "start callback " << *cit << dendl;
            (*cit)->finish(0);
            dout(DBL) << "finish callback " << *cit << dendl;
	    delete *cit;
          }
          pending.clear();
          assert(pending.empty());
        }
        lock.Lock();
      }

    }
    else {
      // sleep
      if (next_due) {
        dout(DBL) << "sleeping until " << next << dendl;
        timed_sleep = true;
        sleeping = true;
        timeout_cond.WaitUntil(lock, next);  // wait for waker or time
        utime_t now = g_clock.now();
        dout(DBL) << "kicked or timed out at " << now << dendl;
      } else {
        dout(DBL) << "sleeping" << dendl;
        timed_sleep = false;
        sleeping = true;
        sleep_cond.Wait(lock);         // wait for waker
        utime_t now = g_clock.now();
        dout(DBL) << "kicked at " << now << dendl;
      }
    }
  }

  lock.Unlock();
}



/**
 * Timer bits
 */

void Timer::register_timer()
{
  if (timer_thread.is_started()) {
    if (sleeping) {
      dout(DBL) << "register_timer kicking thread" << dendl;
      if (timed_sleep)
        timeout_cond.SignalAll();
      else
        sleep_cond.SignalAll();
    } else {
      dout(DBL) << "register_timer doing nothing; thread is awake" << dendl;
      // it's probably doing callbacks.
    }
  } else {
    dout(DBL) << "register_timer starting thread" << dendl;
    timer_thread.create();
  }
}

void Timer::cancel_timer()
{
  // clear my callback pointers
  if (timer_thread.is_started()) {
    dout(10) << "setting thread_stop flag" << dendl;
    lock.Lock();
    thread_stop = true;
    if (timed_sleep)
      timeout_cond.SignalAll();
    else
      sleep_cond.SignalAll();
    lock.Unlock();
    
    dout(10) << "waiting for thread to finish" << dendl;
    void *ptr;
    timer_thread.join(&ptr);
    
    dout(10) << "thread finished, exit code " << ptr << dendl;
  }
}


/*
 * schedule
 */


void Timer::add_event_after(double seconds,
                            Context *callback) 
{
  utime_t when = g_clock.now();
  when += seconds;
  add_event_at(when, callback);
}

void Timer::add_event_at(utime_t when,
                         Context *callback) 
{
  lock.Lock();

  dout(DBL) << "add_event " << callback << " at " << when << dendl;

  // insert
  scheduled[when].insert(callback);
  assert(event_times.count(callback) == 0);
  event_times[callback] = when;
  
  num_event++;
  
  // make sure i wake up on time
  register_timer();
  
  lock.Unlock();
}

bool Timer::cancel_event(Context *callback) 
{
  lock.Lock();
  
  dout(DBL) << "cancel_event " << callback << dendl;

  if (!event_times.count(callback)) {
    dout(DBL) << "cancel_event " << callback << " isn't scheduled (probably executing)" << dendl;
    lock.Unlock();
    return false;     // wasn't scheduled.
  }

  utime_t tp = event_times[callback];
  event_times.erase(callback);

  assert(scheduled.count(tp));
  assert(scheduled[tp].count(callback));
  scheduled[tp].erase(callback);
  if (scheduled[tp].empty())
    scheduled.erase(tp);
  
  lock.Unlock();

  // delete the canceled event.
  delete callback;

  return true;
}


// -------------------------------

void SafeTimer::add_event_after(double seconds, Context *c)
{
  assert(lock.is_locked());
  Context *w = new EventWrapper(this, c);
  dout(DBL) << "SafeTimer.add_event_after wrapping " << c << " with " << w << dendl;
  scheduled[c] = w;
  g_timer.add_event_after(seconds, w);
}

void SafeTimer::add_event_at(utime_t when, Context *c)
{
  assert(lock.is_locked());
  Context *w = new EventWrapper(this, c);
  dout(DBL) << "SafeTimer.add_event_at wrapping " << c << " with " << w << dendl;
  scheduled[c] = w;
  g_timer.add_event_at(when, w);
}

void SafeTimer::EventWrapper::finish(int r)
{
  timer->lock.Lock();
  if (timer->scheduled.count(actual)) {
    // still scheduled.  execute.
    actual->finish(r);
    timer->scheduled.erase(actual);
  } else {
    // i was canceled.
    assert(timer->canceled.count(actual));
  }

  // did i get canceled?
  // (this can happen even if i just executed above. e.g., i may have canceled myself.)
  if (timer->canceled.count(actual)) {
    timer->canceled.erase(actual); 
    timer->cond.Signal();
  }

  // delete the original event
  delete actual;

  timer->lock.Unlock();
}

void SafeTimer::cancel_event(Context *c) 
{
  assert(lock.is_locked());
  assert(scheduled.count(c));

  if (g_timer.cancel_event(scheduled[c])) {
    // hosed wrapper.  hose original event too.
    delete c;
  } else {
    // clean up later.
    canceled[c] = scheduled[c];
  }
  scheduled.erase(c);
}

void SafeTimer::cancel_all()
{
  assert(lock.is_locked());
  
  while (!scheduled.empty()) 
    cancel_event(scheduled.begin()->first);
}

void SafeTimer::join()
{
  assert(lock.is_locked());
  assert(scheduled.empty());

  if (!canceled.empty()) {
    while (!canceled.empty()) {
      // wait
      dout(2) << "SafeTimer.join waiting for " << canceled.size() << " to join: " << canceled << dendl;
      cond.Wait(lock);
    }
    dout(2) << "SafeTimer.join done" << dendl;
  }
}

SafeTimer::~SafeTimer()
{
  if (!scheduled.empty() && !canceled.empty()) {
    derr(0) << "SafeTimer.~SafeTimer " << scheduled.size() << " events scheduled, " 
	    << canceled.size() << " canceled but unflushed" 
	    << dendl;
  }
}
