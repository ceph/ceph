// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#undef dout
#define dout(x)  if (x <= g_conf.debug) cout << g_clock.now() << " TIMER "

#define DBL 10

#include <signal.h>
#include <sys/time.h>
#include <math.h>

// single global instance
Timer      g_timer;


/*
Context* Timer::get_next_scheduled(utime_t& when)
{
  if (scheduled.empty()) {
    dout(10) << "get_next_scheduled - nothing scheduled" << endl;
    return 0;
  }
  map< utime_t, multiset<Context*> >::iterator it = scheduled.begin();
  when = it->first;
  multiset<Context*>::iterator sit = it->second.begin();
  dout(10) << "get_next_scheduled " << *sit << " at " << when << endl;
  return *sit;
}*/


bool Timer::get_next_due(utime_t& when)
{
  if (scheduled.empty()) {
    dout(10) << "get_next_due - nothing scheduled" << endl;
    return false;
  } else {
    map< utime_t, multiset<Context*> >::iterator it = scheduled.begin();
    when = it->first;
    dout(10) << "get_next_due - " << when << endl;
    return true;
  }
}

/**** thread solution *****/

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

      map< utime_t, multiset<Context*> >::iterator it = scheduled.begin();
      while (it != scheduled.end()) {
        if (it->first > now) break;

        utime_t t = it->first;
        dout(DBL) << "queueing event(s) scheduled at " << t << endl;

        for (multiset<Context*>::iterator cit = it->second.begin();
             cit != it->second.end();
             cit++) {
          pending.push_back(*cit);
          event_times.erase(*cit);
          num_event--;
        }

        map< utime_t, multiset<Context*> >::iterator previt = it;
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
            dout(DBL) << "doing callback " << *cit << endl;
            (*cit)->finish(0);
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
        dout(DBL) << "sleeping until " << next << endl;
        timed_sleep = true;
        sleeping = true;
        timeout_cond.WaitUntil(lock, next);  // wait for waker or time
        utime_t now = g_clock.now();
        dout(DBL) << "kicked or timed out at " << now << endl;
      } else {
        dout(DBL) << "sleeping" << endl;
        timed_sleep = false;
        sleeping = true;
        sleep_cond.Wait(lock);         // wait for waker
        utime_t now = g_clock.now();
        dout(DBL) << "kicked at " << now << endl;
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
      dout(DBL) << "register_timer kicking thread" << endl;
      if (timed_sleep)
        timeout_cond.SignalAll();
      else
        sleep_cond.SignalAll();
    } else {
      dout(DBL) << "register_timer doing nothing; thread is awake" << endl;
      // it's probably doing callbacks.
    }
  } else {
    dout(DBL) << "register_timer starting thread" << endl;
    timer_thread.create();
  }
}

void Timer::cancel_timer()
{
  // clear my callback pointers
  if (timer_thread.is_started()) {
    dout(10) << "setting thread_stop flag" << endl;
    lock.Lock();
    thread_stop = true;
    if (timed_sleep)
      timeout_cond.SignalAll();
    else
      sleep_cond.SignalAll();
    lock.Unlock();
    
    dout(10) << "waiting for thread to finish" << endl;
    void *ptr;
    timer_thread.join(&ptr);
    
    dout(10) << "thread finished, exit code " << ptr << endl;
  }
}


/*
 * schedule
 */


void Timer::add_event_after(float seconds,
                            Context *callback) 
{
  utime_t when = g_clock.now();
  when.sec_ref() += (int)seconds;
  add_event_at(when, callback);
}

void Timer::add_event_at(utime_t when,
                         Context *callback) 
{
  // insert
  dout(DBL) << "add_event " << callback << " at " << when << endl;

  lock.Lock();
  scheduled[ when ].insert(callback);
  assert(event_times.count(callback) == 0);     // err.. there can be only one (for now!)
  event_times[callback] = when;
  
  num_event++;

  // make sure i wake up
  register_timer();

  lock.Unlock();
}

bool Timer::cancel_event(Context *callback) 
{
  lock.Lock();
  
  dout(DBL) << "cancel_event " << callback << endl;

  if (!event_times.count(callback)) {
    dout(DBL) << "cancel_event " << callback << " wasn't scheduled?" << endl;
    lock.Unlock();
    assert(0);
    return false;     // wasn't scheduled.
  }

  utime_t tp = event_times[callback];
  assert(scheduled.count(tp));

  multiset<Context*>::iterator p = scheduled[tp].find(callback);  // there may be more than one?
  assert(p != scheduled[tp].end());
  scheduled[tp].erase(p);

  event_times.erase(callback);
  
  lock.Unlock();
  return true;
}
