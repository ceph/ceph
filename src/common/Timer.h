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


#ifndef __TIMER_H
#define __TIMER_H

#include "include/types.h"
#include "include/Context.h"
#include "Clock.h"

#include "Mutex.h"
#include "Cond.h"
#include "Thread.h"

#include <map>
#include <set>
using std::map;
using std::set;

#include <ext/hash_map>
using namespace __gnu_cxx;


/*** Timer
 * schedule callbacks
 */

//class Messenger;


namespace __gnu_cxx {
  template<> struct hash<Context*> {
    size_t operator()(const Context *p) const { 
      static hash<unsigned long> H;
      return H((unsigned long)p); 
    }
  };
}


class Timer {
 private:
  map< utime_t, set<Context*> >  scheduled;    // time -> (context ...)
  hash_map< Context*, utime_t >  event_times;  // event -> time

  bool get_next_due(utime_t &when);

  void register_timer();  // make sure i get a callback
  void cancel_timer();    // make sure i get a callback

  bool      thread_stop;
  Mutex     lock;
  bool      timed_sleep;
  bool      sleeping;
  Cond      sleep_cond;
  Cond      timeout_cond;

 public:
  void timer_entry();    // waiter thread (that wakes us up)

  class TimerThread : public Thread {
    Timer *t;
  public:
    void *entry() {
      t->timer_entry();
      return 0;
    }
    TimerThread(Timer *_t) : t(_t) {}
  } timer_thread;


  int num_event;


 public:
  Timer() :
    thread_stop(false),
    lock("Timer::lock"),
    timed_sleep(false),
    sleeping(false),
    timer_thread(this),
    num_event(0)
  { 
  }
  ~Timer() { 
    // stop.
    cancel_timer();

    // scheduled
    for (map< utime_t, set<Context*> >::iterator it = scheduled.begin();
         it != scheduled.end();
         it++) {
      for (set<Context*>::iterator sit = it->second.begin();
           sit != it->second.end();
           sit++)
        delete *sit;
    }
    scheduled.clear();
  }
  
  void init() {
    register_timer();
  }
  void shutdown() {
    cancel_timer();
    cancel_all_events();
  }

  // schedule events
  virtual void add_event_after(double seconds,
                       Context *callback);
  virtual void add_event_at(utime_t when,
                    Context *callback);
  virtual bool cancel_event(Context *callback);
  virtual void cancel_all_events();

  // execute pending events
  void execute_pending();

};


/*
 * SafeTimer is a wrapper around the raw Timer (or rather, g_timer, it's global
 * instantiation) that protects event execution with an existing mutex.  It 
 * provides for, among other things, reliable event cancellation on class
 * destruction.  The caller just needs to cancel each event (or cancel_all()),
 * and then call join() to ensure any concurrently exectuting events (in other
 * threads) get flushed.
 */
class SafeTimer : public Timer {
  Mutex&        lock;
  Cond          cond;
  map<Context*,Context*> scheduled;  // actual -> wrapper
  map<Context*,Context*> canceled;
  
  class EventWrapper : public Context {
    SafeTimer *timer;
    Context *actual;
  public:
    EventWrapper(SafeTimer *st, Context *c) : timer(st), 
					      actual(c) {}
    void finish(int r);
  };

public:
  SafeTimer(Mutex& l) : lock(l) { }
  ~SafeTimer();

  void add_event_after(double seconds, Context *c);
  void add_event_at(utime_t when, Context *c);
  bool cancel_event(Context *c);
  void cancel_all();
  void join();

  int get_num_scheduled() { return scheduled.size(); }
  int get_num_canceled() { return canceled.size(); }
};


// single global instance
extern Timer g_timer;



#endif
