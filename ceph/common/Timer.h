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
using namespace std;

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
  map< utime_t, multiset<Context*> >  scheduled;    // time -> (context ...)
  hash_map< Context*, utime_t >  event_times;  // event -> time

  // get time of the next event
  Context* get_next_scheduled(utime_t& when) {
    if (scheduled.empty()) return 0;
    map< utime_t, multiset<Context*> >::iterator it = scheduled.begin();
    when = it->first;
    multiset<Context*>::iterator sit = it->second.begin();
    return *sit;
  }

  void register_timer();  // make sure i get a callback
  void cancel_timer();    // make sure i get a callback

  //pthread_t thread_id;
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
    //thread_id0),
    thread_stop(false),
    timed_sleep(false),
    sleeping(false),
    timer_thread(this),
    num_event(0)
  { 
  }
  ~Timer() { 
    // scheduled
    for (map< utime_t, multiset<Context*> >::iterator it = scheduled.begin();
         it != scheduled.end();
         it++) {
      for (multiset<Context*>::iterator sit = it->second.begin();
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
  }

  /*
  void set_messenger_kicker(Context *c);
  void unset_messenger_kicker();

  void set_messenger(Messenger *m);
  void unset_messenger();
  */

  // schedule events
  void add_event_after(float seconds,
                       Context *callback);
  void add_event_at(utime_t when,
                    Context *callback);
  bool cancel_event(Context *callback);

  // execute pending events
  void execute_pending();

};


// single global instance
extern Timer g_timer;



#endif
