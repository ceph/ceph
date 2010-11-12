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

#ifndef CEPH_TIMER_H
#define CEPH_TIMER_H

#include "Cond.h"
#include "Mutex.h"

#include <map>

class Context;
class SafeTimerThread;

class SafeTimer
{
  // This class isn't supposed to be copied
  SafeTimer(const SafeTimer &rhs);
  SafeTimer& operator=(const SafeTimer &rhs);

  Mutex& lock;
  Cond cond;

  friend class SafeTimerThread;
  SafeTimerThread *thread;

  void timer_thread();
  void _shutdown();

  std::multimap<utime_t, Context*> schedule;
  std::map<Context*, std::multimap<utime_t, Context*>::iterator> events;
  bool stopping;

  void dump(const char *caller = 0) const;

public:
  SafeTimer(Mutex &l);
  ~SafeTimer();

  /* Call with the event_lock UNLOCKED.
   *
   * Cancel all events and stop the timer thread.
   *
   * If there are any events that still have to run, they will need to take
   * the event_lock first. */
  void shutdown();

  /* Schedule an event in the future
   * Call with the event_lock LOCKED */
  void add_event_after(double seconds, Context *callback);
  void add_event_at(utime_t when, Context *callback);

  /* Cancel an event.
   * Call with the event_lock LOCKED
   *
   * Returns true if the callback was cancelled.
   * Returns false if you never addded the callback in the first place.
   */
  bool cancel_event(Context *callback);

  /* Cancel all events.
   * Call with the event_lock LOCKED
   *
   * When this function returns, all events have been cancelled, and there are no
   * more in progress.
   */
  void cancel_all_events();

};
#endif
