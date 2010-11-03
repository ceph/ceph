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

#include "Clock.h"
#include "Cond.h"
#include "Mutex.h"
#include "include/types.h"

#include <list>
#include <map>

class Context;
class TimerThread;

/* Timer
 *
 * An instance of the timer class holds a thread which executes callbacks at
 * predetermined times.
 */
class Timer
{
public:
  Timer();

  /* Calls shutdown() */
  ~Timer();

  /* Cancel all events and stop the timer thread.
   *
   * This function might block for a while because it does a thread.join().
   * */
  void shutdown();

  /* Schedule an event in the future */
  void add_event_after(double seconds, Context *callback);
  void add_event_at(utime_t when, Context *callback);

  /* Cancel an event.
   *
   * If this function returns true, you know that the callback has been
   * destroyed and is not currently running.
   * If it returns false, either the callback is in progress, or you never addded
   * the callback in the first place.
   */
  bool cancel_event(Context *callback);

  /* Cancel all events.
   *
   * Even after this function returns, there may be events in progress.
   * Use SafeTimer if you have to be sure that nothing is running after
   * cancelling an event or events.
   */
  void cancel_all_events();

private:
  Timer(Mutex *event_lock_);

  /* Starts the timer thread.
   * Returns 0 on success; error code otherwise. */
  int init();

  bool cancel_event_impl(Context *callback, bool cancel_running);

  void cancel_all_events_impl(bool clear_running);

  void pop_running(std::list <Context*> &running_, const utime_t &now);

  std::string show_all_events(const char *caller) const;

  // This class isn't supposed to be copied
  Timer(const Timer &rhs);
  Timer& operator=(const Timer &rhs);

  Mutex lock;
  Mutex *event_lock;
  Cond cond;
  TimerThread *thread;
  bool exiting;
  std::multimap < utime_t, Context* > scheduled;
  std::map < Context*, std::multimap < utime_t, Context* >::iterator > events;
  std::list<Context*> running;

  friend class TimerThread;
  friend class SafeTimer;
};

/*
 * SafeTimer is a wrapper around the a Timer that protects event
 * execution with an existing mutex.  It provides for, among other
 * things, reliable event cancellation in cancel_event. Unlike in
 * Timer::cancel_event, the caller can be sure that once SafeTimer::cancel_event
 * returns, the callback will not be in progress.
 */
class SafeTimer
{
public:
  SafeTimer(Mutex &event_lock_);
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

private:
  // This class isn't supposed to be copied
  SafeTimer(const SafeTimer &rhs);
  SafeTimer& operator=(const SafeTimer &rhs);

  Timer t;
};
#endif
