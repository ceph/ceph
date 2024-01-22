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
#include "Timer.h"


#define dout_subsys ceph_subsys_timer
#undef dout_prefix
#define dout_prefix *_dout << "timer(" << this << ")."

using std::pair;

using ceph::operator <<;

template <class Mutex>
class CommonSafeTimerThread : public Thread {
  CommonSafeTimer<Mutex> *parent;
public:
  explicit CommonSafeTimerThread(CommonSafeTimer<Mutex> *s) : parent(s) {}
  void *entry() override {
    parent->timer_thread();
    return NULL;
  }
};

template <class Mutex>
CommonSafeTimer<Mutex>::CommonSafeTimer(CephContext *cct_, Mutex &l, bool safe_callbacks)
  : cct(cct_), lock(l),
    safe_callbacks(safe_callbacks),
    thread(NULL),
    stopping(false)
{
}

template <class Mutex>
CommonSafeTimer<Mutex>::~CommonSafeTimer()
{
  ceph_assert(thread == NULL);
}

template <class Mutex>
void CommonSafeTimer<Mutex>::init()
{
  ldout(cct,10) << "init" << dendl;
  thread = new CommonSafeTimerThread<Mutex>(this);
  thread->create("safe_timer");
}

template <class Mutex>
void CommonSafeTimer<Mutex>::shutdown()
{
  ldout(cct,10) << "shutdown" << dendl;
  if (thread) {
    ceph_assert(ceph_mutex_is_locked(lock));
    cancel_all_events();
    stopping = true;
    cond.notify_all();
    lock.unlock();
    thread->join();
    lock.lock();
    delete thread;
    thread = NULL;
  }
}

template <class Mutex>
void CommonSafeTimer<Mutex>::timer_thread()
{
  std::unique_lock l{lock};
  ldout(cct,10) << "timer_thread starting" << dendl;
  while (!stopping) {
    auto now = clock_t::now();

    while (!schedule.empty()) {
      auto p = schedule.begin();

      // is the future now?
      if (p->first > now)
	break;

      ldout(cct, 20) << "timer_thread going to execute and remove the top of a schedule sized " << schedule.size() << dendl;
      Context *callback = p->second;
      events.erase(callback);
      schedule.erase(p);
      ldout(cct,10) << "timer_thread executing " << callback << dendl;
      
      if (!safe_callbacks) {
	l.unlock();
	callback->complete(0);
	l.lock();
      } else {
	callback->complete(0);
      }
    }

    // recheck stopping if we dropped the lock
    if (!safe_callbacks && stopping)
      break;

    if (schedule.empty()) {
      ldout(cct, 20) << "timer_thread going to sleep with an empty schedule" << dendl;
      cond.wait(l);
    } else {
      ldout(cct, 20) << "timer_thread going to sleep with a schedule size " << schedule.size() << dendl;
      auto when = schedule.begin()->first;
      cond.wait_until(l, when);
    }
    ldout(cct,20) << "timer_thread awake" << dendl;
  }
  ldout(cct,10) << "timer_thread exiting" << dendl;
}

template <class Mutex>
Context* CommonSafeTimer<Mutex>::add_event_after(double seconds, Context *callback)
{
  return add_event_after(ceph::make_timespan(seconds), callback);
}

template <class Mutex>
Context* CommonSafeTimer<Mutex>::add_event_after(ceph::timespan duration, Context *callback)
{
  ceph_assert(ceph_mutex_is_locked(lock));

  auto when = clock_t::now() + duration;
  return add_event_at(when, callback);
}

template <class Mutex>
Context* CommonSafeTimer<Mutex>::add_event_at(CommonSafeTimer<Mutex>::clock_t::time_point when, Context *callback)
{
  ceph_assert(ceph_mutex_is_locked(lock));
  ldout(cct,10) << __func__ << " " << when << " -> " << callback << dendl;
  if (stopping) {
    ldout(cct,5) << __func__ << " already shutdown, event not added" << dendl;
    delete callback;
    return nullptr;
  }
  scheduled_map_t::value_type s_val(when, callback);
  scheduled_map_t::iterator i = schedule.insert(s_val);

  event_lookup_map_t::value_type e_val(callback, i);
  pair < event_lookup_map_t::iterator, bool > rval(events.insert(e_val));

  /* If you hit this, you tried to insert the same Context* twice. */
  ceph_assert(rval.second);

  /* If the event we have just inserted comes before everything else, we need to
   * adjust our timeout. */
  if (i == schedule.begin())
    cond.notify_all();
  return callback;
}

template <class Mutex>
Context* CommonSafeTimer<Mutex>::add_event_at(ceph::real_clock::time_point when, Context *callback)
{
  ceph_assert(ceph_mutex_is_locked(lock));
  // convert from real_clock to mono_clock
  auto mono_now = ceph::mono_clock::now();
  auto real_now = ceph::real_clock::now();
  const auto delta = when - real_now;
  const auto mono_atime = (mono_now +
			   std::chrono::ceil<clock_t::duration>(delta));
  return add_event_at(mono_atime, callback);
}

template <class Mutex>
bool CommonSafeTimer<Mutex>::cancel_event(Context *callback)
{
  ceph_assert(ceph_mutex_is_locked(lock));
  
  auto p = events.find(callback);
  if (p == events.end()) {
    ldout(cct,10) << "cancel_event " << callback << " not found" << dendl;
    return false;
  }

  ldout(cct,10) << "cancel_event " << p->second->first << " -> " << callback << dendl;
  delete p->first;

  schedule.erase(p->second);
  events.erase(p);
  return true;
}

template <class Mutex>
void CommonSafeTimer<Mutex>::cancel_all_events()
{
  ldout(cct,10) << "cancel_all_events" << dendl;
  ceph_assert(ceph_mutex_is_locked(lock));

  while (!events.empty()) {
    auto p = events.begin();
    ldout(cct,10) << " cancelled " << p->second->first << " -> " << p->first << dendl;
    delete p->first;
    schedule.erase(p->second);
    events.erase(p);
  }
}

template <class Mutex>
void CommonSafeTimer<Mutex>::dump(const char *caller) const
{
  if (!caller)
    caller = "";
  ldout(cct,10) << "dump " << caller << dendl;

  for (scheduled_map_t::const_iterator s = schedule.begin();
       s != schedule.end();
       ++s)
    ldout(cct,10) << " " << s->first << "->" << s->second << dendl;
}

template class CommonSafeTimer<ceph::mutex>;
template class CommonSafeTimer<ceph::fair_mutex>;
