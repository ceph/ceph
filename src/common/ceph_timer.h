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

#ifndef COMMON_CEPH_TIMER_H
#define COMMON_CEPH_TIMER_H

#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <boost/intrusive/set.hpp>

#include "include/function2.hpp"
#include "include/compat.h"

#include "common/detail/construct_suspended.h"
#include "common/Thread.h"

namespace bi = boost::intrusive;
namespace ceph {

// Compared to the SafeTimer this does fewer allocations (you
// don't have to allocate a new Context every time you
// want to cue the next tick.)
//
// It also does not share a lock with the caller. If you call
// cancel event, it either cancels the event (and returns true) or
// you missed it. If this does not work for you, you can set up a
// flag and mutex of your own.
//
// You get to pick your clock. I like mono_clock, since I usually
// want to wait FOR a given duration. real_clock is worthwhile if
// you want to wait UNTIL a specific moment of wallclock time.  If
// you want you can set up a timer that executes a function after
// you use up ten seconds of CPU time.

template<typename TC>
class timer {
  using sh = bi::set_member_hook<bi::link_mode<bi::normal_link>>;

  struct event {
    typename TC::time_point t = typename TC::zero();
    std::uint64_t id = 0;
    fu2::unique_function<void()> f;

    sh schedule_link;
    sh event_link;

    event() = default;
    event(typename TC::time_point t, std::uint64_t id,
	  fu2::unique_function<void()> f) : t(t), id(id), f(std::move(f)) {}

    event(const event&) = delete;
    event& operator =(const event&) = delete;

    event(event&&) = delete;
    event& operator =(event&&) = delete;

    bool operator <(const event& e) const noexcept {
      return t == e.t ? id < e.id : t < e.t;
    }
  };
  struct id_key {
    using type = std::uint64_t;
    const type& operator ()(const event& e) const noexcept {
      return e.id;
    }
  };

  bi::set<event, bi::member_hook<event, sh, &event::schedule_link>,
	  bi::constant_time_size<false>> schedule;

  bi::set<event, bi::member_hook<event, sh, &event::event_link>,
	  bi::constant_time_size<false>,
	  bi::key_of_value<id_key>> events;

  std::mutex lock;
  std::condition_variable cond;

  event* running = nullptr;
  std::uint64_t next_id = 0;

  bool suspended;
  std::thread thread;

  void timer_thread() {
    std::unique_lock l(lock);
    while (!suspended) {
      auto now = TC::now();

      while (!schedule.empty()) {
	auto p = schedule.begin();
	// Should we wait for the future?
        #if defined(_WIN32)
        if (p->t - now > std::chrono::milliseconds(1)) {
          // std::condition_variable::wait_for uses SleepConditionVariableSRW
          // on Windows, which has millisecond precision. Deltas <1ms will
          // lead to busy loops, which should be avoided. This situation is
          // quite common since "wait_for" often returns ~1ms earlier than
          // requested.
          break;
        }
        #else // !_WIN32
        if (p->t > now) {
          break;
        }
        #endif

	auto& e = *p;
	schedule.erase(e);
	events.erase(e.id);

	// Since we have only one thread it is impossible to have more
	// than one running event
	running = &e;

	l.unlock();
	p->f();
	l.lock();

	if (running) {
	  running = nullptr;
	  delete &e;
	} // Otherwise the event requeued itself
      }

      if (suspended)
	break;
      if (schedule.empty()) {
	cond.wait(l);
      } else {
	// Since wait_until takes its parameter by reference, passing
	// the time /in the event/ is unsafe, as it might be canceled
	// while we wait.
	const auto t = schedule.begin()->t;
	cond.wait_until(l, t);
      }
    }
  }

public:
  timer() : suspended(false) {
    thread = std::thread(&timer::timer_thread, this);
    set_thread_name(thread, "ceph_timer");
  }

  // Create a suspended timer, jobs will be executed in order when
  // it is resumed.
  timer(construct_suspended_t) : suspended(true) {}

  timer(const timer&) = delete;
  timer& operator =(const timer&) = delete;

  ~timer() {
    suspend();
    cancel_all_events();
  }

  // Suspend operation of the timer (and let its thread die).
  void suspend() {
    std::unique_lock l(lock);
    if (suspended)
      return;

    suspended = true;
    cond.notify_one();
    l.unlock();
    thread.join();
  }

  // Resume operation of the timer. (Must have been previously
  // suspended.)
  void resume() {
    std::unique_lock l(lock);
    if (!suspended)
      return;

    suspended = false;
    assert(!thread.joinable());
    thread = std::thread(&timer::timer_thread, this);
  }

  // Schedule an event in the relative future
  template<typename Callable, typename... Args>
  std::uint64_t add_event(typename TC::duration duration,
			  Callable&& f, Args&&... args) {
    return add_event(TC::now() + duration,
		     std::forward<Callable>(f),
		     std::forward<Args>(args)...);
  }

  // Schedule an event in the absolute future
  template<typename Callable, typename... Args>
  std::uint64_t add_event(typename TC::time_point when,
			  Callable&& f, Args&&... args) {
    std::lock_guard l(lock);
    auto e = std::make_unique<event>(when, ++next_id,
				     std::bind(std::forward<Callable>(f),
					       std::forward<Args>(args)...));
    auto id = e->id;
    auto i = schedule.insert(*e);
    events.insert(*(e.release()));

    /* If the event we have just inserted comes before everything
     * else, we need to adjust our timeout. */
    if (i.first == schedule.begin())
      cond.notify_one();

    // Previously each event was a context, identified by a
    // pointer, and each context to be called only once. Since you
    // can queue the same function pointer, member function,
    // lambda, or functor up multiple times, identifying things by
    // function for the purposes of cancellation is no longer
    // suitable. Thus:
    return id;
  }

  // Adjust the timeout of a currently-scheduled event (relative)
  bool adjust_event(std::uint64_t id, typename TC::duration duration) {
    return adjust_event(id, TC::now() + duration);
  }

  // Adjust the timeout of a currently-scheduled event (absolute)
  bool adjust_event(std::uint64_t id, typename TC::time_point when) {
    std::lock_guard l(lock);

    auto it = events.find(id);

    if (it == events.end())
      return false;

    auto& e = *it;

    schedule.erase(e);
    e.t = when;
    schedule.insert(e);

    return true;
  }

  // Cancel an event. If the event has already come and gone (or you
  // never submitted it) you will receive false. Otherwise you will
  // receive true and it is guaranteed the event will not execute.
  bool cancel_event(const std::uint64_t id) {
    std::lock_guard l(lock);
    auto p = events.find(id);
    if (p == events.end()) {
      return false;
    }

    auto& e = *p;
    events.erase(e.id);
    schedule.erase(e);
    delete &e;

    return true;
  }

  // Reschedules a currently running event in the relative
  // future. Must be called only from an event executed by this
  // timer. If you have a function that can be called either from
  // this timer or some other way, it is your responsibility to make
  // sure it can tell the difference only does not call
  // reschedule_me in the non-timer case.
  //
  // Returns an event id. If you had an event_id from the first
  // scheduling, replace it with this return value.
  std::uint64_t reschedule_me(typename TC::duration duration) {
    return reschedule_me(TC::now() + duration);
  }

  // Reschedules a currently running event in the absolute
  // future. Must be called only from an event executed by this
  // timer. if you have a function that can be called either from
  // this timer or some other way, it is your responsibility to make
  // sure it can tell the difference only does not call
  // reschedule_me in the non-timer case.
  //
  // Returns an event id. If you had an event_id from the first
  // scheduling, replace it with this return value.
  std::uint64_t reschedule_me(typename TC::time_point when) {
    assert(std::this_thread::get_id() == thread.get_id());
    std::lock_guard l(lock);
    running->t = when;
    std::uint64_t id = ++next_id;
    running->id = id;
    schedule.insert(*running);
    events.insert(*running);

    // Hacky, but keeps us from being deleted
    running = nullptr;

    // Same function, but you get a new ID.
    return id;
  }

  // Remove all events from the queue.
  void cancel_all_events() {
    std::lock_guard l(lock);
    while (!events.empty()) {
      auto p = events.begin();
      event& e = *p;
      schedule.erase(e);
      events.erase(e.id);
      delete &e;
    }
  }
}; // timer
} // namespace ceph

#endif
