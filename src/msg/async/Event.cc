// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <time.h>

#include "common/errno.h"
#include "Event.h"

#ifdef HAVE_EPOLL
#include "EventEpoll.h"
#else
#ifdef HAVE_KQUEUE
#include "EventKqueue.h"
#else
#include "EventSelect.h"
#endif
#endif

#define dout_subsys ceph_subsys_ms

#undef dout_prefix
#define dout_prefix *_dout << "EventCallback "
class C_handle_notify : public EventCallback {
  EventCenter *center;
  CephContext *cct;

 public:
  C_handle_notify(EventCenter *c, CephContext *cc): center(c), cct(cc) {}
  void do_request(int fd_or_id) {
    char c[256];
    do {
      center->already_wakeup.set(0);
      int r = read(fd_or_id, c, sizeof(c));
      if (r < 0) {
        ldout(cct, 1) << __func__ << " read notify pipe failed: " << cpp_strerror(errno) << dendl;
        break;
      }
    } while (center->already_wakeup.read());
  }
};

#undef dout_prefix
#define dout_prefix _event_prefix(_dout)

ostream& EventCenter::_event_prefix(std::ostream *_dout)
{
  return *_dout << "Event(" << this << " owner=" << get_owner() << " nevent=" << nevent
                << " time_id=" << time_event_next_id << ").";
}

static thread_local pthread_t thread_id = 0;

int EventCenter::init(int n)
{
  // can't init multi times
  assert(nevent == 0);
#ifdef HAVE_EPOLL
  driver = new EpollDriver(cct);
#else
#ifdef HAVE_KQUEUE
  driver = new KqueueDriver(cct);
#else
  driver = new SelectDriver(cct);
#endif
#endif

  if (!driver) {
    lderr(cct) << __func__ << " failed to create event driver " << dendl;
    return -1;
  }

  int r = driver->init(n);
  if (r < 0) {
    lderr(cct) << __func__ << " failed to init event driver." << dendl;
    return r;
  }

  int fds[2];
  if (pipe(fds) < 0) {
    lderr(cct) << __func__ << " can't create notify pipe" << dendl;
    return -errno;
  }

  notify_receive_fd = fds[0];
  notify_send_fd = fds[1];
  r = net.set_nonblock(notify_receive_fd);
  if (r < 0) {
    return r;
  }
  r = net.set_nonblock(notify_send_fd);
  if (r < 0) {
    return r;
  }

  file_events.resize(n);
  nevent = n;
  notify_handler = new C_handle_notify(this, cct),
  r = create_file_event(notify_receive_fd, EVENT_READABLE, notify_handler);
  if (r < 0)
    return r;
  return 0;
}

EventCenter::~EventCenter()
{
  {
    Mutex::Locker l(external_lock);
    while (!external_events.empty()) {
      EventCallbackRef e = external_events.front();
      if (e)
        e->do_request(0);
      external_events.pop_front();
    }
  }
  assert(time_events.empty());

  if (notify_receive_fd >= 0) {
    delete_file_event(notify_receive_fd, EVENT_READABLE);
    ::close(notify_receive_fd);
  }
  if (notify_send_fd >= 0)
    ::close(notify_send_fd);

  delete driver;
  delete notify_handler;
}


void EventCenter::set_owner()
{
  thread_id = owner = pthread_self();
}

int EventCenter::create_file_event(int fd, int mask, EventCallbackRef ctxt)
{
  int r = 0;
  Mutex::Locker l(file_lock);
  if (fd >= nevent) {
    int new_size = nevent << 2;
    while (fd > new_size)
      new_size <<= 2;
    ldout(cct, 10) << __func__ << " event count exceed " << nevent << ", expand to " << new_size << dendl;
    r = driver->resize_events(new_size);
    if (r < 0) {
      lderr(cct) << __func__ << " event count is exceed." << dendl;
      return -ERANGE;
    }
    file_events.resize(new_size);
    nevent = new_size;
  }

  EventCenter::FileEvent *event = _get_file_event(fd);
  ldout(cct, 20) << __func__ << " create event started fd=" << fd << " mask=" << mask
                 << " original mask is " << event->mask << dendl;
  if (event->mask == mask)
    return 0;

  r = driver->add_event(fd, event->mask, mask);
  if (r < 0) {
    // Actually we don't allow any failed error code, caller doesn't prepare to
    // handle error status. So now we need to assert failure here. In practice,
    // add_event shouldn't report error, otherwise it must be a innermost bug!
    assert(0 == "BUG!");
    return r;
  }

  event->mask |= mask;
  if (mask & EVENT_READABLE) {
    event->read_cb = ctxt;
  }
  if (mask & EVENT_WRITABLE) {
    event->write_cb = ctxt;
  }
  ldout(cct, 10) << __func__ << " create event end fd=" << fd << " mask=" << mask
                 << " original mask is " << event->mask << dendl;
  return 0;
}

void EventCenter::delete_file_event(int fd, int mask)
{
  assert(fd >= 0);
  Mutex::Locker l(file_lock);
  if (fd >= nevent) {
    ldout(cct, 1) << __func__ << " delete event fd=" << fd << " is equal or greater than nevent=" << nevent
                  << "mask=" << mask << dendl;
    return ;
  }
  EventCenter::FileEvent *event = _get_file_event(fd);
  ldout(cct, 20) << __func__ << " delete event started fd=" << fd << " mask=" << mask
                 << " original mask is " << event->mask << dendl;
  if (!event->mask)
    return ;

  int r = driver->del_event(fd, event->mask, mask);
  if (r < 0) {
    // see create_file_event
    assert(0 == "BUG!");
  }

  if (mask & EVENT_READABLE && event->read_cb) {
    event->read_cb = nullptr;
  }
  if (mask & EVENT_WRITABLE && event->write_cb) {
    event->write_cb = nullptr;
  }

  event->mask = event->mask & (~mask);
  ldout(cct, 10) << __func__ << " delete event end fd=" << fd << " mask=" << mask
                 << " original mask is " << event->mask << dendl;
}

uint64_t EventCenter::create_time_event(uint64_t microseconds, EventCallbackRef ctxt)
{
  Mutex::Locker l(time_lock);
  uint64_t id = time_event_next_id++;

  ldout(cct, 10) << __func__ << " id=" << id << " trigger after " << microseconds << "us"<< dendl;
  EventCenter::TimeEvent event;
  utime_t expire;
  struct timeval tv;

  if (microseconds < 5) {
    tv.tv_sec = 0;
    tv.tv_usec = microseconds;
  } else {
    expire = ceph_clock_now(cct);
    expire.copy_to_timeval(&tv);
    tv.tv_sec += microseconds / 1000000;
    tv.tv_usec += microseconds % 1000000;
  }
  expire.set_from_timeval(&tv);

  event.id = id;
  event.time_cb = ctxt;
  time_events[expire].push_back(event);
  if (expire < next_time)
    wakeup();

  return id;
}

// TODO: Ineffective implementation now!
void EventCenter::delete_time_event(uint64_t id)
{
  Mutex::Locker l(time_lock);
  ldout(cct, 10) << __func__ << " id=" << id << dendl;
  if (id >= time_event_next_id)
    return ;

  for (map<utime_t, list<TimeEvent> >::iterator it = time_events.begin();
       it != time_events.end(); ++it) {
    for (list<TimeEvent>::iterator j = it->second.begin();
         j != it->second.end(); ++j) {
      if (j->id == id) {
        it->second.erase(j);
        if (it->second.empty())
          time_events.erase(it);
        return ;
      }
    }
  }
}

void EventCenter::wakeup()
{
  if (already_wakeup.compare_and_swap(0, 1)) {
    ldout(cct, 1) << __func__ << dendl;
    char buf[1];
    buf[0] = 'c';
    // wake up "event_wait"
    int n = write(notify_send_fd, buf, 1);
    // FIXME ?
    assert(n == 1);
  }
}

int EventCenter::process_time_events()
{
  int processed = 0;
  time_t now = time(NULL);
  utime_t cur = ceph_clock_now(cct);
  ldout(cct, 10) << __func__ << " cur time is " << cur << dendl;

  time_lock.Lock();
  /* If the system clock is moved to the future, and then set back to the
   * right value, time events may be delayed in a random way. Often this
   * means that scheduled operations will not be performed soon enough.
   *
   * Here we try to detect system clock skews, and force all the time
   * events to be processed ASAP when this happens: the idea is that
   * processing events earlier is less dangerous than delaying them
   * indefinitely, and practice suggests it is. */
  bool clock_skewed = false;
  if (now < last_time) {
    clock_skewed = true;
  }
  last_time = now;

  map<utime_t, list<TimeEvent> >::iterator prev;
  list<TimeEvent> need_process;
  for (map<utime_t, list<TimeEvent> >::iterator it = time_events.begin();
       it != time_events.end(); ) {
    prev = it;
    if (cur >= it->first || clock_skewed) {
      need_process.splice(need_process.end(), it->second);
      ++it;
      time_events.erase(prev);
    } else {
      break;
    }
  }
  time_lock.Unlock();

  for (list<TimeEvent>::iterator it = need_process.begin();
       it != need_process.end(); ++it) {
    ldout(cct, 10) << __func__ << " process time event: id=" << it->id << dendl;
    it->time_cb->do_request(it->id);
    processed++;
  }

  return processed;
}

int EventCenter::process_events(int timeout_microseconds)
{
  // Must set owner before looping
  assert(owner);
  struct timeval tv;
  int numevents;
  bool trigger_time = false;

  utime_t now = ceph_clock_now(cct);;
  // If exists external events, don't block
  if (external_num_events.read()) {
    tv.tv_sec = 0;
    tv.tv_usec = 0;
    next_time = now;
  } else {
    utime_t period, shortest;
    now.copy_to_timeval(&tv);
    if (timeout_microseconds > 0) {
      tv.tv_sec += timeout_microseconds / 1000000;
      tv.tv_usec += timeout_microseconds % 1000000;
    }
    shortest.set_from_timeval(&tv);

    Mutex::Locker l(time_lock);
    map<utime_t, list<TimeEvent> >::iterator it = time_events.begin();
    if (it != time_events.end() && shortest >= it->first) {
      ldout(cct, 10) << __func__ << " shortest is " << shortest << " it->first is " << it->first << dendl;
      shortest = it->first;
      trigger_time = true;
      if (shortest > now) {
        period = shortest - now;
        period.copy_to_timeval(&tv);
      } else {
        tv.tv_sec = 0;
        tv.tv_usec = 0;
      }
    } else {
      tv.tv_sec = timeout_microseconds / 1000000;
      tv.tv_usec = timeout_microseconds % 1000000;
    }
    next_time = shortest;
  }

  ldout(cct, 10) << __func__ << " wait second " << tv.tv_sec << " usec " << tv.tv_usec << dendl;
  vector<FiredFileEvent> fired_events;
  numevents = driver->event_wait(fired_events, &tv);
  file_lock.Lock();
  for (int j = 0; j < numevents; j++) {
    int rfired = 0;
    FileEvent *event;
    EventCallbackRef cb;
    event = _get_file_event(fired_events[j].fd);

    // FIXME: Actually we need to pick up some ways to reduce potential
    // file_lock contention here.
    /* note the event->mask & mask & ... code: maybe an already processed
    * event removed an element that fired and we still didn't
    * processed, so we check if the event is still valid. */
    if (event->mask & fired_events[j].mask & EVENT_READABLE) {
      rfired = 1;
      cb = event->read_cb;
      file_lock.Unlock();
      cb->do_request(fired_events[j].fd);
      file_lock.Lock();
    }

    if (event->mask & fired_events[j].mask & EVENT_WRITABLE) {
      if (!rfired || event->read_cb != event->write_cb) {
        cb = event->write_cb;
        file_lock.Unlock();
        cb->do_request(fired_events[j].fd);
        file_lock.Lock();
      }
    }

    ldout(cct, 20) << __func__ << " event_wq process is " << fired_events[j].fd << " mask is " << fired_events[j].mask << dendl;
  }
  file_lock.Unlock();

  if (trigger_time)
    numevents += process_time_events();

  if (external_num_events.read()) {
    external_lock.Lock();
    if (external_events.empty()) {
      external_lock.Unlock();
    } else {
      deque<EventCallbackRef> cur_process;
      cur_process.swap(external_events);
      external_num_events.set(0);
      external_lock.Unlock();
      while (!cur_process.empty()) {
        EventCallbackRef e = cur_process.front();
        if (e)
          e->do_request(0);
        cur_process.pop_front();
      }
    }
  }
  return numevents;
}

void EventCenter::dispatch_event_external(EventCallbackRef e)
{
  external_lock.Lock();
  external_events.push_back(e);
  uint64_t num = external_num_events.inc();
  external_lock.Unlock();
  if (thread_id != owner)
    wakeup();

  ldout(cct, 10) << __func__ << " " << e << " pending " << num << dendl;
}
