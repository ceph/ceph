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
#define dout_prefix *_dout << "Event "

class C_handle_notify : public EventCallback {
 public:
  C_handle_notify() {}
  void do_request(int fd_or_id) {
    char c[100];
    assert(read(fd_or_id, c, 100));
  }
};

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
    return -1;
  }

  notify_receive_fd = fds[0];
  notify_send_fd = fds[1];
  r = net.set_nonblock(notify_receive_fd);
  if (r < 0) {
    return -1;
  }

  file_events = static_cast<FileEvent *>(malloc(sizeof(FileEvent)*n));
  memset(file_events, 0, sizeof(FileEvent)*n);

  nevent = n;
  create_file_event(notify_receive_fd, EVENT_READABLE, EventCallbackRef(new C_handle_notify()));
  return 0;
}

EventCenter::~EventCenter()
{
  if (driver)
    delete driver;

  if (notify_receive_fd > 0)
    ::close(notify_receive_fd);
  if (notify_send_fd > 0)
    ::close(notify_send_fd);
}

int EventCenter::create_file_event(int fd, int mask, EventCallbackRef ctxt)
{
  int r;
  if (fd > nevent) {
    int new_size = nevent << 2;
    while (fd > new_size)
      new_size <<= 2;
    ldout(cct, 10) << __func__ << " event count exceed " << nevent << ", expand to " << new_size << dendl;
    r = driver->resize_events(new_size);
    if (r < 0) {
      lderr(cct) << __func__ << " event count is exceed." << dendl;
      return -ERANGE;
    }
    FileEvent *new_events = static_cast<FileEvent *>(realloc(file_events, sizeof(FileEvent)*new_size));
    if (!new_events) {
      lderr(cct) << __func__ << " failed to realloc file_events" << cpp_strerror(errno) << dendl;
      return -errno;
    }
    file_events = new_events;
    nevent = new_size;
  }

  EventCenter::FileEvent *event = _get_file_event(fd);

  r = driver->add_event(fd, event->mask, mask);
  if (r < 0)
    return r;

  event->mask |= mask;
  if (mask & EVENT_READABLE) {
    event->read_cb = ctxt;
  }
  if (mask & EVENT_WRITABLE) {
    event->write_cb = ctxt;
  }
  ldout(cct, 10) << __func__ << " create event fd=" << fd << " mask=" << mask
                 << " now mask is " << event->mask << dendl;
  return 0;
}

void EventCenter::delete_file_event(int fd, int mask)
{
  EventCenter::FileEvent *event = _get_file_event(fd);
  if (!event->mask)
    return ;

  driver->del_event(fd, event->mask, mask);

  if (mask & EVENT_READABLE && event->read_cb) {
    event->read_cb.reset();
  }
  if (mask & EVENT_WRITABLE && event->write_cb) {
    event->write_cb.reset();
  }

  event->mask = event->mask & (~mask);
  ldout(cct, 10) << __func__ << " delete fd=" << fd << " mask=" << mask
                 << " now mask is " << event->mask << dendl;
}

uint64_t EventCenter::create_time_event(uint64_t microseconds, EventCallbackRef ctxt)
{
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

  return id;
}

void EventCenter::wakeup()
{
  ldout(cct, 1) << __func__ << dendl;
  char buf[1];
  buf[0] = 'c';
  // wake up "event_wait"
  int n = write(notify_send_fd, buf, 1);
  // FIXME ?
  assert(n == 1);
}

int EventCenter::process_time_events()
{
  int processed = 0;
  time_t now = time(NULL);
  utime_t cur = ceph_clock_now(cct);
  ldout(cct, 10) << __func__ << " cur time is " << cur << dendl;

  /* If the system clock is moved to the future, and then set back to the
   * right value, time events may be delayed in a random way. Often this
   * means that scheduled operations will not be performed soon enough.
   *
   * Here we try to detect system clock skews, and force all the time
   * events to be processed ASAP when this happens: the idea is that
   * processing events earlier is less dangerous than delaying them
   * indefinitely, and practice suggests it is. */
  if (now < last_time) {
    map<utime_t, list<TimeEvent> > changed;
    for (map<utime_t, list<TimeEvent> >::iterator it = time_events.begin();
         it != time_events.end(); ++it) {
      changed[utime_t()].swap(it->second);
    }
    time_events.swap(changed);
  }
  last_time = now;

  map<utime_t, list<TimeEvent> >::iterator prev;
  for (map<utime_t, list<TimeEvent> >::iterator it = time_events.begin();
       it != time_events.end(); ) {
    prev = it;
    if (cur >= it->first) {
      for (list<TimeEvent>::iterator j = it->second.begin();
           j != it->second.end(); ++j) {
        ldout(cct, 10) << __func__ << " process time event: id=" << j->id << " time is "
                      << it->first << dendl;
        j->time_cb->do_request(j->id);
      }
      processed++;
      ++it;
      time_events.erase(prev);
    } else {
      break;
    }
  }

  return processed;
}

int EventCenter::process_events(int timeout_microseconds)
{
  struct timeval tv;
  int numevents;
  bool trigger_time = false;

  utime_t period, shortest, now = ceph_clock_now(cct);
  now.copy_to_timeval(&tv);
  if (timeout_microseconds > 0) {
    tv.tv_sec += timeout_microseconds / 1000000;
    tv.tv_usec += timeout_microseconds % 1000000;
  }
  shortest.set_from_timeval(&tv);

  {
    map<utime_t, list<TimeEvent> >::iterator it = time_events.begin();
    if (it != time_events.end() && shortest >= it->first) {
      ldout(cct, 10) << __func__ << " shortest is " << shortest << " it->first is " << it->first << dendl;
      shortest = it->first;
      trigger_time = true;
      if (shortest > now) {
        period = now - shortest;
        period.copy_to_timeval(&tv);
      } else {
        tv.tv_sec = 0;
        tv.tv_usec = 0;
      }
    } else {
      tv.tv_sec = timeout_microseconds / 1000000;
      tv.tv_usec = timeout_microseconds % 1000000;
    }
  }

  ldout(cct, 10) << __func__ << " wait second " << tv.tv_sec << " usec " << tv.tv_usec << dendl;
  vector<FiredFileEvent> fired_events;
  numevents = driver->event_wait(fired_events, &tv);
  for (int j = 0; j < numevents; j++) {
    int rfired = 0;
    FileEvent *event = _get_file_event(fired_events[j].fd);
    if (!event)
      continue;

    /* note the event->mask & mask & ... code: maybe an already processed
    * event removed an element that fired and we still didn't
    * processed, so we check if the event is still valid. */
    if (event->mask & fired_events[j].mask & EVENT_READABLE) {
      rfired = 1;
      event->read_cb->do_request(fired_events[j].fd);
    }
    event = _get_file_event(fired_events[j].fd);
    if (!event)
      continue;

    if (event->mask & fired_events[j].mask & EVENT_WRITABLE) {
      if (!rfired || event->read_cb != event->write_cb)
        event->write_cb->do_request(fired_events[j].fd);
    }

    ldout(cct, 20) << __func__ << " event_wq process is " << fired_events[j].fd << " mask is " << fired_events[j].mask << dendl;
  }

  if (trigger_time)
    numevents += process_time_events();

  {
    lock.Lock();
    while (!external_events.empty()) {
      EventCallbackRef e = external_events.front();
      external_events.pop_front();
      lock.Unlock();
      e->do_request(0);
      lock.Lock();
    }
    lock.Unlock();
  }
  return numevents;
}

void EventCenter::dispatch_event_external(EventCallbackRef e)
{
  lock.Lock();
  external_events.push_back(e);
  lock.Unlock();
  wakeup();
}
