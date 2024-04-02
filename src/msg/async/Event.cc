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

#include "include/compat.h"
#include "common/errno.h"
#include <cerrno>
#include "Event.h"

#ifdef HAVE_DPDK
#include "dpdk/EventDPDK.h"
#endif

#ifdef HAVE_EPOLL
#include "EventEpoll.h"
#else
#ifdef HAVE_KQUEUE
#include "EventKqueue.h"
#else
#ifdef HAVE_POLL
#include "EventPoll.h"
#else
#include "EventSelect.h"
#endif
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
  void do_request(uint64_t fd_or_id) override {
    char c[256];
    int r = 0;
    do {
      #ifdef _WIN32
      r = recv(fd_or_id, c, sizeof(c), 0);
      #else
      r = read(fd_or_id, c, sizeof(c));
      #endif
      if (r < 0) {
        if (ceph_sock_errno() != EAGAIN)
          ldout(cct, 1) << __func__ << " read notify pipe failed: " << cpp_strerror(ceph_sock_errno()) << dendl;
      }
    } while (r > 0);
  }
};

#undef dout_prefix
#define dout_prefix _event_prefix(_dout)

/**
 * Construct a Poller.
 *
 * \param center
 *      EventCenter object through which the poller will be invoked (defaults
 *      to the global #RAMCloud::center object).
 * \param pollerName
 *      Human readable name that can be printed out in debugging messages
 *      about the poller. The name of the superclass is probably sufficient
 *      for most cases.
 */
EventCenter::Poller::Poller(EventCenter* center, const std::string& name)
    : owner(center), poller_name(name), slot(owner->pollers.size())
{
  owner->pollers.push_back(this);
}

/**
 * Destroy a Poller.
 */
EventCenter::Poller::~Poller()
{
  // Erase this Poller from the vector by overwriting it with the
  // poller that used to be the last one in the vector.
  //
  // Note: this approach is reentrant (it is safe to delete a
  // poller from a poller callback, which means that the poll
  // method is in the middle of scanning the list of all pollers;
  // the worst that will happen is that the poller that got moved
  // may not be invoked in the current scan).
  owner->pollers[slot] = owner->pollers.back();
  owner->pollers[slot]->slot = slot;
  owner->pollers.pop_back();
  slot = -1;
}

std::ostream& EventCenter::_event_prefix(std::ostream *_dout)
{
  return *_dout << "Event(" << this << " nevent=" << nevent
                << " time_id=" << time_event_next_id << ").";
}

int EventCenter::init(int nevent, unsigned center_id, const std::string &type)
{
  // can't init multi times
  ceph_assert(this->nevent == 0);

  this->type = type;
  this->center_id = center_id;

  if (type == "dpdk") {
#ifdef HAVE_DPDK
    driver = new DPDKDriver(cct);
#endif
  } else {
#ifdef HAVE_EPOLL
  driver = new EpollDriver(cct);
#else
#ifdef HAVE_KQUEUE
  driver = new KqueueDriver(cct);
#else
#ifdef HAVE_POLL
  driver = new PollDriver(cct);
#else
  driver = new SelectDriver(cct);
#endif
#endif
#endif
  }

  if (!driver) {
    lderr(cct) << __func__ << " failed to create event driver " << dendl;
    return -1;
  }

  int r = driver->init(this, nevent);
  if (r < 0) {
    lderr(cct) << __func__ << " failed to init event driver." << dendl;
    return r;
  }

  file_events.resize(nevent);
  this->nevent = nevent;

  if (!driver->need_wakeup())
    return 0;

  int fds[2];

  #ifdef _WIN32
  if (win_socketpair(fds) < 0) {
  #else
  if (pipe_cloexec(fds, 0) < 0) {
  #endif
    int e = ceph_sock_errno();
    lderr(cct) << __func__ << " can't create notify pipe: " << cpp_strerror(e) << dendl;
    return -e;
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

  return r;
}

EventCenter::~EventCenter()
{
  {
    std::lock_guard<std::mutex> l(external_lock);
    while (!external_events.empty()) {
      EventCallbackRef e = external_events.front();
      if (e)
        e->do_request(0);
      external_events.pop_front();
    }
  }
  time_events.clear();
  //assert(time_events.empty());

  if (notify_receive_fd >= 0)
    compat_closesocket(notify_receive_fd);
  if (notify_send_fd >= 0)
    compat_closesocket(notify_send_fd);

  delete driver;
  if (notify_handler)
    delete notify_handler;
}


void EventCenter::set_owner()
{
  owner = pthread_self();
  ldout(cct, 2) << __func__ << " center_id=" << center_id << " owner=" << owner << dendl;
  if (!global_centers) {
    global_centers = &cct->lookup_or_create_singleton_object<
      EventCenter::AssociatedCenters>(
	"AsyncMessenger::EventCenter::global_center::" + type, true);
    ceph_assert(global_centers);
    global_centers->centers[center_id] = this;
    if (driver->need_wakeup()) {
      notify_handler = new C_handle_notify(this, cct);
      int r = create_file_event(notify_receive_fd, EVENT_READABLE, notify_handler);
      ceph_assert(r == 0);
    }
  }
}

int EventCenter::create_file_event(int fd, int mask, EventCallbackRef ctxt)
{
  ceph_assert(in_thread());
  int r = 0;
  if (fd >= nevent) {
    int new_size = nevent << 2;
    while (fd >= new_size)
      new_size <<= 2;
    ldout(cct, 20) << __func__ << " event count exceed " << nevent << ", expand to " << new_size << dendl;
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
    lderr(cct) << __func__ << " add event failed, ret=" << r << " fd=" << fd
               << " mask=" << mask << " original mask is " << event->mask << dendl;
    ceph_abort_msg("BUG!");
    return r;
  }

  event->mask |= mask;
  if (mask & EVENT_READABLE) {
    event->read_cb = ctxt;
  }
  if (mask & EVENT_WRITABLE) {
    event->write_cb = ctxt;
  }
  ldout(cct, 20) << __func__ << " create event end fd=" << fd << " mask=" << mask
                 << " current mask is " << event->mask << dendl;
  return 0;
}

void EventCenter::delete_file_event(int fd, int mask)
{
  ceph_assert(in_thread() && fd >= 0);
  if (fd >= nevent) {
    ldout(cct, 1) << __func__ << " delete event fd=" << fd << " is equal or greater than nevent=" << nevent
                  << "mask=" << mask << dendl;
    return ;
  }
  EventCenter::FileEvent *event = _get_file_event(fd);
  ldout(cct, 30) << __func__ << " delete event started fd=" << fd << " mask=" << mask
                 << " original mask is " << event->mask << dendl;
  if (!event->mask)
    return ;

  int r = driver->del_event(fd, event->mask, mask);
  if (r < 0 && r != -ENOENT) {
    // if the socket fd is closed by the underlying nic driver, the
    // corresponding epoll item would be removed from the interest list, that'd
    // lead to ENOENT when removing the fd from the list.
    // see create_file_event
    ceph_abort_msg("BUG!");
  }

  if (mask & EVENT_READABLE && event->read_cb) {
    event->read_cb = nullptr;
  }
  if (mask & EVENT_WRITABLE && event->write_cb) {
    event->write_cb = nullptr;
  }

  event->mask = event->mask & (~mask);
  ldout(cct, 30) << __func__ << " delete event end fd=" << fd << " mask=" << mask
                 << " current mask is " << event->mask << dendl;
}

uint64_t EventCenter::create_time_event(uint64_t microseconds, EventCallbackRef ctxt)
{
  ceph_assert(in_thread());
  uint64_t id = time_event_next_id++;

  ldout(cct, 30) << __func__ << " id=" << id << " trigger after " << microseconds << "us"<< dendl;
  EventCenter::TimeEvent event;
  clock_type::time_point expire = clock_type::now() + std::chrono::microseconds(microseconds);
  event.id = id;
  event.time_cb = ctxt;
  std::multimap<clock_type::time_point, TimeEvent>::value_type s_val(expire, event);
  auto it = time_events.insert(std::move(s_val));
  event_map[id] = it;

  return id;
}

void EventCenter::delete_time_event(uint64_t id)
{
  ceph_assert(in_thread());
  ldout(cct, 30) << __func__ << " id=" << id << dendl;
  if (id >= time_event_next_id || id == 0)
    return ;

  auto it = event_map.find(id);
  if (it == event_map.end()) {
    ldout(cct, 10) << __func__ << " id=" << id << " not found" << dendl;
    return ;
  }

  time_events.erase(it->second);
  event_map.erase(it);
}

void EventCenter::wakeup()
{
  // No need to wake up since we never sleep
  if (!pollers.empty() || !driver->need_wakeup())
    return ;

  ldout(cct, 20) << __func__ << dendl;
  char buf = 'c';
  // wake up "event_wait"
  #ifdef _WIN32
  int n = send(notify_send_fd, &buf, sizeof(buf), 0);
  #else
  int n = write(notify_send_fd, &buf, sizeof(buf));
  #endif
  if (n < 0) {
    if (ceph_sock_errno() != EAGAIN) {
      ldout(cct, 1) << __func__ << " write notify pipe failed: "
                    << cpp_strerror(ceph_sock_errno()) << dendl;
      ceph_abort();
    }
  }
}

int EventCenter::process_time_events()
{
  int processed = 0;
  clock_type::time_point now = clock_type::now();
  using ceph::operator <<;
  ldout(cct, 30) << __func__ << " cur time is " << now << dendl;

  while (!time_events.empty()) {
    auto it = time_events.begin();
    if (now >= it->first) {
      TimeEvent &e = it->second;
      EventCallbackRef cb = e.time_cb;
      uint64_t id = e.id;
      time_events.erase(it);
      event_map.erase(id);
      ldout(cct, 30) << __func__ << " process time event: id=" << id << dendl;
      processed++;
      cb->do_request(id);
    } else {
      break;
    }
  }

  return processed;
}

int EventCenter::process_events(unsigned timeout_microseconds,  ceph::timespan *working_dur)
{
  struct timeval tv;
  int numevents;
  bool trigger_time = false;
  auto now = clock_type::now();
  clock_type::time_point end_time = now + std::chrono::microseconds(timeout_microseconds);

  auto it = time_events.begin();
  if (it != time_events.end() && end_time >= it->first) {
    trigger_time = true;
    end_time = it->first;

    if (end_time > now) {
      timeout_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end_time - now).count();
    } else {
      timeout_microseconds = 0;
    }
  }

  bool blocking = pollers.empty() && !external_num_events.load();
  if (!blocking)
    timeout_microseconds = 0;
  tv.tv_sec = timeout_microseconds / 1000000;
  tv.tv_usec = timeout_microseconds % 1000000;

  ldout(cct, 30) << __func__ << " wait second " << tv.tv_sec << " usec " << tv.tv_usec << dendl;
  std::vector<FiredFileEvent> fired_events;
  numevents = driver->event_wait(fired_events, &tv);
  auto working_start = ceph::mono_clock::now();
  for (int event_id = 0; event_id < numevents; event_id++) {
    int rfired = 0;
    FileEvent *event;
    EventCallbackRef cb;
    event = _get_file_event(fired_events[event_id].fd);

    /* note the event->mask & mask & ... code: maybe an already processed
    * event removed an element that fired and we still didn't
    * processed, so we check if the event is still valid. */
    if (event->mask & fired_events[event_id].mask & EVENT_READABLE) {
      rfired = 1;
      cb = event->read_cb;
      cb->do_request(fired_events[event_id].fd);
    }

    if (event->mask & fired_events[event_id].mask & EVENT_WRITABLE) {
      if (!rfired || event->read_cb != event->write_cb) {
        cb = event->write_cb;
        cb->do_request(fired_events[event_id].fd);
      }
    }

    ldout(cct, 30) << __func__ << " event_wq process is " << fired_events[event_id].fd
                   << " mask is " << fired_events[event_id].mask << dendl;
  }

  if (trigger_time)
    numevents += process_time_events();

  if (external_num_events.load()) {
    external_lock.lock();
    std::deque<EventCallbackRef> cur_process;
    cur_process.swap(external_events);
    external_num_events.store(0);
    external_lock.unlock();
    numevents += cur_process.size();
    while (!cur_process.empty()) {
      EventCallbackRef e = cur_process.front();
      ldout(cct, 30) << __func__ << " do " << e << dendl;
      e->do_request(0);
      cur_process.pop_front();
    }
  }

  if (!numevents && !blocking) {
    for (uint32_t i = 0; i < pollers.size(); i++)
      numevents += pollers[i]->poll();
  }

  if (working_dur)
    *working_dur = ceph::mono_clock::now() - working_start;
  return numevents;
}

void EventCenter::dispatch_event_external(EventCallbackRef e)
{
  uint64_t num = 0;
  {
    std::lock_guard lock{external_lock};
    if (external_num_events > 0 && *external_events.rbegin() == e) {
      return;
    }
    external_events.push_back(e);
    num = ++external_num_events;
  }
  if (num == 1 && !in_thread())
    wakeup();

  ldout(cct, 30) << __func__ << " " << e << " pending " << num << dendl;
}
