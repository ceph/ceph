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

#ifndef CEPH_MSG_EVENT_H
#define CEPH_MSG_EVENT_H

#ifdef __APPLE__
#include <AvailabilityMacros.h>
#endif

// We use epoll, kqueue, evport, select in descending order by performance.
#if defined(__linux__)
#define HAVE_EPOLL 1
#endif

#if (defined(__APPLE__) && defined(MAC_OS_X_VERSION_10_6)) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined (__NetBSD__)
#define HAVE_KQUEUE 1
#endif

#ifdef __sun
#include <sys/feature_tests.h>
#ifdef _DTRACE_VERSION
#define HAVE_EVPORT 1
#endif
#endif

#include <pthread.h>

#include <atomic>
#include <mutex>
#include <condition_variable>

#include "include/Context.h"
#include "include/unordered_map.h"
#include "common/ceph_time.h"
#include "common/WorkQueue.h"
#include "net_handler.h"

#define EVENT_NONE 0
#define EVENT_READABLE 1
#define EVENT_WRITABLE 2

class EventCenter;

class EventCallback {

 public:
  virtual void do_request(int fd_or_id) = 0;
  virtual ~EventCallback() {}       // we want a virtual destructor!!!
};

typedef EventCallback* EventCallbackRef;

struct FiredFileEvent {
  int fd;
  int mask;
};

/*
 * EventDriver is a wrap of event mechanisms depends on different OS.
 * For example, Linux will use epoll(2), BSD will use kqueue(2) and select will
 * be used for worst condition.
 */
class EventDriver {
 public:
  virtual ~EventDriver() {}       // we want a virtual destructor!!!
  virtual int init(int nevent) = 0;
  virtual int add_event(int fd, int cur_mask, int mask) = 0;
  virtual int del_event(int fd, int cur_mask, int del_mask) = 0;
  virtual int event_wait(vector<FiredFileEvent> &fired_events, struct timeval *tp) = 0;
  virtual int resize_events(int newsize) = 0;
};


/*
 * EventCenter maintain a set of file descriptor and handle registered events.
 */
class EventCenter {
  using clock_type = ceph::coarse_mono_clock;
  thread_local static pthread_t thread_id;

  struct FileEvent {
    int mask;
    EventCallbackRef read_cb;
    EventCallbackRef write_cb;
    FileEvent(): mask(0), read_cb(NULL), write_cb(NULL) {}
  };

  struct TimeEvent {
    uint64_t id;
    EventCallbackRef time_cb;

    TimeEvent(): id(0), time_cb(NULL) {}
  };

  CephContext *cct;
  int nevent;
  // Used only to external event
  Mutex external_lock;
  std::atomic_ulong external_num_events;
  deque<EventCallbackRef> external_events;
  vector<FileEvent> file_events;
  EventDriver *driver;
  map<clock_type::time_point, list<TimeEvent> > time_events;
  uint64_t time_event_next_id;
  clock_type::time_point last_time; // last time process time event
  clock_type::time_point next_time; // next wake up time
  int notify_receive_fd;
  int notify_send_fd;
  NetHandler net;
  pthread_t owner = 0;
  EventCallbackRef notify_handler;

  int process_time_events();
  FileEvent *_get_file_event(int fd) {
    assert(fd < nevent);
    return &file_events[fd];
  }

 public:
  atomic_t already_wakeup;

  explicit EventCenter(CephContext *c):
    cct(c), nevent(0),
    external_lock("AsyncMessenger::external_lock"),
    external_num_events(0),
    driver(NULL), time_event_next_id(1),
    notify_receive_fd(-1), notify_send_fd(-1), net(c), owner(0),
    notify_handler(NULL),
    already_wakeup(0) {
    last_time = clock_type::now();
  }
  ~EventCenter();
  ostream& _event_prefix(std::ostream *_dout);

  int init(int nevent);
  void set_owner();
  pthread_t get_owner() { return owner; }

  // Used by internal thread
  int create_file_event(int fd, int mask, EventCallbackRef ctxt);
  uint64_t create_time_event(uint64_t milliseconds, EventCallbackRef ctxt);
  void delete_file_event(int fd, int mask);
  void delete_time_event(uint64_t id);
  int process_events(int timeout_microseconds);
  void wakeup();

  // Used by external thread
  void dispatch_event_external(EventCallbackRef e);
  inline bool in_thread() const {
    return thread_id == owner;
  }
 private:
  template <typename func>
  class C_submit_event : public EventCallback {
    std::mutex lock;
    std::condition_variable cond;
    bool done = false;
    func &&f;
   public:
    C_submit_event(func &&_f): f(std::move(_f)) {}
    void do_request(int id) {
      f();
      std::lock_guard<std::mutex> l(lock);
      cond.notify_all();
      done = true;
    }
    void wait() {
      std::unique_lock<std::mutex> l(lock);
      while (!done)
        cond.wait(l);
    }
  };
 public:
  template <typename func>
  void submit_event(func &&f) {
    if (in_thread()) {
      f();
      return ;
    }
    C_submit_event<func> event(std::move(f));
    dispatch_event_external(&event);
    event.wait();
  };
};

#endif
