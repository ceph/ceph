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

#ifdef _WIN32
#define HAVE_POLL 1
#endif

#ifdef __sun
#include <sys/feature_tests.h>
#ifdef _DTRACE_VERSION
#define HAVE_EVPORT 1
#endif
#endif

#include <atomic>
#include <mutex>
#include <condition_variable>

#include "common/ceph_time.h"
#include "common/dout.h"
#include "net_handler.h"

#define EVENT_NONE 0
#define EVENT_READABLE 1
#define EVENT_WRITABLE 2

class EventCenter;

class EventCallback {

 public:
  virtual void do_request(uint64_t fd_or_id) = 0;
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
  virtual int init(EventCenter *center, int nevent) = 0;
  virtual int add_event(int fd, int cur_mask, int mask) = 0;
  virtual int del_event(int fd, int cur_mask, int del_mask) = 0;
  virtual int event_wait(std::vector<FiredFileEvent> &fired_events, struct timeval *tp) = 0;
  virtual int resize_events(int newsize) = 0;
  virtual bool need_wakeup() { return true; }
};

/*
 * EventCenter maintain a set of file descriptor and handle registered events.
 */
class EventCenter {
 public:
  // should be enough;
  static const int MAX_EVENTCENTER = 24;

 private:
  using clock_type = ceph::coarse_mono_clock;

  struct AssociatedCenters {
    EventCenter *centers[MAX_EVENTCENTER];
    AssociatedCenters() {
      // FIPS zeroization audit 20191115: this memset is not security related.
      memset(centers, 0, MAX_EVENTCENTER * sizeof(EventCenter*));
    }
  };

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

 public:
  /**
     * A Poller object is invoked once each time through the dispatcher's
     * inner polling loop.
     */
  class Poller {
   public:
    explicit Poller(EventCenter* center, const std::string& pollerName);
    virtual ~Poller();

    /**
     * This method is defined by a subclass and invoked once by the
     * center during each pass through its inner polling loop.
     *
     * \return
     *      1 means that this poller did useful work during this call.
     *      0 means that the poller found no work to do.
     */
    virtual int poll() = 0;

   private:
    /// The EventCenter object that owns this Poller.  NULL means the
    /// EventCenter has been deleted.
    EventCenter* owner;

    /// Human-readable string name given to the poller to make it
    /// easy to identify for debugging. For most pollers just passing
    /// in the subclass name probably makes sense.
    std::string poller_name;

    /// Index of this Poller in EventCenter::pollers.  Allows deletion
    /// without having to scan all the entries in pollers. -1 means
    /// this poller isn't currently in EventCenter::pollers (happens
    /// after EventCenter::reset).
    int slot;
  };

 private:
  CephContext *cct;
  std::string type;
  int nevent;
  // Used only to external event
  pthread_t owner = 0;
  std::mutex external_lock;
  std::atomic_ulong external_num_events;
  std::deque<EventCallbackRef> external_events;
  std::vector<FileEvent> file_events;
  EventDriver *driver;
  std::multimap<clock_type::time_point, TimeEvent> time_events;
  // Keeps track of all of the pollers currently defined.  We don't
  // use an intrusive list here because it isn't reentrant: we need
  // to add/remove elements while the center is traversing the list.
  std::vector<Poller*> pollers;
  std::map<uint64_t, std::multimap<clock_type::time_point, TimeEvent>::iterator> event_map;
  uint64_t time_event_next_id;
  int notify_receive_fd;
  int notify_send_fd;
  ceph::NetHandler net;
  EventCallbackRef notify_handler;
  unsigned center_id;
  AssociatedCenters *global_centers = nullptr;

  int process_time_events();
  FileEvent *_get_file_event(int fd) {
    ceph_assert(fd < nevent);
    return &file_events[fd];
  }

 public:
  explicit EventCenter(CephContext *c):
    cct(c), nevent(0),
    external_num_events(0),
    driver(NULL), time_event_next_id(1),
    notify_receive_fd(-1), notify_send_fd(-1), net(c),
    notify_handler(NULL), center_id(0) { }
  ~EventCenter();
  std::ostream& _event_prefix(std::ostream *_dout);

  int init(int nevent, unsigned center_id, const std::string &type);
  void set_owner();
  pthread_t get_owner() const { return owner; }
  unsigned get_id() const { return center_id; }

  EventDriver *get_driver() { return driver; }

  // Used by internal thread
  int create_file_event(int fd, int mask, EventCallbackRef ctxt);
  uint64_t create_time_event(uint64_t microseconds, EventCallbackRef ctxt);
  void delete_file_event(int fd, int mask);
  void delete_time_event(uint64_t id);
  int process_events(unsigned timeout_microseconds, ceph::timespan *working_dur = nullptr);
  void wakeup();

  // Used by external thread
  void dispatch_event_external(EventCallbackRef e);
  inline bool in_thread() const {
    return pthread_equal(pthread_self(), owner);
  }

 private:
  template <typename func>
  class C_submit_event : public EventCallback {
    std::mutex lock;
    std::condition_variable cond;
    bool done = false;
    func f;
    bool nonwait;
   public:
    C_submit_event(func &&_f, bool nowait)
      : f(std::move(_f)), nonwait(nowait) {}
    void do_request(uint64_t id) override {
      f();
      lock.lock();
      cond.notify_all();
      done = true;
      bool del = nonwait;
      lock.unlock();
      if (del)
        delete this;
    }
    void wait() {
      ceph_assert(!nonwait);
      std::unique_lock<std::mutex> l(lock);
      while (!done)
        cond.wait(l);
    }
  };

 public:
  template <typename func>
  void submit_to(int i, func &&f, bool always_async = false) {
    ceph_assert(i < MAX_EVENTCENTER && global_centers);
    EventCenter *c = global_centers->centers[i];
    ceph_assert(c);
    if (always_async) {
      C_submit_event<func> *event = new C_submit_event<func>(std::move(f), true);
      c->dispatch_event_external(event);
    } else if (c->in_thread()) {
      f();
      return;
    } else {
      C_submit_event<func> event(std::move(f), false);
      c->dispatch_event_external(&event);
      event.wait();
    }
  };
};

#endif
