// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_MSG_EVENT_H
#define CEPH_MSG_EVENT_H

#ifdef __APPLE__
#include <AvailabilityMacros.h>
#endif

// We use epoll, kqueue, evport, select in descending order by performance.
#ifdef __linux__
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

#include "include/Context.h"
#include "common/WorkQueue.h"

#define EVENT_NONE 0
#define EVENT_READABLE 1
#define EVENT_WRITABLE 2

class EventCenter;

// Attention:
// This event library use file description as index to search correspond event
// in `events` and `fired_events`. So it's important to estimate a suitable
// capacity in calling eventcenterInit(capacity).

struct FiredEvent {
  int mask;
  int fd;

  FiredEvent(): mask(0), fd(0) {}
};

class EventDriver {
 public:
  virtual ~EventDriver() {}       // we want a virtual destructor!!!
  virtual int init(int nevent) = 0;
  virtual int add_event(int fd, int mask) = 0;
  virtual void delete_event(int fd, int del_mask) = 0;
  virtual int event_wait(FiredEvent &fired_events, struct timeval *tp) = 0;
};

class EventCallback {

 public:
  virtual void do_request(int fd, int mask) = 0;
  virtual ~EventCallback() {}       // we want a virtual destructor!!!
};

class EventCenter {
  struct Event {
    int mask;
    EventCallback *read_cb;
    EventCallback *write_cb;
    Event(): mask(0), read_cb(NULL), write_cb(NULL) {}
  };

  Mutex lock;
  map<int, Event> events;
  EventDriver *driver;
  CephContext *cct;
  int nevent;
  ThreadPool event_tp;

  Event *get_event(int fd) {
    Mutex::Locker l(lock);
    map<int, Event>::iterator it = events.find(fd);
    if (it != events.end()) {
      return &it->second;
    }

    return NULL;
  }
  struct EventWQ : public ThreadPool::WorkQueueVal<FiredEvent> {
    EventCenter *center;
    // In order to ensure the file descriptor is unique in conn_queue,
    // pending is introduced to check
    //
    // <File Descriptor>
    deque<int> conn_queue;
    // <File Descriptor, Mask>
    map<int, int> pending;

    EventWQ(EventCenter *c, time_t timeout, time_t suicide_timeout, ThreadPool *tp)
      : ThreadPool::WorkQueueVal<FiredEvent>("Event::EventWQ", timeout, suicide_timeout, tp), center(c) {}

    void _enqueue(FiredEvent e) {
      // Ensure only one thread process one file descriptor
      map<int, int>::iterator it = pending.find(e.fd);
      if (it != pending.end())
        it->second |= e.mask;
      else
        pending[e.fd] = e.mask;
    }
    void _enqueue_front(FiredEvent e) {
      assert(0);
    }
    void _dequeue(FiredEvent c) {
      assert(0);
    }
    bool _empty() {
      return conn_queue.empty();
    }
    FiredEvent _dequeue() {
      assert(!conn_queue.empty());
      FiredEvent e;
      e.fd = conn_queue.front();
      conn_queue.pop_front();
      assert(pending.count(e.fd));
      e.mask = pending[e.fd];
      pending.erase(e.fd);
      return e;
    }
    void _process(FiredEvent e, ThreadPool::TPHandle &handle) {
      int rfired = 0;
      Event *event = center->get_event(e.fd);
      if (!event)
        return ;

      /* note the event->mask & mask & ... code: maybe an already processed
       * event removed an element that fired and we still didn't
       * processed, so we check if the event is still valid. */
      if (event->mask & e.mask & EVENT_READABLE) {
        rfired = 1;
        event->read_cb->do_request(e.fd, e.mask);
      }
      if (event->mask & e.mask & EVENT_WRITABLE) {
        if (!rfired || event->read_cb != event->write_cb)
          event->write_cb->do_request(e.fd, e.mask);
      }
    }
    void _clear() {
      assert(conn_queue.empty());
    }
  } event_wq;

 public:
  EventCenter(CephContext *c):
    lock("EventCenter::lock"), driver(NULL), cct(c), nevent(0),
    event_tp(c, "EventCenter::event_tp", c->_conf->ms_event_op_threads, "eventcenter_op_threads"),
    event_wq(this, c->_conf->ms_event_thread_timeout, c->_conf->ms_event_thread_suicide_timeout, &event_tp) {}
  ~EventCenter();
  int init(int nevent);
  int create_event(int fd, int mask, EventCallback *ctxt);
  int delete_event(int fd, int mask);
  int process_events(int timeout_milliseconds);
};

#endif
