// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#include "include/Context.h"
#include "include/unordered_map.h"
#include "common/WorkQueue.h"

#define EVENT_NONE 0
#define EVENT_READABLE 1
#define EVENT_WRITABLE 2

class EventCenter;

class EventCallback {

 public:
  virtual void do_request(int fd_or_id) = 0;
  virtual ~EventCallback() {}       // we want a virtual destructor!!!
};

typedef ceph::shared_ptr<EventCallback> EventCallbackRef;

struct FiredFileEvent {
  int fd;
  int mask;
};

class EventDriver {
 public:
  virtual ~EventDriver() {}       // we want a virtual destructor!!!
  virtual int init(int nevent) = 0;
  virtual int add_event(int fd, int cur_mask, int mask) = 0;
  virtual void del_event(int fd, int cur_mask, int del_mask) = 0;
  virtual int event_wait(vector<FiredFileEvent> &fired_events, struct timeval *tp) = 0;
  virtual int resize_events(int newsize) = 0;
};

class EventCenter {
  struct FileEvent {
    int mask;
    EventCallbackRef read_cb;
    EventCallbackRef write_cb;
    FileEvent(): mask(0) {}
  };

  struct TimeEvent {
    uint64_t id;
    EventCallbackRef time_cb;

    TimeEvent(): id(0) {}
  };

  CephContext *cct;
  uint64_t nevent;
  // Used only to external event
  Mutex lock;
  deque<EventCallbackRef> external_events;
  unordered_map<int, FileEvent> file_events;
  EventDriver *driver;
  map<utime_t, list<TimeEvent> > time_events;
  uint64_t time_event_next_id;
  time_t last_time; // last time process time event
  int notify_receive_fd;
  int notify_send_fd;

  int process_time_events();
  FileEvent *_get_file_event(int fd) {
    unordered_map<int, FileEvent>::iterator it = file_events.find(fd);
    if (it != file_events.end()) {
      return &it->second;
    }
    return NULL;
  }

 public:
  EventCenter(CephContext *c):
    cct(c), nevent(0),
    lock("AsyncMessenger::lock"),
    driver(NULL), time_event_next_id(0),
    notify_receive_fd(-1), notify_send_fd(-1) {
    last_time = time(NULL);
  }
  ~EventCenter();
  int init(int nevent);
  // Used by internal thread
  int create_file_event(int fd, int mask, EventCallbackRef ctxt);
  uint64_t create_time_event(uint64_t milliseconds, EventCallbackRef ctxt);
  void delete_file_event(int fd, int mask);
  int process_events(int timeout_microseconds);
  void wakeup();

  // Used by external thread
  void dispatch_event_external(EventCallbackRef e);
};

#endif
