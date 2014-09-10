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

struct FiredTimeEvent {
  uint64_t id;
  EventCallbackRef time_cb;
};

struct FiredEvent {
  FiredFileEvent file_event;
  FiredTimeEvent time_event;
  bool is_file;

  FiredEvent(): is_file(true) {}
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

  Mutex lock;
  map<int, FileEvent> file_events;
  // The second element is id
  map<utime_t, uint64_t> time_to_ids;
  // The first element is id
  unordered_map<uint64_t, TimeEvent> time_events;
  EventDriver *driver;
  CephContext *cct;
  uint64_t nevent;
  uint64_t time_event_next_id;
  ThreadPool event_tp;
  time_t last_time; // last time process time event
  int notify_receive_fd;
  int notify_send_fd;
  utime_t next_wake;
  bool tp_stop;

  int process_time_events();
  FileEvent *_get_file_event(int fd) {
    map<int, FileEvent>::iterator it = file_events.find(fd);
    if (it != file_events.end()) {
      return &it->second;
    }
    return NULL;
  }

  struct EventWQ : public ThreadPool::WorkQueueVal<FiredEvent> {
    EventCenter *center;
    // In order to ensure the file descriptor is unique in conn_queue,
    // pending is introduced to check
    //
    deque<FiredEvent> conn_queue;
    // used only by file event <File Descriptor, Mask>
    map<int, int> pending;

    EventWQ(EventCenter *c, time_t timeout, time_t suicide_timeout, ThreadPool *tp)
      : ThreadPool::WorkQueueVal<FiredEvent>("Event::EventWQ", timeout, suicide_timeout, tp), center(c) {}

    void _enqueue(FiredEvent e) {
      if (e.is_file) {
        // Ensure only one thread process one file descriptor
        map<int, int>::iterator it = pending.find(e.file_event.fd);
        if (it != pending.end()) {
          it->second |= e.file_event.mask;
        } else {
          pending[e.file_event.fd] = e.file_event.mask;
          conn_queue.push_back(e);
        }
      } else {
        conn_queue.push_back(e);
      }
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
      FiredEvent e = conn_queue.front();
      conn_queue.pop_front();
      if (e.is_file) {
        assert(pending.count(e.file_event.fd));
        e.file_event.mask = pending[e.file_event.fd];
        pending.erase(e.file_event.fd);
      }
      return e;
    }
    void _process(FiredEvent e, ThreadPool::TPHandle &handle) {
      if (e.is_file) {
        int rfired = 0;
        FileEvent *event = center->get_file_event(e.file_event.fd);
        if (!event)
          return ;

        /* note the event->mask & mask & ... code: maybe an already processed
        * event removed an element that fired and we still didn't
        * processed, so we check if the event is still valid. */
        if (event->mask & e.file_event.mask & EVENT_READABLE) {
          rfired = 1;
          event->read_cb->do_request(e.file_event.fd);
        }
        if (event->mask & e.file_event.mask & EVENT_WRITABLE) {
          if (!rfired || event->read_cb != event->write_cb)
            event->write_cb->do_request(e.file_event.fd);
        }
      } else {
        e.time_event.time_cb->do_request(e.time_event.id);
      }
    }
    void _clear() {
      assert(conn_queue.empty());
    }
  } event_wq;

 public:
  EventCenter(CephContext *c):
    lock("EventCenter::lock"), driver(NULL), cct(c), nevent(0), time_event_next_id(0),
    event_tp(c, "EventCenter::event_tp", c->_conf->ms_event_op_threads, "eventcenter_op_threads"),
    notify_receive_fd(-1), notify_send_fd(-1),tp_stop(true),
    event_wq(this, c->_conf->ms_event_thread_timeout, c->_conf->ms_event_thread_suicide_timeout, &event_tp) {
    last_time = time(NULL);
  }
  ~EventCenter();
  int init(int nevent);
  int create_file_event(int fd, int mask, EventCallbackRef ctxt);
  uint64_t create_time_event(uint64_t milliseconds, EventCallbackRef ctxt);
  void delete_file_event(int fd, int mask);
  void delete_time_event(uint64_t id);
  int process_events(int timeout_milliseconds);
  void start();
  void stop();
  FileEvent *get_file_event(int fd) {
    Mutex::Locker l(lock);
    return _get_file_event(fd);
  }
};

#endif
