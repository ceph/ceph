// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#include "common/errno.h"

#ifdef HAVE_EPOLL
#include "EventEpoll.h""
#else
#ifdef HAVE_KQUEUE
#include "EventKqueue.h"
#else
#include "EventSelect.h"
#endif
#endif

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

  if (!driver) {
    lderr(cct) << __func__ << " failed to create event driver " << dendl;
    return -1;
  }

  int r = driver->init(n);
  if (r < 0) {
    lderr(cct) << __func__ << " failed to init event driver." << dendl;
    return r;
  }

  nevent = n;
  event_tp.start();
  return 0;
}

EventCenter::~EventCenter();
{
  if (driver)
    delete driver;
}

int EventCenter::create_event(int fd, int mask, EventCallback *ctxt)
{
  Mutex::Locker l(lock);
  if (events.size() > center->nevent) {
    lderr(cct) << __func__ << " event count is exceed." << dendl;
    return -ERANGE;
  }

  int r = driver->add_event(fd, mask);
  if (r < 0)
    return r;

  if (events.find(fd) == events.end()) {
    events[fd] = EventCenter::Event();
  }

  EventCenter::Event *event = &events[fd];

  event->mask |= mask;
  if (mask & EVENT_READABLE)
    event->read_cb = ctxt;
  if (mask & EVENT_WRITABLE)
    event->write_cb = ctxt;
  return 0;
}

void delete_event(int fd, int mask)
{
  Mutex::Locker l(lock);
  if (event->mask == EVENT_NONE)
    return;

  driver->del_event(fd, mask);
  struct event *event = &events[fd];

  if (mask & EVENT_READABLE)
    delete event->read_cb;
  if (mask & EVENT_WRITABLE)
    delete event->write_cb;

  event->mask = event->mask & (~mask);
  if (event->mask == EVENT_NONE)
    events.erase(fd);
}

int process_events(int timeout_millionseconds)
{
  struct timeval tv;
  int j, processed, numevents, mask, fd, rfired;

  if (timeout_millionseconds > 0) {
    tv.tv_sec = timeout_millionseconds / 1000;
    tv.tv_usec = (timeout_millionseconds % 1000) * 1000;
  }
  else {
    tv.tv_sec = 0;
    tv.tv_usec = 0;
  }

  processed = 0;
  vector<FiredEvent> fired_events;
  numevents = driver->event_wait(fired_events, &tv);
  for (j = 0; j < numevents; j++)
    event_wq.queue(fired_events[i]);

  return numevents;
}
