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

#include "common/errno.h"
#include "EventEpoll.h"

#define dout_subsys ceph_subsys_ms

#undef dout_prefix
#define dout_prefix *_dout << "EpollDriver."

int EpollDriver::init(int nevent)
{
  events = (struct epoll_event*)malloc(sizeof(struct epoll_event)*nevent);
  if (!events) {
    lderr(cct) << __func__ << " unable to malloc memory. " << dendl;
    return -ENOMEM;
  }
  memset(events, 0, sizeof(struct epoll_event)*nevent);

  epfd = epoll_create(1024); /* 1024 is just an hint for the kernel */
  if (epfd == -1) {
    lderr(cct) << __func__ << " unable to do epoll_create: "
                       << cpp_strerror(errno) << dendl;
    return -errno;
  }

  size = nevent;

  return 0;
}

int EpollDriver::add_event(int fd, int cur_mask, int add_mask)
{
  ldout(cct, 20) << __func__ << " add event fd=" << fd << " cur_mask=" << cur_mask
                 << " add_mask=" << add_mask << " to " << epfd << dendl;
  struct epoll_event ee;
  /* If the fd was already monitored for some event, we need a MOD
   * operation. Otherwise we need an ADD operation. */
  int op;
  op = cur_mask == EVENT_NONE ? EPOLL_CTL_ADD: EPOLL_CTL_MOD;

  ee.events = EPOLLET;
  add_mask |= cur_mask; /* Merge old events */
  if (add_mask & EVENT_READABLE)
    ee.events |= EPOLLIN;
  if (add_mask & EVENT_WRITABLE)
    ee.events |= EPOLLOUT;
  ee.data.u64 = 0; /* avoid valgrind warning */
  ee.data.fd = fd;
  if (epoll_ctl(epfd, op, fd, &ee) == -1) {
    lderr(cct) << __func__ << " epoll_ctl: add fd=" << fd << " failed. "
               << cpp_strerror(errno) << dendl;
    return -errno;
  }

  return 0;
}

int EpollDriver::del_event(int fd, int cur_mask, int delmask)
{
  ldout(cct, 20) << __func__ << " del event fd=" << fd << " cur_mask=" << cur_mask
                 << " delmask=" << delmask << " to " << epfd << dendl;
  struct epoll_event ee;
  int mask = cur_mask & (~delmask);
  int r = 0;

  ee.events = 0;
  if (mask & EVENT_READABLE) ee.events |= EPOLLIN;
  if (mask & EVENT_WRITABLE) ee.events |= EPOLLOUT;
  ee.data.u64 = 0; /* avoid valgrind warning */
  ee.data.fd = fd;
  if (mask != EVENT_NONE) {
    if ((r = epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ee)) < 0) {
      lderr(cct) << __func__ << " epoll_ctl: modify fd=" << fd << " mask=" << mask
                 << " failed." << cpp_strerror(errno) << dendl;
      return -errno;
    }
  } else {
    /* Note, Kernel < 2.6.9 requires a non null event pointer even for
     * EPOLL_CTL_DEL. */
    if ((r = epoll_ctl(epfd, EPOLL_CTL_DEL, fd, &ee)) < 0) {
      lderr(cct) << __func__ << " epoll_ctl: delete fd=" << fd
                 << " failed." << cpp_strerror(errno) << dendl;
      return -errno;
    }
  }
  return 0;
}

int EpollDriver::resize_events(int newsize)
{
  return 0;
}

int EpollDriver::event_wait(vector<FiredFileEvent> &fired_events, struct timeval *tvp)
{
  int retval, numevents = 0;

  retval = epoll_wait(epfd, events, size,
                      tvp ? (tvp->tv_sec*1000 + tvp->tv_usec/1000) : -1);
  if (retval > 0) {
    int j;

    numevents = retval;
    fired_events.resize(numevents);
    for (j = 0; j < numevents; j++) {
      int mask = 0;
      struct epoll_event *e = events + j;

      if (e->events & EPOLLIN) mask |= EVENT_READABLE;
      if (e->events & EPOLLOUT) mask |= EVENT_WRITABLE;
      if (e->events & EPOLLERR) mask |= EVENT_READABLE|EVENT_WRITABLE;
      if (e->events & EPOLLHUP) mask |= EVENT_READABLE|EVENT_WRITABLE;
      fired_events[j].fd = e->data.fd;
      fired_events[j].mask = mask;
    }
  }
  return numevents;
}
