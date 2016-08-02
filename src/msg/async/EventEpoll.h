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

#ifndef CEPH_MSG_EVENTEPOLL_H
#define CEPH_MSG_EVENTEPOLL_H

#include <unistd.h>
#include <sys/epoll.h>

#include "Event.h"

class EpollDriver : public EventDriver {
  int epfd;
  struct epoll_event *events;
  CephContext *cct;
  int size;

 public:
  explicit EpollDriver(CephContext *c): epfd(-1), events(NULL), cct(c), size(0) {}
  virtual ~EpollDriver() {
    if (epfd != -1)
      close(epfd);

    if (events)
      free(events);
  }

  int init(EventCenter *c, int nevent) override;
  int add_event(int fd, int cur_mask, int add_mask) override;
  int del_event(int fd, int cur_mask, int del_mask) override;
  int resize_events(int newsize) override;
  int event_wait(vector<FiredFileEvent> &fired_events,
		 struct timeval *tp) override;
};

#endif
