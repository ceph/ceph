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

#ifndef CEPH_MSG_EVENTKQUEUE_H
#define CEPH_MSG_EVENTKQUEUE_H

#include <sys/types.h>
#include <sys/event.h>
#include <unistd.h>

#include "Event.h"

class KqueueDriver : public EventDriver {
  int kqfd;
  struct kevent *events;
  CephContext *cct;
  int size;

 public:
  explicit KqueueDriver(CephContext *c): kqfd(-1), events(NULL), cct(c), size(0) {}
  virtual ~KqueueDriver() {
    if (kqfd != -1)
      close(kqfd);

    if (events)
      free(events);
  }

  int init(int nevent);
  int add_event(int fd, int cur_mask, int add_mask);
  int del_event(int fd, int cur_mask, int del_mask);
  int resize_events(int newsize);
  int event_wait(vector<FiredFileEvent> &fired_events, struct timeval *tp);
};

#endif
