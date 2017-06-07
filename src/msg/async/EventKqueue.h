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
  pthread_t mythread;
  struct kevent *res_events;
  CephContext *cct;
  int size;

  // Keep what we set on the kqfd
  struct SaveEvent{
    int fd;
    int mask;
  };
  struct SaveEvent *sav_events;
  int sav_max;
  int restore_events();
  int test_kqfd();
  int test_thread_change(const char* funcname);

 public:
  explicit KqueueDriver(CephContext *c): kqfd(-1), res_events(NULL), cct(c), 
		size(0), sav_max(0) {}
  virtual ~KqueueDriver() {
    if (kqfd != -1)
      close(kqfd);

    if (res_events)
      free(res_events);
    size = 0;
    if (sav_events)
      free(sav_events);
    sav_max = 0;
  }

  int init(EventCenter *c, int nevent) override;
  int add_event(int fd, int cur_mask, int add_mask) override;
  int del_event(int fd, int cur_mask, int del_mask) override;
  int resize_events(int newsize) override;
  int event_wait(vector<FiredFileEvent> &fired_events,
		 struct timeval *tp) override;
};

#endif
