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

#ifndef CEPH_MSG_EVENTSELECT_H
#define CEPH_MSG_EVENTSELECT_H

#include "Event.h"

class SelectDriver : public EventDriver {
  fd_set rfds, wfds;
  /* We need to have a copy of the fd sets as it's not safe to reuse
   * FD sets after select(). */
  fd_set _rfds, _wfds;
  int max_fd;
  CephContext *cct;

 public:
  explicit SelectDriver(CephContext *c): max_fd(0), cct(c) {}
  virtual ~SelectDriver() {}

  int init(EventCenter *c, int nevent) override;
  int add_event(int fd, int cur_mask, int add_mask) override;
  int del_event(int fd, int cur_mask, int del_mask) override;
  int resize_events(int newsize) override;
  int event_wait(vector<FiredFileEvent> &fired_events,
		 struct timeval *tp) override;
};

#endif
