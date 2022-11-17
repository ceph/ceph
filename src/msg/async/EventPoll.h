// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Rafael Lopez <rafael.lopez@softiron.com>
 *
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MSG_EVENTPOLL_H
#define CEPH_MSG_EVENTPOLL_H

#ifdef _WIN32
#include <winsock2.h>
#else
#include <poll.h>
#endif

#include "Event.h"

typedef struct pollfd POLLFD;

class PollDriver : public EventDriver {
  int max_pfds;
  int hard_max_pfds;
  POLLFD *pfds;
  CephContext *cct;

 private:
  int poll_ctl(int, int, int);

 public:
  explicit PollDriver(CephContext *c): cct(c) {}
  ~PollDriver() override {}

  int init(EventCenter *c, int nevent) override;
  int add_event(int fd, int cur_mask, int add_mask) override;
  int del_event(int fd, int cur_mask, int del_mask) override;
  int resize_events(int newsize) override;
  int event_wait(std::vector<FiredFileEvent> &fired_events,
		 struct timeval *tp) override;
};

#endif
