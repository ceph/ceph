// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_EVENTDPDK_H
#define CEPH_EVENTDPDK_H

#include "msg/async/Event.h"
#include "msg/async/Stack.h"
#include "UserspaceEvent.h"

class DPDKDriver : public EventDriver {
  CephContext *cct;

 public:
  UserspaceEventManager manager;

  DPDKDriver(CephContext *c): cct(c), manager(c) {}
  virtual ~DPDKDriver() { }

  int init(EventCenter *c, int nevent) override;
  int add_event(int fd, int cur_mask, int add_mask) override;
  int del_event(int fd, int cur_mask, int del_mask) override;
  int resize_events(int newsize) override;
  int event_wait(vector<FiredFileEvent> &fired_events, struct timeval *tp) override;
  bool wakeup_support() { return false; }
};

#endif //CEPH_EVENTDPDK_H
