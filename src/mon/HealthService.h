// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_MON_HEALTH_SERVICE_H
#define CEPH_MON_HEALTH_SERVICE_H

#include "mon/Monitor.h"
#include "mon/QuorumService.h"

#include "messages/MMonHealth.h"

#include "common/config.h"

struct HealthService : public QuorumService
{
  enum {
    SERVICE_HEALTH_DATA              = 0x01
  };

  HealthService(Monitor *m) : QuorumService(m) { }
  ~HealthService() override { }

  bool service_dispatch(MonOpRequestRef op) override {
    return service_dispatch_op(op);
  }

  virtual bool service_dispatch_op(MonOpRequestRef op) = 0;
};

#endif // CEPH_MON_HEALTH_SERVICE_H
