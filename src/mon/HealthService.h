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
  virtual ~HealthService() { }

  virtual bool service_dispatch(Message *m) {
    return service_dispatch(static_cast<MMonHealth*>(m));
  }

  virtual bool service_dispatch(MMonHealth *m) = 0;

public:
  virtual health_status_t get_health(Formatter *f,
                          list<pair<health_status_t,string> > *detail) = 0;
  virtual int get_type() = 0;
  virtual string get_name() const = 0;
};

#endif // CEPH_MON_HEALTH_SERVICE_H
