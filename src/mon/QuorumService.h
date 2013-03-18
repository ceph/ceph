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
#ifndef CEPH_MON_QUORUM_SERVICE_H
#define CEPH_MON_QUORUM_SERVICE_H

#include <boost/intrusive_ptr.hpp>
// Because intusive_ptr clobbers our assert...
#include "include/assert.h"

#include <errno.h>

#include "include/types.h"
#include "include/Context.h"
#include "common/RefCountedObj.h"
#include "common/config.h"

#include "mon/Monitor.h"
#include "messages/MMonQuorumService.h"

class QuorumService : public RefCountedObject
{
  uint32_t flags;
  Context *tick_event;
  double tick_period;

  struct C_Tick : public Context {
    boost::intrusive_ptr<QuorumService> s;
    C_Tick(boost::intrusive_ptr<QuorumService> qs) : s(qs) { }
    void finish(int r) {
      if (r < 0)
        return;
      s->tick();
    }
  };

public:
  static const int SERVICE_HEALTH                   = 0x01;
  static const int SERVICE_TIMECHECK                = 0x02;
  static const uint32_t FLAG_SHUTDOWN               = 0x01;
  static const uint32_t FLAG_OUT_OF_QUORUM_TICK     = 0x02;
  static const uint32_t FLAG_OUT_OF_QUORUM_DISPATCH = 0x04;

protected:
  Monitor *mon;
  epoch_t epoch;
  bool going;

  QuorumService(Monitor *m, uint32_t flags = 0) :
    flags(flags),
    tick_event(NULL),
    tick_period(g_conf->mon_tick_interval),
    mon(m),
    epoch(0),
    going(false)
  {
  }

  void cancel_tick() {
    if (tick_event)
      mon->timer.cancel_event(tick_event);
    tick_event = NULL;
  }

  void start_tick() {
    generic_dout(10) << __func__ << dendl;
    if (flags & FLAG_SHUTDOWN)
      return;

    cancel_tick();
    if (tick_period <= 0)
      return;

    tick_event = new C_Tick(
        boost::intrusive_ptr<QuorumService>(this));
    mon->timer.add_event_after(tick_period, tick_event);
  }

  void set_update_period(double t) {
    tick_period = t;
  }

  bool in_quorum() {
    return (mon->is_leader() || mon->is_peon());
  }

  virtual bool service_dispatch(Message *m) = 0;
  virtual void service_tick() = 0;
  virtual void service_shutdown() = 0;

  virtual void start_epoch() = 0;
  virtual void finish_epoch() = 0;
  virtual void cleanup() = 0;

public:
  virtual ~QuorumService() { }
  QuorumService *get() {
    return static_cast<QuorumService *>(RefCountedObject::get());
  }

  void start(epoch_t new_epoch) {
    epoch = new_epoch;
    going = true;
    start_epoch();
  }

  void finish() {
    generic_dout(20) << "QuorumService::finish" << dendl;
    going = false;
    finish_epoch();
  }

  epoch_t get_epoch() const {
    return epoch;
  }

  bool is_active() {
    return going;
  }

  bool dispatch(MMonQuorumService *m) {
    if ((!in_quorum() && !(flags & FLAG_OUT_OF_QUORUM_DISPATCH))
        || (flags & FLAG_SHUTDOWN)) {
      m->put();
      return false;
    }
    return service_dispatch(m);
  }

  void tick() {
    if (flags & FLAG_SHUTDOWN)
      return;

    if (!in_quorum() && !(flags & FLAG_OUT_OF_QUORUM_TICK)) {
      return;
    }
    service_tick();
    start_tick();
  }

  void shutdown() {
    generic_dout(0) << "quorum service shutdown" << dendl;
    cancel_tick();
    flags = FLAG_SHUTDOWN;
    service_shutdown();
  }

  virtual void init() { }

  virtual void get_health(Formatter *f,
                          list<pair<health_status_t,string> > *detail) = 0;
  virtual int get_type() = 0;
  virtual string get_name() const = 0;

};
typedef boost::intrusive_ptr<QuorumService> QuorumServiceRef;

#endif /* CEPH_MON_QUORUM_SERVICE_H */
