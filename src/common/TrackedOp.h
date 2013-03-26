// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 New Dream Network/Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef TRACKEDREQUEST_H_
#define TRACKEDREQUEST_H_
#include <sstream>
#include <stdint.h>
#include <include/utime.h>
#include "common/Mutex.h"
#include "include/xlist.h"
#include "msg/Message.h"
#include <tr1/memory>

class TrackedOp {
protected:
  list<pair<utime_t, string> > events; /// list of events and their times
  Mutex lock; /// to protect the events list
public:
  // move these to private once friended OpTracker
  Message *request;
  xlist<TrackedOp*>::item xitem;
  utime_t received_time;
  // figure out how to get rid of this one?
  uint8_t warn_interval_multiplier;
  string current;
  uint64_t seq;

  TrackedOp(Message *req) :
    lock("TrackedOp::lock"),
    request(req),
    xitem(this),
    warn_interval_multiplier(1),
    seq(0) {}

  virtual void init_from_message() {};

  virtual ~TrackedOp() {}

  utime_t get_arrived() const {
    return received_time;
  }
  // This function maybe needs some work; assumes last event is completion time
  double get_duration() const {
    return events.size() ?
      (events.rbegin()->first - received_time) :
      0.0;
  }

  virtual void mark_event(const string &event) = 0;
  virtual const char *state_string() const = 0;
  virtual void dump(utime_t now, Formatter *f) const = 0;
};
typedef std::tr1::shared_ptr<TrackedOp> TrackedOpRef;

#endif
