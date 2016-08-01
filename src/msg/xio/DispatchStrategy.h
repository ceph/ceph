// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef DISPATCH_STRATEGY_H
#define DISPATCH_STRATEGY_H

#include "msg/Message.h"

class Messenger;

class DispatchStrategy
{
protected:
  Messenger *msgr;
public:
  DispatchStrategy() {}
  Messenger *get_messenger() { return msgr; }
  void set_messenger(Messenger *_msgr) { msgr = _msgr; }
  virtual void ds_dispatch(Message *m) = 0;
  virtual void shutdown() = 0;
  virtual void start() = 0;
  virtual void wait() = 0;
  virtual ~DispatchStrategy() {}
};

#endif /* DISPATCH_STRATEGY_H */
