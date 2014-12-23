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


#ifndef FAST_STRATEGY_H
#define FAST_STRATEGY_H
#include "DispatchStrategy.h"

class FastStrategy : public DispatchStrategy {
public:
  FastStrategy() {}
  virtual void ds_dispatch(Message *m) {
    msgr->ms_fast_preprocess(m);
    if (msgr->ms_can_fast_dispatch(m))
      msgr->ms_fast_dispatch(m);
    else
      msgr->ms_deliver_dispatch(m);
  }
  virtual void shutdown() {}
  virtual void start() {}
  virtual void wait() {}
  virtual ~FastStrategy() {}
};
#endif /* FAST_STRATEGY_H */
