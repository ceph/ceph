// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef __DISPATCHER_H
#define __DISPATCHER_H

#include "Message.h"

class Messenger;

class Dispatcher {
 public:
  virtual ~Dispatcher() { }

  // how i receive messages
  virtual void dispatch(Message *m) = 0;

  // how i deal with transmission failures.
  virtual void ms_handle_failure(Message *m, const entity_inst_t& inst) { delete m; }
};

#endif
