// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
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
  virtual Message *ms_handle_failure(msg_addr_t dest, entity_inst_t& inst) { return 0; }

  // lookups
  virtual bool ms_lookup(msg_addr_t dest, entity_inst_t& inst) { assert(0); return 0; }

  // this is how i send messages
  //int send_message(Message *m, msg_addr_t dest, int dest_port);
};

#endif
