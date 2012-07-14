// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#ifndef CEPH_MSG_ACCEPTER_H
#define CEPH_MSG_ACCEPTER_H

#include "msg/msg_types.h"
#include "common/Thread.h"

class SimpleMessenger;

/**
 * If the SimpleMessenger binds to a specific address, the Accepter runs
 * and listens for incoming connections.
 */
class Accepter : public Thread {
public:
  SimpleMessenger *msgr;
  bool done;
  int listen_sd;

  Accepter(SimpleMessenger *r) : msgr(r), done(false), listen_sd(-1) {}
    
  void *entry();
  void stop();
  int bind(entity_addr_t &bind_addr, int avoid_port1=0, int avoid_port2=0);
  int rebind(int avoid_port);
  int start();
};


#endif
