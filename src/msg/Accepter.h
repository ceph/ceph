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
  SimpleMessenger *msgr;
  bool done;
  int listen_sd;
  uint64_t nonce;

public:
  Accepter(SimpleMessenger *r, uint64_t n) : msgr(r), done(false), listen_sd(-1), nonce(n) {}
    
  void *entry();
  void stop();
  int bind(const entity_addr_t &bind_addr, const set<int>& avoid_ports);
  int rebind(const set<int>& avoid_port);
  int start();
};


#endif
