// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __AUTHORIZESERVER_H
#define __AUTHORIZESERVER_H

#include "config.h"

#include "msg/SimpleMessenger.h"

class Messenger;
class KeysKeeper;
class Message;
class MAuthorize;

class AuthorizeServer : public Dispatcher {
  Messenger *messenger;

  bool _dispatch(Message *m);
  bool ms_dispatch(Message *m);

  bool ms_handle_reset(Connection *con, const entity_addr_t& peer) { return false; }
  void ms_handle_failure(Connection *con, Message *m, const entity_addr_t& peer) { }
  void ms_handle_remote_reset(Connection *con, const entity_addr_t& peer) {}

  KeysKeeper *keys;

  Mutex lock;

  int do_authorize(bufferlist::iterator& indata, bufferlist& result_bl);
  void build_cephx_response_header(int request_type, int status, bufferlist& bl);
public:
  AuthorizeServer(Messenger *m, KeysKeeper *k) : messenger(m), keys(k), lock("auth_server") {}
  ~AuthorizeServer();

  bool init();
  void handle_request(MAuthorize *m);
};

#endif
