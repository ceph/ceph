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

#ifndef __AUTHMANAGER_H
#define __AUTHMANAGER_H

#include <map>
#include <set>
using namespace std;

#include "include/types.h"

#include "config.h"

class Monitor;


class AuthHandler {
  Monitor *mon;
  AuthHandler *instance;
protected:
  bufferlist response_payload;
public:
  AuthHandler() : instance(NULL) {}
  void init(Monitor *m) { mon = m; }
  virtual ~AuthHandler();
  virtual int handle_request(bufferlist& bl, bufferlist& result);
  AuthHandler *get_instance();
  bufferlist& get_response_payload();
};

class AuthManager
{
  /* FIXME: map locking */
  map<entity_addr_t, AuthHandler> m;
  Monitor *mon;

public:
  AuthHandler *get_auth_handler(entity_addr_t& addr);
  void init(Monitor *m) { mon = m; }
};

#endif
