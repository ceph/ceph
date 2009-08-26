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

#ifndef __AUTHSERVICEMANAGER_H
#define __AUTHSERVICEMANAGER_H

#include <map>
#include <set>
using namespace std;

#include "include/types.h"

#include "config.h"

class Monitor;


class AuthServiceHandler {
  Monitor *mon;
  AuthServiceHandler *instance;
protected:
  bufferlist response_payload;
public:
  AuthServiceHandler() : instance(NULL) {}
  void init(Monitor *m) { mon = m; }
  virtual ~AuthServiceHandler();
  virtual int handle_request(bufferlist& bl, bufferlist& result);
  AuthServiceHandler *get_instance();
  bufferlist& get_response_payload();
};

class AuthServiceManager
{
  /* FIXME: map locking */
  map<entity_addr_t, AuthServiceHandler> m;
  Monitor *mon;

public:
  AuthServiceHandler *get_auth_handler(entity_addr_t& addr);
  void init(Monitor *m) { mon = m; }
};

#endif
