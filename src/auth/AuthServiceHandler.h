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

#include "include/types.h"
#include "config.h"

class Monitor;

class AuthServiceHandler {
protected:
  Monitor *mon;

public:
  AuthServiceHandler(Monitor *m) : mon(m) { }
  virtual ~AuthServiceHandler() { }

  virtual int handle_request(bufferlist::iterator& indata, bufferlist& result) = 0;
};

extern AuthServiceHandler *get_auth_handler(Monitor *mon, set<__u32>& supported);

#endif
