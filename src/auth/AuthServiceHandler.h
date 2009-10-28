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

class KeyServer;

struct AuthServiceHandler {
  virtual ~AuthServiceHandler() { }

  virtual int start_session(bufferlist& result) = 0;
  virtual int handle_request(bufferlist::iterator& indata, bufferlist& result, bufferlist& caps) = 0;

  virtual EntityName& get_entity_name() = 0;
};

extern AuthServiceHandler *get_auth_service_handler(KeyServer *ks, set<__u32>& supported);

#endif
