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

#ifndef CEPH_AUTHSERVICEHANDLER_H
#define CEPH_AUTHSERVICEHANDLER_H

#include "include/types.h"
#include "common/config.h"
#include "Auth.h"

class CephContext;
class KeyServer;

struct AuthServiceHandler {
protected:
  CephContext *cct;
public:
  EntityName entity_name;
  uint64_t global_id;

  explicit AuthServiceHandler(CephContext *cct_) : cct(cct_), global_id(0) {}

  virtual ~AuthServiceHandler() { }

  virtual int start_session(EntityName& name, bufferlist::iterator& indata, bufferlist& result, AuthCapsInfo& caps) = 0;
  virtual int handle_request(bufferlist::iterator& indata, bufferlist& result, uint64_t& global_id, AuthCapsInfo& caps, uint64_t *auid = NULL) = 0;

  EntityName& get_entity_name() { return entity_name; }
};

extern AuthServiceHandler *get_auth_service_handler(int type, CephContext *cct, KeyServer *ks);

#endif
