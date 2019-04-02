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

#include <stddef.h>              // for NULL
#include <stdint.h>              // for uint64_t
#include "common/entity_name.h"  // for EntityName
#include "include/buffer_fwd.h"  // for ceph::buffer::list

class CephContext;
class KeyServer;
class CryptoKey;
struct AuthCapsInfo;

struct AuthServiceHandler {
protected:
  CephContext *cct;
public:
  EntityName entity_name;
  uint64_t global_id;

  explicit AuthServiceHandler(CephContext *cct_) : cct(cct_), global_id(0) {}

  virtual ~AuthServiceHandler() { }

  virtual int start_session(const EntityName& name,
			    size_t connection_secret_required_length,
			    ceph::buffer::list *result,
			    AuthCapsInfo *caps,
			    CryptoKey *session_key,
			    std::string *connection_secret) = 0;
  virtual int handle_request(ceph::buffer::list::const_iterator& indata,
			     size_t connection_secret_required_length,
			     ceph::buffer::list *result,
			     uint64_t *global_id,
			     AuthCapsInfo *caps,
			     CryptoKey *session_key,
			     std::string *connection_secret) = 0;

  EntityName& get_entity_name() { return entity_name; }
};

extern AuthServiceHandler *get_auth_service_handler(int type, CephContext *cct, KeyServer *ks);

#endif
