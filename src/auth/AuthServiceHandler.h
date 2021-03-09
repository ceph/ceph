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
#include "include/common_fwd.h"
#include "include/buffer_fwd.h"  // for ceph::buffer::list

class KeyServer;
class CryptoKey;
struct AuthCapsInfo;

struct AuthServiceHandler {
protected:
  CephContext *cct;
  EntityName entity_name;
  uint64_t global_id = 0;

public:
  explicit AuthServiceHandler(CephContext *cct_) : cct(cct_) {}

  virtual ~AuthServiceHandler() { }

  int start_session(const EntityName& entity_name,
		    uint64_t global_id,
		    bool is_new_global_id,
		    ceph::buffer::list *result,
		    AuthCapsInfo *caps);
  virtual int handle_request(ceph::buffer::list::const_iterator& indata,
			     size_t connection_secret_required_length,
			     ceph::buffer::list *result,
			     uint64_t *global_id,
			     AuthCapsInfo *caps,
			     CryptoKey *session_key,
			     std::string *connection_secret) = 0;

  const EntityName& get_entity_name() { return entity_name; }
  uint64_t get_global_id() { return global_id; }

private:
  virtual int do_start_session(bool is_new_global_id,
			       ceph::buffer::list *result,
			       AuthCapsInfo *caps) = 0;
};

extern AuthServiceHandler *get_auth_service_handler(int type, CephContext *cct, KeyServer *ks);

#endif
