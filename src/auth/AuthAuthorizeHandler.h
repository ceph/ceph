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

#ifndef __AUTHAUTHORIZEHANDLER_H
#define __AUTHAUTHORIZEHANDLER_H

#include "include/types.h"
#include "config.h"
#include "Auth.h"

class KeyRing;
class RotatingKeyRing;

struct AuthAuthorizeHandler {
  virtual ~AuthAuthorizeHandler() {}
  virtual bool verify_authorizer(KeyStore *keys,
				 bufferlist& authorizer_data, bufferlist& authorizer_reply,
                                 EntityName& entity_name, uint64_t& global_id,
				 AuthCapsInfo& caps_info, uint64_t *auid = NULL) = 0;
};

extern AuthAuthorizeHandler *get_authorize_handler(int protocol);

#endif
