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

#ifndef CEPH_AUTHAUTHORIZEHANDLER_H
#define CEPH_AUTHAUTHORIZEHANDLER_H

#include "Auth.h"
#include "AuthMethodList.h"
#include "include/types.h"

// Different classes of session crypto handling

#define SESSION_CRYPTO_NONE 0
#define SESSION_SYMMETRIC_AUTHENTICATE 1
#define SESSION_SYMMETRIC_ENCRYPT 2

class CephContext;
class KeyRing;
class RotatingKeyRing;

struct AuthAuthorizeHandler {
  virtual ~AuthAuthorizeHandler() {}
  virtual bool verify_authorizer(CephContext *cct, KeyStore *keys,
				 bufferlist& authorizer_data, bufferlist& authorizer_reply,
                                 EntityName& entity_name, uint64_t& global_id,
				 AuthCapsInfo& caps_info, CryptoKey& session_key, uint64_t *auid = NULL) = 0;
  virtual int authorizer_session_crypto() = 0;
};

class AuthAuthorizeHandlerRegistry {
  Mutex m_lock;
  map<int,AuthAuthorizeHandler*> m_authorizers;
  AuthMethodList supported;

public:
  AuthAuthorizeHandlerRegistry(CephContext *cct_, std::string methods)
    : m_lock("AuthAuthorizeHandlerRegistry::m_lock"), supported(cct_, methods)
  {}
  ~AuthAuthorizeHandlerRegistry();
  
  AuthAuthorizeHandler *get_handler(int protocol);
};

#endif
