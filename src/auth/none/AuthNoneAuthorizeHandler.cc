// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "AuthNoneAuthorizeHandler.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_auth

bool AuthNoneAuthorizeHandler::verify_authorizer(CephContext *cct, KeyStore *keys,
						 bufferlist& authorizer_data, bufferlist& authorizer_reply,
						 EntityName& entity_name, uint64_t& global_id, AuthCapsInfo& caps_info, CryptoKey& session_key,
uint64_t *auid)
{
  bufferlist::iterator iter = authorizer_data.begin();

  try {
    __u8 struct_v = 1;
    ::decode(struct_v, iter);
    ::decode(entity_name, iter);
    ::decode(global_id, iter);
  } catch (const buffer::error &err) {
    ldout(cct, 0) << "AuthNoneAuthorizeHandle::verify_authorizer() failed to decode" << dendl;
    return false;
  }

  caps_info.allow_all = true;

  return true;
}

// Return type of crypto used for this session's data;  for none, no crypt used

int AuthNoneAuthorizeHandler::authorizer_session_crypto() 
{
  return SESSION_CRYPTO_NONE;
}
