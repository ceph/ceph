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

#include "AuthUnknownAuthorizeHandler.h"

bool AuthUnknownAuthorizeHandler::verify_authorizer(
  CephContext *cct,
  KeyStore *keys,
  const bufferlist& authorizer_data,
  size_t connection_secret_required_len,
  bufferlist * authorizer_reply,
  EntityName *entity_name,
  uint64_t *global_id,
  AuthCapsInfo *caps_info,
  CryptoKey *session_key,
  std::string *connection_secret,
  std::unique_ptr<AuthAuthorizerChallenge> *challenge)
{
  // For unknown authorizers, there's nothing to verify.  They're "OK" by definition.  PLR

  return true;
}

// Return type of crypto used for this session's data;  for unknown, no crypt used

int AuthUnknownAuthorizeHandler::authorizer_session_crypto() 
{
  return SESSION_CRYPTO_NONE;
}
