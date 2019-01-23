// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (c) 2018 SUSE LLC.
 * Author: Daniel Oliveira <doliveira@suse.com>
 * 
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "KrbAuthorizeHandler.hpp"

#include "common/debug.h"

#define dout_subsys ceph_subsys_auth 

bool KrbAuthorizeHandler::verify_authorizer(
  CephContext* ceph_ctx,
  KeyStore* keys,
  const bufferlist& authorizer_data,
  size_t connection_secret_required_len,
  bufferlist *authorizer_reply,
  EntityName *entity_name,
  uint64_t *global_id,
  AuthCapsInfo *caps_info,
  CryptoKey *session_key,
  std::string *connection_secret,
  std::unique_ptr<AuthAuthorizerChallenge>* challenge)
{
  auto itr(authorizer_data.cbegin());

  try {
    uint8_t value = (1);

    using ceph::decode;
    decode(value, itr);
    decode(*entity_name, itr);
    decode(*global_id, itr);
  } catch (const buffer::error& err) {
    ldout(ceph_ctx, 0) 
        << "Error: KrbAuthorizeHandler::verify_authorizer() failed!" << dendl;
    return false;
  }
  caps_info->allow_all = true;
  return true;
}


