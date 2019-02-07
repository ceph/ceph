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

#ifndef KRB_AUTHORIZE_HANDLER_HPP
#define KRB_AUTHORIZE_HANDLER_HPP

#include "auth/AuthAuthorizeHandler.h"

class KrbAuthorizeHandler : public AuthAuthorizeHandler {
  bool verify_authorizer(
    CephContext*,
    KeyStore*,
    const bufferlist&,
    size_t,
    bufferlist *,
    EntityName *,
    uint64_t *,
    AuthCapsInfo *,
    CryptoKey *,
    std::string *connection_secret,
    std::unique_ptr<
    AuthAuthorizerChallenge>* = nullptr) override;

  int authorizer_session_crypto() override { 
    return SESSION_SYMMETRIC_AUTHENTICATE; 
  };

  ~KrbAuthorizeHandler() override = default;

};


#endif    //-- KRB_AUTHORIZE_HANDLER_HPP

