// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Daniel Oliveira <doliveira@suse.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef KRB_SESSION_HANDLER_HPP
#define KRB_SESSION_HANDLER_HPP

/* Include order and names:
 * a) Immediate related header
 * b) C libraries (if any),
 * c) C++ libraries,
 * d) Other support libraries
 * e) Other project's support libraries
 *
 * Within each section the includes should
 * be ordered alphabetically.
 */

#include "auth/AuthServiceHandler.h"
#include "msg/Message.h"

class KrbSessionHandler : public AuthSessionHandler
{
  public:
    KrbSessionHandler(CephContext* ceph_ctx, CryptoKey session_key) : 
        AuthSessionHandler(ceph_ctx, CEPH_AUTH_KRB5, session_key) { }
    ~KrbSessionHandler() = default; 

    bool no_security() { return true; }
    int sign_message(Message* msg) { return 0; }
    int check_message_signature(Message* msg) { return 0; }
    int encrypt_message(Message* msg) { return 0; }
    int decrypt_message(Message* msg) { return 0; }

  private:
};

#endif    //-- KRB_SESSION_HANDLER_HPP

// ----------------------------- END-OF-FILE --------------------------------//


