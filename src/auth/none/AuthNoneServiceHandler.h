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

#ifndef CEPH_AUTHNONESERVICEHANDLER_H
#define CEPH_AUTHNONESERVICEHANDLER_H

#include "auth/AuthServiceHandler.h"
#include "auth/Auth.h"
#include "include/common_fwd.h"

class AuthNoneServiceHandler  : public AuthServiceHandler {
public:
  explicit AuthNoneServiceHandler(CephContext *cct_)
    : AuthServiceHandler(cct_) {}
  ~AuthNoneServiceHandler() override {}
  
  int handle_request(ceph::buffer::list::const_iterator& indata,
		     size_t connection_secret_required_length,
		     ceph::buffer::list *result_bl,
		     AuthCapsInfo *caps,
		     CryptoKey *session_key,
		     std::string *connection_secret) override {
    return 0;
  }

private:
  int do_start_session(bool is_new_global_id,
		       ceph::buffer::list *result_bl,
		       AuthCapsInfo *caps) override {
    caps->allow_all = true;
    return 1;
  }
};

#endif
