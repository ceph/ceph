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

#ifndef CEPH_AUTHUNKNOWNSERVICEHANDLER_H
#define CEPH_AUTHUNKNOWNSERVICEHANDLER_H

#include "auth/AuthServiceHandler.h"
#include "auth/Auth.h"

class CephContext;

class AuthUnknownServiceHandler  : public AuthServiceHandler {
public:
  AuthUnknownServiceHandler(CephContext *cct_) 
    : AuthServiceHandler(cct_) {}
  ~AuthUnknownServiceHandler() {}
  
  int start_session(const EntityName& name,
		    size_t connection_secret_required_length,
		    bufferlist *result_bl,
		    AuthCapsInfo *caps,
		    CryptoKey *session_key,
		    std::string *connection_secret) {
    return 1;
  }
  int handle_request(bufferlist::iterator& indata,
		     size_t connection_secret_required_length,
		     bufferlist *result_bl,
		     uint64_t *global_id,
		     AuthCapsInfo *caps,
		     CryptoKey *session_key,
		     std::string *connection_secret) {
    ceph_abort();  // shouldn't get called
    return 0;
  }

  void build_cephx_response_header(int request_type, int status,
				   bufferlist& bl) {
  }
};

#endif
