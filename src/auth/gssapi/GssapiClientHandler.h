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

#ifndef CEPH_AUTHGSSAPICLIENTHANDLER_H
#define CEPH_AUTHGSSAPICLIENTHANDLER_H

#include "auth/AuthClientHandler.h"
#include "GssapiProtocol.h"
#include "common/ceph_context.h"
#include "common/config.h"

#include <gssapi/gssapi_generic.h>
#include <gssapi/gssapi_krb5.h>
#include <gssapi/gssapi_ext.h>
 
class GssapiClientHandler : public AuthClientHandler {

  gss_name_t gss_client_name;
  gss_name_t gss_service_name;
  gss_cred_id_t gss_cred;
  gss_ctx_id_t gss_sec_context;
  gss_buffer_desc gss_output_token;

public:
  GssapiClientHandler(CephContext *cct_, RotatingKeyRing *rsecrets) 
    : AuthClientHandler(cct_),
      gss_client_name(GSS_C_NO_NAME),
      gss_service_name(GSS_C_NO_NAME),
      gss_cred(GSS_C_NO_CREDENTIAL),
      gss_sec_context(GSS_C_NO_CONTEXT),
      gss_output_token({0})
  {
    reset();
  }

  void reset() {
    RWLock::WLocker l(lock);
    gss_client_name = GSS_C_NO_NAME;
    gss_service_name = GSS_C_NO_NAME;
    gss_cred = GSS_C_NO_CREDENTIAL;
    gss_sec_context = GSS_C_NO_CONTEXT;
    gss_output_token = {0};
  }

  ~GssapiClientHandler();

  void prepare_build_request() {}
  int build_request(bufferlist& bl) const;
  int handle_response(int ret, bufferlist::iterator& iter);
  bool build_rotating_request(bufferlist& bl) const { return false; }

  int get_protocol() const { return CEPH_AUTH_GSSAPI; }
  
  AuthAuthorizer *build_authorizer(uint32_t service_id) const;

  bool need_tickets() { return false; }

  void set_global_id(uint64_t id) {
    RWLock::WLocker l(lock);
    global_id = id;
  }
  void validate_tickets() {}
};

#endif
