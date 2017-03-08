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

#ifndef CEPH_AUTHGSSAPISERVICEHANDLER_H
#define CEPH_AUTHGSSAPISERVICEHANDLER_H

#include "auth/AuthServiceHandler.h"
#include "auth/Auth.h"
#include "auth/cephx/CephxKeyServer.h"

#include <gssapi/gssapi_generic.h>
#include <gssapi/gssapi_krb5.h>
#include <gssapi/gssapi_ext.h>

class CephContext;
class KeyServer;

class GssapiServiceHandler  : public AuthServiceHandler {

  KeyServer *key_server;
  gss_name_t gss_service_name;
  gss_cred_id_t gss_cred;
  gss_ctx_id_t gss_sec_context;
  gss_buffer_desc gss_output_token;

public:
  explicit GssapiServiceHandler(CephContext *cct_, KeyServer *ks)
    : AuthServiceHandler(cct_),
      key_server(ks),
      gss_service_name(GSS_C_NO_NAME),
      gss_cred(GSS_C_NO_CREDENTIAL),
      gss_sec_context(GSS_C_NO_CONTEXT),
      gss_output_token({0}) {}
  ~GssapiServiceHandler();
  int start_session(EntityName& name, bufferlist::iterator& indata, bufferlist& result_bl, AuthCapsInfo& caps);
  int handle_request(bufferlist::iterator& indata, bufferlist& result_bl, uint64_t& global_id, AuthCapsInfo& caps, uint64_t *auid = NULL);
};

#endif
