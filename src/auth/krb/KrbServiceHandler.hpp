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

#ifndef KRB_SERVICE_HANDLER_HPP
#define KRB_SERVICE_HANDLER_HPP

#include "auth/AuthServiceHandler.h"
#include "auth/Auth.h"
#include "auth/cephx/CephxKeyServer.h"

#include <gssapi.h>
#include <gssapi/gssapi_generic.h>
#include <gssapi/gssapi_krb5.h>
#include <gssapi/gssapi_ext.h>


class KrbServiceHandler : public AuthServiceHandler {

  public:
    explicit KrbServiceHandler(CephContext* ceph_ctx, KeyServer* kserver) : 
      AuthServiceHandler(ceph_ctx), 
      m_gss_buffer_out({0, nullptr}), 
      m_gss_credentials(GSS_C_NO_CREDENTIAL), 
      m_gss_sec_ctx(GSS_C_NO_CONTEXT), 
      m_gss_service_name(GSS_C_NO_NAME), 
      m_key_server(kserver) { }
    ~KrbServiceHandler();
    int handle_request(bufferlist::const_iterator& indata,
		       size_t connection_secret_required_length,
		       bufferlist *buff_list,
                       uint64_t *global_id,
                       AuthCapsInfo *caps,
		       CryptoKey *session_key,
		       std::string *connection_secret) override;

    int start_session(const EntityName& name,
		      size_t connection_secret_required_length,
		      bufferlist *buff_list,
                      AuthCapsInfo *caps,
		      CryptoKey *session_key,
		      std::string *connection_secret) override;

  private:
    gss_buffer_desc m_gss_buffer_out;
    gss_cred_id_t m_gss_credentials; 
    gss_ctx_id_t m_gss_sec_ctx; 
    gss_name_t m_gss_service_name; 
    KeyServer* m_key_server;

};

#endif    //-- KRB_SERVICE_HANDLER_HPP

