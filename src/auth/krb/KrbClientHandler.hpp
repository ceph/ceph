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

#ifndef KRB_CLIENT_HANDLER_HPP
#define KRB_CLIENT_HANDLER_HPP

#include "auth/AuthClientHandler.h"
#include "auth/RotatingKeyRing.h"

#include "KrbProtocol.hpp"

#include <gssapi.h>
#include <gssapi/gssapi_generic.h>
#include <gssapi/gssapi_krb5.h>
#include <gssapi/gssapi_ext.h>


class CephContext;
class Keyring;


class KrbClientHandler : public AuthClientHandler {

  public:
    KrbClientHandler(CephContext* ceph_ctx = nullptr) 
      : AuthClientHandler(ceph_ctx) {
      reset();
    }
    ~KrbClientHandler() override;
    
    int get_protocol() const override { return CEPH_AUTH_GSS; }
    void reset() override {
      m_gss_client_name = GSS_C_NO_NAME; 
      m_gss_service_name = GSS_C_NO_NAME; 
      m_gss_credentials = GSS_C_NO_CREDENTIAL;
      m_gss_sec_ctx = GSS_C_NO_CONTEXT;
      m_gss_buffer_out = {0, 0};
    }

    void prepare_build_request() override { };
    int build_request(bufferlist& buff_list) const override;
    int handle_response(int ret, 
                        bufferlist::const_iterator& buff_list,
			CryptoKey *session_key,
			std::string *connection_secret) override;

    bool build_rotating_request(bufferlist& buff_list) const override { 
      return false; 
    }

    AuthAuthorizer* build_authorizer(uint32_t service_id) const override;
    bool need_tickets() override { return false; }
    void set_global_id(uint64_t guid) override { global_id = guid; }


  private:
    gss_name_t m_gss_client_name; 
    gss_name_t m_gss_service_name; 
    gss_cred_id_t m_gss_credentials; 
    gss_ctx_id_t m_gss_sec_ctx; 
    gss_buffer_desc m_gss_buffer_out; 

  protected:
    void validate_tickets() override { } 
};

#endif    //-- KRB_CLIENT_HANDLER_HPP

