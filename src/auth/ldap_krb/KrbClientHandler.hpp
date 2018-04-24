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

#ifndef KRB_CLIENT_HANDLER_HPP
#define KRB_CLIENT_HANDLER_HPP

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

#include "auth/Auth.h"
#include "auth/AuthClientHandler.h"

#include "KrbProtocol.hpp"
#include "common/ceph_context.h"
#include "common/config.h"
#include "ceph_krb_auth.hpp"


class KrbClientHandler : public AuthClientHandler 
{

  public:
    KrbClientHandler(CephContext* ceph_ctx = nullptr, 
                     RotatingKeyRing* key_secrets = nullptr) 
      : AuthClientHandler(ceph_ctx), 
      m_gss_client_name(GSS_C_NO_NAME), 
      m_gss_service_name(GSS_C_NO_NAME), 
      m_gss_credentials(GSS_C_NO_CREDENTIAL), 
      m_gss_sec_ctx(GSS_C_NO_CONTEXT), 
      m_gss_buffer_out({0})
    {
      reset();
    }
    ~KrbClientHandler(); 

    void reset() override
    {
      RWLock::WLocker lck(lock);
      m_gss_client_name = GSS_C_NO_NAME; 
      m_gss_service_name = GSS_C_NO_NAME; 
      m_gss_credentials = GSS_C_NO_CREDENTIAL;
      m_gss_sec_ctx = GSS_C_NO_CONTEXT;
      m_gss_buffer_out = {0};
    }

    AuthAuthorizer* build_authorizer(uint32_t) const override;
    void prepare_build_request() override { }
    void validate_tickets() override { } 
    int build_request(bufferlist&) const override;
    int handle_response(int, bufferlist::iterator&) override;
    bool build_rotating_request(bufferlist&) const override { return false; }
    int get_protocol() const override { return CEPH_AUTH_KRB5; }
    bool need_tickets() override { return false; }
    void set_global_id(uint64_t guid) override {
      RWLock::WLocker lck(lock);
      global_id = guid;
    }

  private:
    gss_name_t m_gss_client_name; 
    gss_name_t m_gss_service_name; 
    gss_cred_id_t m_gss_credentials; 
    gss_ctx_id_t m_gss_sec_ctx; 
    gss_buffer_desc m_gss_buffer_out; 
};


#endif    //-- KRB_CLIENT_HANDLER_HPP

// ----------------------------- END-OF-FILE --------------------------------//

