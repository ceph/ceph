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

#ifndef KRB_SERVICE_HANDLER_HPP
#define KRB_SERVICE_HANDLER_HPP

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
#include "auth/Auth.h"
#include "auth/cephx/CephxKeyServer.h"
#include "gss_utils.hpp"

class KrbServiceHandler : public AuthServiceHandler 
{

  public:
    explicit KrbServiceHandler(CephContext* cct, KeyServer* kserver) : 
      AuthServiceHandler(cct), 
      m_gss_buffer_out({0}), 
      m_gss_credentials(GSS_C_NO_CREDENTIAL), 
      m_gss_sec_ctx(GSS_C_NO_CONTEXT), 
      m_gss_service_name(GSS_C_NO_NAME), 
      m_key_server(kserver) { }
    ~KrbServiceHandler();
    int handle_request(bufferlist::iterator&, 
                       bufferlist&, 
                       uint64_t&, 
                       AuthCapsInfo&, 
                       uint64_t* auid = nullptr) override;

    int start_session(EntityName&, 
                      bufferlist::iterator&, 
                      bufferlist&, 
                      AuthCapsInfo&) override;

  private: 
    gss_buffer_desc m_gss_buffer_out; 
    gss_cred_id_t m_gss_credentials; 
    gss_ctx_id_t m_gss_sec_ctx; 
    gss_name_t m_gss_service_name; 
    KeyServer* m_key_server;

};


#endif    //-- KRB_SERVICE_HANDLER_HPP

// ----------------------------- END-OF-FILE --------------------------------//

