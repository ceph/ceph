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

#include "KrbServiceHandler.hpp"

#include <errno.h>
#include <sstream>

#include "KrbProtocol.hpp"
#include "common/config.h"
#include "common/debug.h"


#define dout_subsys ceph_subsys_auth
#undef dout_prefix
#define dout_prefix *_dout << "krb5/gssapi service: " << entity_name <<  " : "


int KrbServiceHandler::handle_request(bufferlist::iterator& indata, 
                                      bufferlist& buff_list, 
                                      uint64_t& global_id, 
                                      AuthCapsInfo& caps, 
                                      uint64_t* auid) 
{
  constexpr auto SUBSYSTEM_ID(20);
  constexpr auto SUBSYSTEM_ZERO(0);

  auto result(0);
  gss_buffer_desc gss_buffer_in = {0};
  gss_name_t gss_client_name = GSS_C_NO_NAME;
  gss_OID gss_object_id = {0};
  OM_uint32 gss_major_status(0); 
  OM_uint32 gss_minor_status(0);
  OM_uint32 gss_result_flags(0); 
  std::string status_str(" ");

  ldout(cct, SUBSYSTEM_ID) 
      << "KrbServiceHandler::handle_request() " << dendl; 
  KrbRequest krb_request; 
  KrbTokenBlob krb_token; 
  ::decode(krb_request, indata); 
  ::decode(krb_token, indata);

  gss_buffer_in.length = krb_token.m_token_blob.length();
  gss_buffer_in.value  = krb_token.m_token_blob.c_str();

  ldout(cct, SUBSYSTEM_ID) 
      << "KrbClientHandler::handle_request() : Token Blob: " 
      << "\n"; 
  krb_token.m_token_blob.hexdump(*_dout); 
  *_dout << dendl;

  if (m_gss_buffer_out.length != 0) {
    gss_release_buffer(&gss_minor_status, 
                       static_cast<gss_buffer_t>(&m_gss_buffer_out)); 
  }

  gss_major_status = gss_accept_sec_context(&gss_minor_status, 
                                            &m_gss_sec_ctx, 
                                            m_gss_credentials, 
                                            &gss_buffer_in, 
                                            GSS_C_NO_CHANNEL_BINDINGS, 
                                            &gss_client_name, 
                                            &gss_object_id, 
                                            &m_gss_buffer_out, 
                                            &gss_result_flags, 
                                            nullptr, 
                                            nullptr);
  switch (gss_major_status) {
    case GSS_S_CONTINUE_NEEDED: 
      {
        ldout(cct, SUBSYSTEM_ID) 
            << "KrbServiceHandler::handle_response() : "
               "[KrbServiceHandler(GSS_S_CONTINUE_NEEDED)] " << dendl;
        result = 0;
        break;
      }

    case GSS_S_COMPLETE: 
      {
        result = 0;
        ldout(cct, SUBSYSTEM_ID) 
            << "KrbServiceHandler::handle_response() : "
               "[KrbServiceHandler(GSS_S_COMPLETE)] " << dendl; 
        if (!m_key_server->get_service_caps(entity_name, 
                                            CEPH_ENTITY_TYPE_MON, 
                                            caps)) {
          result = (-EACCES);
          ldout(cct, SUBSYSTEM_ZERO)
              << "KrbServiceHandler::handle_response() : "
                 "ERROR: Could not get MONITOR CAPS : " << entity_name << dendl;
        }
        else {
          if (!caps.caps.c_str()) {
            result = (-EACCES);
            ldout(cct, SUBSYSTEM_ZERO)
                << "KrbServiceHandler::handle_response() : "
                   "ERROR: MONITOR CAPS invalid : " << entity_name << dendl;
          }
        }
        break;
      }
            
    default: 
      {
        status_str = gss_auth_show_status(gss_major_status, 
                                          gss_minor_status);
        ldout(cct, SUBSYSTEM_ZERO) 
            << "ERROR: KrbServiceHandler::handle_response() "
               "[gss_accept_sec_context()] failed! " 
            << gss_major_status << " " 
            << gss_minor_status << " " 
            << status_str 
            << dendl;
        result = (-EPERM);
        break;
      }
  }

  if (m_gss_buffer_out.length != 0) {
    KrbResponse krb_response;
    KrbTokenBlob krb_token;
    krb_response.m_response_type = 
        static_cast<int>(gss_client_auth::
                         GSSAuthenticationRequest::GSS_TOKEN);
    ::encode(krb_response, buff_list);

    krb_token.m_token_blob.append(buffer::create_static(
                                    m_gss_buffer_out.length, 
                                    reinterpret_cast<char*>
                                      (m_gss_buffer_out.value)));
    ::encode(krb_token, buff_list);
    ldout(cct, SUBSYSTEM_ID) 
        << "KrbServiceHandler::handle_request() : Token Blob: " << "\n"; 
    krb_token.m_token_blob.hexdump(*_dout);
    *_dout << dendl;
  }
  gss_release_name(&gss_minor_status, &gss_client_name);
  return result;
}

int KrbServiceHandler::start_session(EntityName& name, 
                                     bufferlist::iterator& indata, 
                                     bufferlist& buff_list,
                                     AuthCapsInfo& caps)
{
  constexpr auto SUBSYSTEM_ID(20);
  constexpr auto SUBSYSTEM_ZERO(0);

  auto result(0);
  gss_buffer_desc gss_buffer_in = {0};
  gss_name_t gss_client_name = GSS_C_NO_NAME;
  gss_OID gss_object_id = GSS_C_NT_HOSTBASED_SERVICE;
  gss_OID_set gss_mechs_wanted = GSS_C_NO_OID_SET;
  OM_uint32 gss_major_status(0); 
  OM_uint32 gss_minor_status(0);
  OM_uint32 gss_result_flags(0);
  std::string gss_service_name(gss_utils::GSS_TARGET_DEFAULT_NAME);

  gss_buffer_in.length = gss_utils::GSS_TARGET_DEFAULT_NAME.length();
  gss_buffer_in.value  = /*reinterpret_cast<void*>*/(const_cast<char*>(gss_service_name.c_str()));
  entity_name = name;

  gss_major_status = gss_import_name(&gss_minor_status, 
                                     &gss_buffer_in, 
                                     gss_object_id, 
                                     &m_gss_service_name);
  if (gss_major_status != GSS_S_COMPLETE) {
    auto status_str(gss_auth_show_status(gss_major_status, 
                                         gss_minor_status));
    ldout(cct, SUBSYSTEM_ZERO) 
        << "ERROR: KrbServiceHandler::start_session() "
           "[gss_import_name(gss_client_name)] failed! " 
        << gss_major_status << " " 
        << gss_minor_status << " " 
        << status_str 
        << dendl;
  }

  gss_major_status = gss_acquire_cred(&gss_minor_status, 
                                      m_gss_service_name, 
                                      0, 
                                      gss_mechs_wanted, 
                                      GSS_C_ACCEPT, 
                                      &m_gss_credentials, 
                                      nullptr, 
                                      nullptr);
  if (gss_major_status != GSS_S_COMPLETE) {
    auto status_str(gss_auth_show_status(gss_major_status, 
                                         gss_minor_status));
    ldout(cct, SUBSYSTEM_ZERO) 
        << "ERROR: KrbServiceHandler::start_session() "
           "[gss_acquire_cred()] failed! " 
        << gss_major_status << " " 
        << gss_minor_status << " " 
        << status_str 
        << dendl;
    /*  We are using: Permission Error (EPERM = 1), but need to check
        if Permission Denied (EACCES = 13) would be more appropriate.
        <errno-base.h>
    */
    return (-EACCES);
  }
  else {
    KrbResponse krb_response;
    krb_response.m_response_type = 
        static_cast<int>(gss_client_auth::
                            GSSAuthenticationRequest::GSS_MUTUAL);
    ::encode(krb_response, buff_list);
    return (CEPH_AUTH_KRB5);
  }
  //return result;
}

KrbServiceHandler::~KrbServiceHandler()
{
  OM_uint32 gss_major_status(0); 
  OM_uint32 gss_minor_status(0); 

  gss_release_name(&gss_minor_status, &m_gss_service_name);
  gss_release_cred(&gss_minor_status, &m_gss_credentials); 
  gss_delete_sec_context(&gss_minor_status, &m_gss_sec_ctx, GSS_C_NO_BUFFER); 
  gss_release_buffer(&gss_minor_status, static_cast<gss_buffer_t>(&m_gss_buffer_out)); 
}


