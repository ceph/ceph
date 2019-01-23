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

#include "KrbServiceHandler.hpp"
#include "KrbProtocol.hpp"
#include <errno.h>
#include <sstream>

#include "common/config.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_auth
#undef dout_prefix
#define dout_prefix *_dout << "krb5/gssapi service: " << entity_name <<  " : "


int KrbServiceHandler::handle_request(
  bufferlist::const_iterator& indata,
  size_t connection_secret_required_length,
  bufferlist *buff_list,
  uint64_t *global_id,
  AuthCapsInfo *caps,
  CryptoKey *session_key,
  std::string *connection_secret)
{
  auto result(0);
  gss_buffer_desc gss_buffer_in = {0, nullptr};
  gss_name_t gss_client_name = GSS_C_NO_NAME;
  gss_OID gss_object_id = {0};
  OM_uint32 gss_major_status(0); 
  OM_uint32 gss_minor_status(0);
  OM_uint32 gss_result_flags(0); 
  std::string status_str(" ");

  ldout(cct, 20) 
      << "KrbServiceHandler::handle_request() " << dendl; 

  KrbRequest krb_request; 
  KrbTokenBlob krb_token; 

  using ceph::decode;
  decode(krb_request, indata); 
  decode(krb_token, indata);

  gss_buffer_in.length = krb_token.m_token_blob.length();
  gss_buffer_in.value  = krb_token.m_token_blob.c_str();

  ldout(cct, 20) 
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
        ldout(cct, 20) 
            << "KrbServiceHandler::handle_response() : "
               "[KrbServiceHandler(GSS_S_CONTINUE_NEEDED)] " << dendl;
        result = 0;
        break;
      }

    case GSS_S_COMPLETE: 
      {
        result = 0;
        ldout(cct, 20) 
            << "KrbServiceHandler::handle_response() : "
               "[KrbServiceHandler(GSS_S_COMPLETE)] " << dendl; 
        if (!m_key_server->get_service_caps(entity_name, 
                                            CEPH_ENTITY_TYPE_MON, 
                                            *caps)) {
          result = (-EACCES);
          ldout(cct, 0)
              << "KrbServiceHandler::handle_response() : "
                 "ERROR: Could not get MONITOR CAPS : " << entity_name << dendl;
        } else {
          if (!caps->caps.c_str()) {
            result = (-EACCES);
            ldout(cct, 0)
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
        ldout(cct, 0) 
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
        static_cast<int>(GSSAuthenticationRequest::GSS_TOKEN);

    using ceph::encode;
    encode(krb_response, *buff_list);

    krb_token.m_token_blob.append(buffer::create_static(
                                    m_gss_buffer_out.length, 
                                    reinterpret_cast<char*>
                                      (m_gss_buffer_out.value)));
    encode(krb_token, *buff_list);
    ldout(cct, 20) 
        << "KrbServiceHandler::handle_request() : Token Blob: " << "\n"; 
    krb_token.m_token_blob.hexdump(*_dout);
    *_dout << dendl;
  }
  gss_release_name(&gss_minor_status, &gss_client_name);
  return result;
}

int KrbServiceHandler::start_session(
  const EntityName& name,
  size_t connection_secret_required_length,
  bufferlist *buff_list,
  AuthCapsInfo *caps,
  CryptoKey *session_key,
  std::string *connection_secret)
{
  gss_buffer_desc gss_buffer_in = {0, nullptr};
  gss_OID gss_object_id = GSS_C_NT_HOSTBASED_SERVICE;
  gss_OID_set gss_mechs_wanted = GSS_C_NO_OID_SET;
  OM_uint32 gss_major_status(0); 
  OM_uint32 gss_minor_status(0);
  std::string gss_service_name(cct->_conf.get_val<std::string>
                                            ("gss_target_name"));

  gss_buffer_in.length = gss_service_name.length();
  gss_buffer_in.value  = (const_cast<char*>(gss_service_name.c_str()));
  entity_name = name;

  gss_major_status = gss_import_name(&gss_minor_status, 
                                     &gss_buffer_in, 
                                     gss_object_id, 
                                     &m_gss_service_name);
  if (gss_major_status != GSS_S_COMPLETE) {
    auto status_str(gss_auth_show_status(gss_major_status, 
                                         gss_minor_status));
    ldout(cct, 0) 
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
    ldout(cct, 0) 
        << "ERROR: KrbServiceHandler::start_session() "
           "[gss_acquire_cred()] failed! " 
        << gss_major_status << " " 
        << gss_minor_status << " " 
        << status_str 
        << dendl;
    return (-EPERM);
  } else {
    KrbResponse krb_response;
    krb_response.m_response_type = 
        static_cast<int>(GSSAuthenticationRequest::GSS_MUTUAL);

    using ceph::encode;
    encode(krb_response, *buff_list);
    return (CEPH_AUTH_GSS);
  }
}

KrbServiceHandler::~KrbServiceHandler()
{
  OM_uint32 gss_minor_status(0); 

  gss_release_name(&gss_minor_status, &m_gss_service_name);
  gss_release_cred(&gss_minor_status, &m_gss_credentials); 
  gss_delete_sec_context(&gss_minor_status, &m_gss_sec_ctx, GSS_C_NO_BUFFER); 
  gss_release_buffer(&gss_minor_status, static_cast<gss_buffer_t>(&m_gss_buffer_out)); 
}

