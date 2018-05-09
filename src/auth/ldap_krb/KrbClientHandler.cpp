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

#include "KrbClientHandler.hpp"

#include <errno.h>
#include <string>
#include "KrbProtocol.hpp"

#include "auth/KeyRing.h"
#include "common/config.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_auth
#undef dout_prefix
#define dout_prefix *_dout << "krb5/gssapi request: "

struct AuthAuthorizer;

AuthAuthorizer* KrbClientHandler::
build_authorizer(uint32_t service_id) const 
{
  constexpr auto SUBSYSTEM_ID(20);

  ldout(cct, SUBSYSTEM_ID) 
      << "KrbClientHandler::build_authorizer(): Service: " 
      << ceph_entity_type_name(service_id) << dendl; 

  RWLock::RLocker lck(lock);
  KrbAuthorizer* krb_auth = new KrbAuthorizer();
  if (krb_auth) {
    krb_auth->build_authorizer(cct->_conf->name, global_id);
  }
  return krb_auth;
}

KrbClientHandler::~KrbClientHandler() 
{
  OM_uint32 gss_major_status(0); 
  OM_uint32 gss_minor_status(0); 

  gss_release_name(&gss_minor_status, &m_gss_client_name);
  gss_release_name(&gss_minor_status, &m_gss_service_name);
  gss_release_cred(&gss_minor_status, &m_gss_credentials); 
  gss_delete_sec_context(&gss_minor_status, &m_gss_sec_ctx, GSS_C_NO_BUFFER); 
  gss_release_buffer(&gss_minor_status, static_cast<gss_buffer_t>(&m_gss_buffer_out)); 
}

int KrbClientHandler::build_request(bufferlist& buff_list) const
{
  constexpr auto SUBSYSTEM_ID(20);

  ldout(cct, SUBSYSTEM_ID) 
      << "KrbClientHandler::build_request() " << dendl; 

  RWLock::RLocker lck(lock);
  KrbTokenBlob krb_token; 
  KrbRequest krb_request; 

  krb_request.m_request_type = 
      static_cast<int>(gss_client_auth::
                          GSSAuthenticationRequest::GSS_TOKEN);
  using ceph::encode;
  encode(krb_request, buff_list);

  if (m_gss_buffer_out.length != 0) {
    krb_token.m_token_blob.append(buffer::create_static(
                                    m_gss_buffer_out.length, 
                                    reinterpret_cast<char*>
                                      (m_gss_buffer_out.value)));

    encode(krb_token, buff_list);
    ldout(cct, SUBSYSTEM_ID) 
        << "KrbClientHandler::build_request() : Token Blob: " << "\n"; 
    krb_token.m_token_blob.hexdump(*_dout);
    *_dout << dendl;
  }
  return 0;
}

int KrbClientHandler::handle_response(int ret, 
                                      bufferlist::iterator& buff_list)
{
  constexpr auto SUBSYSTEM_ID(20);
  constexpr auto SUBSYSTEM_ZERO(0);

  auto result(ret);
  gss_buffer_desc gss_buffer_in; 
  gss_OID_set_desc gss_mechs_wanted;
  OM_uint32 gss_major_status(0); 
  OM_uint32 gss_minor_status(0); 
  OM_uint32 gss_wanted_flags(GSS_C_MUTUAL_FLAG | 
                             GSS_C_INTEG_FLAG);
  OM_uint32 gss_result_flags(0);

  ldout(cct, SUBSYSTEM_ID) 
      << "KrbClientHandler::handle_response() " << dendl; 

  RWLock::WLocker lck(lock);
  if (result < 0) {
    return result;
  }

  gss_mechs_wanted.elements = const_cast<gss_OID>(&GSS_API_SPNEGO_OID_PTR);
  gss_mechs_wanted.count = 1; 

  KrbResponse krb_response; 
  using ceph::decode;
  decode(krb_response, buff_list);
  if (m_gss_credentials == GSS_C_NO_CREDENTIAL) {
    gss_buffer_desc krb_client_name_buff;
    gss_OID krb_client_type = GSS_C_NT_USER_NAME;
    std::string krb_client_name(cct->_conf->name.to_str());

    krb_client_name_buff.length = krb_client_name.length();
    krb_client_name_buff.value  = /*reinterpret_cast<void*>*/
                                    (const_cast<char*>(krb_client_name.c_str()));
    /*  Defined in msgr.h
       #define CEPH_ENTITY_TYPE_CLIENT 0x08
    */
    if (cct->_conf->name.get_type() == CEPH_ENTITY_TYPE_CLIENT) {
      gss_major_status = gss_import_name(&gss_minor_status, 
                                         &gss_buffer_in, 
                                         krb_client_type, 
                                         &m_gss_client_name); 
      if (gss_major_status != GSS_S_COMPLETE) {
        auto status_str(gss_auth_show_status(gss_major_status, 
                                             gss_minor_status));
        ldout(cct, SUBSYSTEM_ZERO) 
            << "ERROR: KrbClientHandler::handle_response() "
               "[gss_import_name(gss_client_name)] failed! " 
            << gss_major_status << " " 
            << gss_minor_status << " " 
            << status_str 
            << dendl;
      }
    }

    gss_major_status = gss_acquire_cred(&gss_minor_status, 
                                        m_gss_client_name, 
                                        0, 
                                        &gss_mechs_wanted, 
                                        GSS_C_INITIATE, 
                                        &m_gss_credentials, 
                                        nullptr, 
                                        nullptr);
    if (gss_major_status != GSS_S_COMPLETE) {
      auto status_str(gss_auth_show_status(gss_major_status, 
                                           gss_minor_status));
      ldout(cct, SUBSYSTEM_ZERO) 
          << "ERROR: KrbClientHandler::handle_response() "
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

    gss_buffer_desc krb_input_name_buff;
    gss_OID krb_input_type = GSS_C_NT_HOSTBASED_SERVICE;
    krb_input_name_buff.length = gss_utils::GSS_TARGET_DEFAULT_NAME.length();
    krb_input_name_buff.value  = /*reinterpret_cast<void*>*/
                                    (const_cast<char*>(gss_utils::GSS_TARGET_DEFAULT_NAME.c_str()));

    gss_major_status = gss_import_name(&gss_minor_status, 
                                       &krb_input_name_buff, 
                                       krb_input_type, 
                                       &m_gss_service_name); 
    if (gss_major_status != GSS_S_COMPLETE) {
      auto status_str(gss_auth_show_status(gss_major_status, 
                                           gss_minor_status));
      ldout(cct, SUBSYSTEM_ZERO) 
          << "ERROR: KrbClientHandler::handle_response() "
             "[gss_import_name(gss_service_name)] failed! " 
          << gss_major_status << " " 
          << gss_minor_status << " " 
          << status_str 
          << dendl;
    }
  }
  else {
    KrbTokenBlob krb_token; 
    using ceph::decode;
    decode(krb_token, buff_list);
    ldout(cct, SUBSYSTEM_ID) 
        << "KrbClientHandler::handle_response() : Token Blob: " << "\n"; 
    krb_token.m_token_blob.hexdump(*_dout);
    *_dout << dendl;

    gss_buffer_in.length = krb_token.m_token_blob.length();
    gss_buffer_in.value  = krb_token.m_token_blob.c_str();
  }

  const gss_OID gss_mech_type = gss_mechs_wanted.elements; 
  if (m_gss_buffer_out.length != 0) {
    gss_release_buffer(&gss_minor_status, 
                       static_cast<gss_buffer_t>(&m_gss_buffer_out)); 
  }

  gss_major_status = gss_init_sec_context(&gss_minor_status, 
                                          m_gss_credentials, 
                                          &m_gss_sec_ctx, 
                                          m_gss_service_name, 
                                          gss_mech_type, 
                                          gss_wanted_flags, 
                                          0, 
                                          nullptr, 
                                          &gss_buffer_in, 
                                          nullptr, 
                                          &m_gss_buffer_out, 
                                          &gss_result_flags, 
                                          nullptr);
  switch (gss_major_status) {
    case GSS_S_CONTINUE_NEEDED: 
      ldout(cct, SUBSYSTEM_ID) 
          << "KrbClientHandler::handle_response() : "
             "[gss_init_sec_context(GSS_S_CONTINUE_NEEDED)] " << dendl; 
      result = (-EAGAIN);
      break;

    case GSS_S_COMPLETE: 
      ldout(cct, SUBSYSTEM_ID) 
          << "KrbClientHandler::handle_response() : "
             "[gss_init_sec_context(GSS_S_COMPLETE)] " << dendl; 
      result = 0;
      break;

    default: 
      auto status_str(gss_auth_show_status(gss_major_status, 
                                           gss_minor_status));
      ldout(cct, SUBSYSTEM_ZERO) 
          << "ERROR: KrbClientHandler::handle_response() "
             "[gss_init_sec_context()] failed! " 
          << gss_major_status << " " 
          << gss_minor_status << " " 
          << status_str 
          << dendl;
      result = (-EPERM);
      break;
  }

  return result;
}

// ----------------------------- END-OF-FILE --------------------------------//


