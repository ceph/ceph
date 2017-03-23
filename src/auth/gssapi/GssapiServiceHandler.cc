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


#include "GssapiProtocol.h"
#include "GssapiServiceHandler.h"

#include <errno.h>
#include <sstream>

#include "common/config.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_auth
#undef dout_prefix
#define dout_prefix *_dout << "gssapi server " << entity_name << ": "

GssapiServiceHandler::~GssapiServiceHandler(void)
{
  OM_uint32 min_stat;

  gss_release_name(&min_stat, &gss_service_name);
  gss_release_cred(&min_stat, &gss_cred);
  gss_delete_sec_context(&min_stat, &gss_sec_context, GSS_C_NO_BUFFER);
  gss_release_buffer(&min_stat, (gss_buffer_t)&gss_output_token);
}

int GssapiServiceHandler::start_session(
  EntityName& name,
  bufferlist::iterator& indata,
  bufferlist& result_bl,
  AuthCapsInfo& caps)
{
  ldout(cct, 20) << "start_session" << dendl;

  entity_name = name;

  int ret = 0;
  OM_uint32 maj_stat, min_stat;
  gss_buffer_desc name_buffer;
  gss_OID input_name_type = (gss_OID) gss_nt_service_name;

  string service_name = "ceph";
  name_buffer.value = (void *)service_name.c_str();
  name_buffer.length = service_name.length();

  maj_stat = gss_import_name(
                     &min_stat,
                     &name_buffer,
                     input_name_type,
                     &gss_service_name);
  if (maj_stat != GSS_S_COMPLETE) {
    string st = auth_gssapi_display_status(maj_stat, min_stat);
    ldout(cct, 0) << "gss_import_name() failed: " << maj_stat << " " << min_stat << " " << st << dendl;
  }

  gss_OID_set desired_mechs = GSS_C_NO_OID_SET;

  maj_stat = gss_acquire_cred(
                     &min_stat,
                     gss_service_name,
                     0,
                     desired_mechs,
                     GSS_C_ACCEPT,
                     &gss_cred,
                     NULL,
                     NULL);
  if (maj_stat != GSS_S_COMPLETE) {
    string st = auth_gssapi_display_status(maj_stat, min_stat);
    ldout(cct, 0) << "gss_acquire_cred() failed: " << maj_stat << " " << min_stat << " " << st << dendl;
    ret = -EPERM;
  } else {
    GssapiResponseHeader header;
    header.response_type = GSSAPI_MUTUAL_AUTH;
    ::encode(header, result_bl);
    ret = CEPH_AUTH_GSSAPI;
  }

  return ret;
}

int GssapiServiceHandler::handle_request(
  bufferlist::iterator& indata,
  bufferlist& result_bl,
  uint64_t& global_id,
  AuthCapsInfo& caps,
  uint64_t *auid)
{
  ldout(cct, 20) << "handle_request" << dendl;

  int ret = 0;

  struct GssapiRequestHeader header;
  ::decode(header, indata);

  GssapiTokenBlob token_blob;
  ::decode(token_blob, indata);

  ldout(cct, 20) << "gssapi input token blob is:\n";
  token_blob.blob.hexdump(*_dout);
  *_dout << dendl;

  OM_uint32 maj_stat, min_stat;

  gss_buffer_desc input_token = {0};
  gss_name_t client_name = GSS_C_NO_NAME;
  OM_uint32 ret_flags;
  gss_OID doid;

  input_token.value = token_blob.blob.c_str();
  input_token.length = token_blob.blob.length();

  if (gss_output_token.length != 0) {
    gss_release_buffer(&min_stat, (gss_buffer_t)&gss_output_token);
  }

  maj_stat = gss_accept_sec_context(
                     &min_stat,
                     &gss_sec_context,
                     gss_cred,
                     &input_token,
                     GSS_C_NO_CHANNEL_BINDINGS,
                     &client_name,
                     &doid,
                     &gss_output_token,
                     &ret_flags,
                     NULL,  /* time_rec */
                     NULL); /* del_cred_handle */
  switch (maj_stat) {
    case GSS_S_COMPLETE:
      ldout(cct, 20) << "gss_accept_sec_context() SUCCESS " << dendl;
      ret = 0;

      if (!key_server->get_service_caps(entity_name, CEPH_ENTITY_TYPE_MON, caps)) {
        ldout(cct, 0) << " could not get mon caps for " << entity_name << dendl;
        ret = -EACCES;
      } else {
        char *caps_str = caps.caps.c_str();
        if (!caps_str || !caps_str[0]) {
          ldout(cct,0) << "mon caps null for " << entity_name << dendl;
          ret = -EACCES;
        }
      }
      break;

    case GSS_S_CONTINUE_NEEDED:
      ldout(cct, 20) << "gss_accept_sec_context() CONTINUE_NEEDED" << dendl;
      ret = 0;
      break;

    default:
      string st = auth_gssapi_display_status(maj_stat, min_stat);
      ldout(cct, 0) << "gss_accept_sec_context() failed: " << maj_stat << " " << min_stat << " " << st << dendl;
      ret = -EPERM;
      break;
  }

  if (gss_output_token.length != 0) {
    GssapiResponseHeader header;
    header.response_type = GSSAPI_TOKEN;
    ::encode(header, result_bl);

    GssapiTokenBlob token_blob;

    token_blob.blob.append(buffer::create_static(
          gss_output_token.length,
          (char*)gss_output_token.value));

    ::encode(token_blob, result_bl);

    ldout(cct, 20) << "gssapi output token blob is:\n";
    token_blob.blob.hexdump(*_dout);
    *_dout << dendl;
  }

  gss_release_name(&min_stat, &client_name);

  return ret;
}
