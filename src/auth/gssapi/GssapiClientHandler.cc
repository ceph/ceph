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

#include <errno.h>

#include "GssapiProtocol.h"
#include "GssapiClientHandler.h"

#include "auth/KeyRing.h"
#include "common/config.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_auth
#undef dout_prefix
#define dout_prefix *_dout << "gssapi client: "

static gss_OID_desc gss_spnego_mechanism_oid_desc =
    {6, (void *)"\x2b\x06\x01\x05\x05\x02"};

GssapiClientHandler::~GssapiClientHandler(void)
{
  OM_uint32 min_stat;

  gss_release_name(&min_stat, &gss_client_name);
  gss_release_name(&min_stat, &gss_service_name);
  gss_release_cred(&min_stat, &gss_cred);
  gss_delete_sec_context(&min_stat, &gss_sec_context, GSS_C_NO_BUFFER);
  gss_release_buffer(&min_stat, (gss_buffer_t)&gss_output_token);
}

int GssapiClientHandler::build_request(bufferlist& bl) const
{
  ldout(cct, 20) << "build_request" << dendl;
  RWLock::RLocker l(lock);

  GssapiTokenBlob token_blob;
  GssapiRequestHeader header;
  header.request_type = GSSAPI_TOKEN;
  ::encode(header, bl);

  if (gss_output_token.length != 0) {
    token_blob.blob.append(buffer::create_static(
          gss_output_token.length,
          (char*)gss_output_token.value));

    ::encode(token_blob, bl);

    ldout(cct, 20) << "build_request: gssapi token blob is:\n";
    token_blob.blob.hexdump(*_dout);
    *_dout << dendl;
  }

  return 0;
}


int GssapiClientHandler::handle_response(
  int ret,
  bufferlist::iterator& indata)
{
  ldout(cct, 20) << "handle_response ret = " << ret << dendl;
  RWLock::WLocker l(lock);

  if (ret < 0)
    return ret;

  int retval = 0;
  OM_uint32 maj_stat, min_stat;
  gss_buffer_desc input_token = {0};
  gss_OID_set_desc desired_mechs;

  desired_mechs.elements = &gss_spnego_mechanism_oid_desc;
  desired_mechs.count = 1;

  struct GssapiResponseHeader header;
  ::decode(header, indata);

  if (gss_cred == GSS_C_NO_CREDENTIAL) {
    string client_name = cct->_conf->name.to_str();
    gss_OID client_name_type = gss_nt_user_name;
    gss_buffer_desc client_name_buffer;

    client_name_buffer.value = (void *)client_name.c_str();
    client_name_buffer.length = client_name.length();

    if (cct->_conf->name.get_type() == CEPH_ENTITY_TYPE_CLIENT) {
      maj_stat = gss_import_name(
                         &min_stat,
                         &client_name_buffer,
                         client_name_type,
                         &gss_client_name);
      if (maj_stat != GSS_S_COMPLETE) {
        string st = auth_gssapi_display_status(maj_stat, min_stat);
        ldout(cct, 0) << "gss_import_name() failed: " << maj_stat << " " << min_stat << " " << st << dendl;
      }
    }

    maj_stat = gss_acquire_cred(
                       &min_stat,
                       gss_client_name,
                       0,
                       &desired_mechs,
                       GSS_C_INITIATE,
                       &gss_cred,
                       NULL,
                       NULL);
    if (maj_stat != GSS_S_COMPLETE) {
      string st = auth_gssapi_display_status(maj_stat, min_stat);
      ldout(cct, 0) << "gss_acquire_cred() failed: " << maj_stat << " " << min_stat << " " << st << dendl;
      return -EPERM;
    }

    string target_name = "ceph";
    gss_OID input_name_type = gss_nt_service_name;
    gss_buffer_desc input_name_buffer;

    input_name_buffer.value = (void *)target_name.c_str();
    input_name_buffer.length = target_name.length();

    maj_stat = gss_import_name(
                       &min_stat,
                       &input_name_buffer,
                       input_name_type,
                       &gss_service_name);
    if (maj_stat != GSS_S_COMPLETE) {
      // error parsing name
      string st = auth_gssapi_display_status(maj_stat, min_stat);
      ldout(cct, 0) << "gss_import_name() failed: " << maj_stat << " " << min_stat << " " << st << dendl;
    }

  } else {

    GssapiTokenBlob input_token_blob;
    ::decode(input_token_blob, indata);

    ldout(cct, 20) << "gssapi token blob is:\n";
    input_token_blob.blob.hexdump(*_dout);
    *_dout << dendl;

    input_token.value = input_token_blob.blob.c_str();
    input_token.length = input_token_blob.blob.length();
  }

  const gss_OID mech_type = desired_mechs.elements;
  OM_uint32 req_flags = GSS_C_MUTUAL_FLAG | GSS_C_INTEG_FLAG;
  OM_uint32 ret_flags = 0;

  if (gss_output_token.length != 0) {
    gss_release_buffer(&min_stat, (gss_buffer_t)&gss_output_token);
  }

  maj_stat = gss_init_sec_context(
                     &min_stat,
                     gss_cred,
                     &gss_sec_context,
                     gss_service_name,
                     mech_type,
                     req_flags,
                     0,
                     NULL, /* channel bindings */
                     &input_token,
                     NULL, /* mech type */
                     &gss_output_token,
                     &ret_flags,
                     NULL);  /* time_rec */
  switch (maj_stat) {
    case GSS_S_COMPLETE:
      ldout(cct, 20) << "gss_init_sec_context() COMPLETE" << dendl;
      retval = 0;
      break;

    case GSS_S_CONTINUE_NEEDED:
      ldout(cct, 20) << "gss_init_sec_context() CONTINUE_NEEDED" << dendl;
      retval = -EAGAIN;
      break;

    default:
      string st = auth_gssapi_display_status(maj_stat, min_stat);
      ldout(cct, 0) << "gss_init_sec_context() failed: " << maj_stat << " " << min_stat << " " << st << dendl;

      retval = -EPERM;
      break;
  }

  return retval;
}


AuthAuthorizer *GssapiClientHandler::build_authorizer(uint32_t service_id) const
{
  RWLock::RLocker l(lock);
  ldout(cct, 20) << "build_authorizer for service " << ceph_entity_type_name(service_id) << dendl;
  GssapiAuthorizer *auth = new GssapiAuthorizer();
  if (auth) {
    auth->build_authorizer(cct->_conf->name, global_id);
  }
  return auth;
}
