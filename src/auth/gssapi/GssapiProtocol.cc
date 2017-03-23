// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "GssapiProtocol.h"

#include "common/Clock.h"
#include "common/config.h"
#include "common/debug.h"
#include "include/buffer.h"

#define dout_subsys ceph_subsys_auth
#undef dout_prefix
#define dout_prefix *_dout << "gssapi: "

static gss_OID_desc gss_mech_krb5_oid =
        { 9, (void *)"\052\206\110\206\367\022\001\002\002" };

string auth_gssapi_display_status(
  OM_uint32 major,
  OM_uint32 minor)
{
  string str;
  OM_uint32 maj_stat;
  OM_uint32 min_stat;
  OM_uint32 message_context = 0;
  gss_buffer_desc status_string = {0};

  do {
    maj_stat = gss_display_status(
                       &min_stat,
                       major,
                       GSS_C_GSS_CODE,
                       GSS_C_NO_OID,
                       &message_context,
                       &status_string);
    if (maj_stat == GSS_S_COMPLETE) {
      str.append((char *)status_string.value, status_string.length);
      str += ".";
      if (message_context != 0) {
          str += " ";
      }
      gss_release_buffer(&min_stat, &status_string);
    }
  } while (message_context != 0);

  if (major == GSS_S_FAILURE) {
    do {
       maj_stat = gss_display_status(
                 &min_stat,
                 minor,
                 GSS_C_MECH_CODE,
                 &gss_mech_krb5_oid,
                 &message_context,
                 &status_string);
       if (maj_stat == GSS_S_COMPLETE) {
         str.append((char *)status_string.value, status_string.length);
         str += ".";
         if (message_context != 0) {
             str += " ";
         }
         gss_release_buffer(&min_stat, &status_string);
       }
    } while (message_context != 0);
  }

  return str;
}
