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

#include "KrbProtocol.hpp"

#include "common/Clock.h"
#include "common/config.h"
#include "common/debug.h"
#include "include/buffer.h"

#define dout_subsys ceph_subsys_auth
#undef dout_prefix
#define dout_prefix *_dout << "krb5/gssapi protocol: "


std::string gss_auth_show_status(const OM_uint32 gss_major_status, 
                                 const OM_uint32 gss_minor_status)
{
  const std::string STR_DOT(".");
  const std::string STR_BLANK(" ");

  gss_buffer_desc gss_str_status = {0, nullptr};
  OM_uint32 gss_maj_status(0); 
  OM_uint32 gss_min_status(0);
  OM_uint32 gss_ctx_message(-1);

  std::string str_status("");

  const auto gss_complete_status_str_format = [&](const uint32_t gss_status) {
    if (gss_status == GSS_S_COMPLETE) {
      std::string str_tmp("");
      str_tmp.append(reinterpret_cast<char*>(gss_str_status.value), 
                     gss_str_status.length);
      str_tmp += STR_DOT;
      if (gss_ctx_message != 0) {
        str_tmp += STR_BLANK;
      }
      return str_tmp;
    }
    return STR_BLANK;
  };

  while (gss_ctx_message != 0) {
    gss_maj_status = gss_display_status(&gss_min_status, 
                                        gss_major_status, 
                                        GSS_C_GSS_CODE, 
                                        GSS_C_NO_OID, 
                                        &gss_ctx_message, 
                                        &gss_str_status); 
    
    if (gss_maj_status == GSS_S_COMPLETE) {
      str_status += gss_complete_status_str_format(gss_maj_status);
      gss_release_buffer(&gss_min_status, &gss_str_status);
    }
  }

  if (gss_major_status == GSS_S_FAILURE) {
    gss_ctx_message = -1;
    while (gss_ctx_message != 0) {
      gss_maj_status = gss_display_status(&gss_min_status, 
                                          gss_minor_status, 
                                          GSS_C_MECH_CODE,
                                          const_cast<gss_OID>(&GSS_API_KRB5_OID_PTR),
                                          &gss_ctx_message, 
                                          &gss_str_status); 
      if (gss_maj_status == GSS_S_COMPLETE) {
        str_status += gss_complete_status_str_format(gss_maj_status);
        gss_release_buffer(&gss_min_status, &gss_str_status);
      }
    }
  }
  return str_status;
}

