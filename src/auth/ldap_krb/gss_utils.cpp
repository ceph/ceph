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

#include <cstring>

#include "gss_utils.hpp"

#include "gss_auth_mechanism.hpp"


namespace gss_utils {

/*
 *  Mechanism Name          Object Identifier       Shared Library  Kernel Module
 *  diffie_hellman_640_0    1.3.6.4.1.42.2.26.2.4   dh640-0.so.1
 *  diffie_hellman_1024_0   1.3.6.4.1.42.2.26.2.5   dh1024-0.so.1
 *  SPNEGO                  1.3.6.1.5.5.2
 *  iakerb                  1.3.6.1.5.2.5
 *  SCRAM-SHA-1             1.3.6.1.5.5.14
 *  SCRAM-SHA-256           1.3.6.1.5.5.18
 *  GSS-EAP (arc)           1.3.6.1.5.5.15.1.1.*
 *  kerberos_v5             1.2.840.113554.1.2.2    gl/mech_krb5.so gl_kmech_krb5
 *
 * There are two different formats:
 *   The first, "{ 1 2 3 4 }", is officially mandated by the GSS-API specs.
 *   gss_str_to_oid() expects this first format.
 *
 *   The second, "1.2.3.4", is more widely used but is not an official
 *   standard format.
 */
std::string transform_gss_oid(const std::string& oid_to_check) {
  if (!oid_to_check.empty()) {
    std::string new_gss_oid(common_utils::str_trim(oid_to_check));
    std::replace(std::begin(new_gss_oid),
                 std::end(new_gss_oid),
                 common_utils::DOT,
                 common_utils::BLANK);

    if (!new_gss_oid.empty() &&
        std::all_of(std::begin(new_gss_oid),
                    std::end(new_gss_oid),
                    common_utils::is_digit_or_blank)) {
      if (!std::none_of(std::begin(new_gss_oid),
                        std::end(new_gss_oid),
                        common_utils::is_open_or_close)) {
        new_gss_oid.insert(std::begin(new_gss_oid),
                           common_utils::OPEN_OID);
        new_gss_oid.insert(std::end(new_gss_oid),
                           common_utils::CLOSE_OID);
      }
      return (new_gss_oid);
    }
  }
  return {};
}


/*  GSSExceptionHandler Implementation.
*/
GSSExceptionHandler::
GSSExceptionHandler(OM_uint32 gss_major_status,
                    OM_uint32 gss_minor_status,
                    const char* gss_func) throw() :
    m_gss_major_status(gss_major_status),
    m_gss_minor_status(gss_minor_status)
{
  std::strncpy(m_gss_func, gss_func, (sizeof(m_gss_func) - 1));
  m_gss_func[(sizeof(m_gss_func) - 1)] = 0;

  try {
    show_msg_helper(gss_major_status, GSS_C_GSS_CODE,
                    m_gss_major_msg, sizeof(m_gss_major_msg));
    show_msg_helper(gss_minor_status, GSS_C_MECH_CODE,
                    m_gss_minor_msg, sizeof(m_gss_minor_msg));
  }
  catch (GSSExceptionHandler& except) {
    std::strcpy(m_gss_major_msg, except.m_gss_major_msg);
    std::strcpy(m_gss_minor_msg, except.m_gss_minor_msg);
    std::strcpy(m_gss_func, except.m_gss_func);
  }
}

const char* GSSExceptionHandler::what() const throw()
{
  if (m_gss_minor_status) {
    return m_gss_minor_msg;
  }
  return m_gss_major_msg;
}

void show_msg_helper(OM_uint32 gss_msg_code,
                     int gss_msg_type,
                     char* gss_msg,
                     int gss_size)
{
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);
  OM_uint32 msg_ctx(0);
  auto position_tmp(0);

  do {
    gss_client_auth::GSSDataBuffer gss_buff;

    gss_major_status = gss_display_status(&gss_minor_status,
                                          gss_msg_code,
                                          gss_msg_type,
                                          GSS_C_NO_OID,
                                          &msg_ctx,
                                          gss_buff);

    if (gss_major_status != GSS_S_COMPLETE) {
      throw GSSExceptionHandler(gss_major_status,
                                gss_minor_status,
                                "ERROR: gss_display_status() failed!");
    }

    std::strncpy((gss_msg + position_tmp),
                 gss_buff.get_bytes(),
                 static_cast<size_t>((gss_size - position_tmp) - 1));
    position_tmp += gss_buff.gss_buff_getsize();
  } while (msg_ctx && position_tmp < gss_size);
  gss_msg[(gss_size - 1)] = 0;
}

}   //-- namespace gss_utils

// ----------------------------- END-OF-FILE --------------------------------//
