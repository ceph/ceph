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

#ifndef GSS_UTILS_HPP
#define GSS_UTILS_HPP

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

#include <gssapi.h>
#include <gssapi/gssapi_generic.h>
#include <gssapi/gssapi_krb5.h>
#include <gssapi/gssapi_ext.h>

#include <string>

#include "common_utils.hpp"


static const gss_OID_desc GSS_API_KRB5_OID_PTR = 
    { 9, (void *)"\052\206\110\206\367\022\001\002\002" };
static const gss_OID_desc GSS_API_SPNEGO_OID_PTR =
    {6, (void *)"\x2b\x06\x01\x05\x05\x02"};

namespace gss_utils {

/// Common GSS constants used.
static constexpr int32_t GSS_AUTH_OK(0);
static constexpr int32_t GSS_AUTH_FAILED(-1);
static constexpr uint32_t GSS_MAX_BUFF_MSG_SIZE(128);
static constexpr uint32_t GSS_MAX_FUNC_SIZE(64);
static constexpr u_short KRB_DEFAULT_PORT_NUM(88);
static const std::string KRB_DEFAULT_PORT_STR(std::to_string(KRB_DEFAULT_PORT_NUM));
static const std::string KRB_SERVICE_NAME("kerberos");
static const std::string GSS_API_SPNEGO_OID("{1.3.6.1.5.5.2}");
static const std::string GSS_API_KRB5_OID("{1.2.840.113554.1.2.2}");
static const std::string GSS_TARGET_DEFAULT_NAME("ceph"); 

///
std::string transform_gss_oid(const std::string&);

void show_msg_helper(OM_uint32, int, char*, int);

class GSSExceptionHandler : public std::exception
{
  public:
    GSSExceptionHandler() throw() : m_gss_major_status(GSS_S_COMPLETE),
                                    m_gss_minor_status(GSS_S_COMPLETE) { }
    GSSExceptionHandler(OM_uint32, OM_uint32,
                        const char* =
                          common_utils::EMPTY_STR.c_str()) throw();

    ~GSSExceptionHandler() throw() override = default;
    const char* what() const throw() override;

    OM_uint32 m_gss_major_status;
    OM_uint32 m_gss_minor_status;
    char m_gss_major_msg[GSS_MAX_BUFF_MSG_SIZE]{};
    char m_gss_minor_msg[GSS_MAX_BUFF_MSG_SIZE]{};
    char m_gss_func[GSS_MAX_FUNC_SIZE]{};
};

}   //-- namespace gss_utils

#endif    //-- GSS_UTILS_HPP

// ----------------------------- END-OF-FILE --------------------------------//

