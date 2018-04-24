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

#ifndef LDAP_CLIENT_HPP
#define LDAP_CLIENT_HPP

#ifndef __cplusplus
  #error "C++ Compiler is required for this library <ldap_client.hpp>"
#endif

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

#include <string>

#include "auth_mechanism.hpp"
#include "auth_options.hpp"
#include "ldap_auth_mechanism.hpp"


namespace ldap_client_auth {

class LDAPClientAuthentication : public LDAPMechanismBase
{

  public:
    LDAPClientAuthentication() = default;
    LDAPClientAuthentication(const std::string&,
                             const ldap_utils::LDAPSSLOption&);
    ~LDAPClientAuthentication() = default;

  private:

  protected:

};  //-- class LDAPClientAuthentication

}   //-- namespace ldap_client_auth

#endif    //-- LDAP_CLIENT_HPP

// ----------------------------- END-OF-FILE --------------------------------//
