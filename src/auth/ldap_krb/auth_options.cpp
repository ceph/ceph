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

#include "auth_options.hpp"


namespace auth_mechanisms {

AuthOptions::AuthOptions() {
  AuthOptions(AuthenticationMethods::NONE);
}

AuthOptions::AuthOptions(const AuthenticationMethods& auth_method) {

  AuthExclusiveOptions auth_exclusive_options = AuthExclusiveOptions::NONE;


  auto auth_chosen_options(0);

  switch (auth_method) {
    case AuthenticationMethods::NONE:
      m_auth_exclusive_method = (AuthExclusiveOptions::NONE);
      auth_chosen_options |= static_cast<int>(m_auth_exclusive_method);
      break;

    case AuthenticationMethods::LDAP:
    case AuthenticationMethods::LDAP_LDAP:
      m_auth_exclusive_method = (AuthExclusiveOptions::LDAP);
      auth_chosen_options |= static_cast<int>(m_auth_exclusive_method);
      break;

    case AuthenticationMethods::GSSAPI:
    case AuthenticationMethods::GSSAPI_GSSAPI:
      m_auth_exclusive_method = (AuthExclusiveOptions::GSSAPI);
      auth_chosen_options |= static_cast<int>(m_auth_exclusive_method);
      break;

    case AuthenticationMethods::LDAP_GSSAPI:
      m_auth_exclusive_method = (AuthExclusiveOptions::ALL);
      auth_chosen_options |= static_cast<int>(AuthExclusiveOptions::LDAP) |
                             static_cast<int>(AuthExclusiveOptions::GSSAPI);
      break;
  }

  if (auth_chosen_options & static_cast<int>(AuthExclusiveOptions::NONE)) {
    m_ipv6            = false;
    m_socket_id       = 0;
    m_sending_mark    = 0;
    m_receiving_mark  = 0;
  }
  if (auth_chosen_options & static_cast<int>(AuthExclusiveOptions::LDAP)) {
    m_ldap_plaintext    = true;
    m_ldap_search_base  = {};
    m_ldap_server_list  = {};
    m_auth_username     = {};
    m_auth_password     = {};
    m_auth_method       = auth_method;
  }
  if (auth_chosen_options & static_cast<int>(AuthExclusiveOptions::GSSAPI)) {
    m_gss_plaintext         = true;
    m_gss_principal         = {};
    m_gss_service_principal = {};
    m_auth_method           = auth_method;
  }

}

}   //-- namespace auth_mechanisms

// ----------------------------- END-OF-FILE --------------------------------//
