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

#ifndef AUTH_OPTIONS_HPP
#define AUTH_OPTIONS_HPP

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


namespace auth_mechanisms {

// Possible/supported methods
enum class AuthenticationMethods {
  NONE,
  LDAP,
  GSSAPI,
  LDAP_LDAP,
  GSSAPI_GSSAPI,
  LDAP_GSSAPI,
};

enum class AuthExclusiveOptions {
  NONE    = 0x00,
  LDAP    = 0x01,
  GSSAPI  = 0x02,
  ALL     = 0x04,
};


class AuthOptions {

  public:
    AuthOptions();
    explicit AuthOptions(const AuthenticationMethods&);
    //
    bool m_ipv6{false};
    bool m_gss_plaintext{true};
    bool m_ldap_plaintext{true};
    int m_socket_id{0};
    int m_sending_mark{0};
    int m_receiving_mark{0};
    AuthenticationMethods m_auth_method{AuthenticationMethods::NONE};
    AuthExclusiveOptions m_auth_exclusive_method{AuthExclusiveOptions::NONE};
    std::string m_auth_username{};
    std::string m_auth_password{};
    std::string m_ldap_search_base{};
    std::string m_ldap_server_list{};
    std::string m_ldap_uri{};
    std::string m_gss_principal{};
    std::string m_gss_service_principal{};

  private:

  protected:

};


}   //-- namespace auth_mechanisms

#endif    //-- AUTH_OPTIONS_HPP

// ----------------------------- END-OF-FILE --------------------------------//
