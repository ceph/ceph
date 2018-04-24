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

#include "auth_mechanism.hpp"


namespace auth_mechanisms {

AuthMechanism::AuthMechanism(const AuthOptions& auth_options) :
    m_auth_options(auth_options) {
}

AuthMechanism::~AuthMechanism() {}

AuthenticationStatus AuthMechanism::get_auth_status() const {
  return AuthenticationStatus::NONE;
}

bool AuthMechanism::compare_auth_type(const std::string& auth_type) const {
  switch(m_auth_options.m_auth_exclusive_method) {
    case(AuthExclusiveOptions::NONE):
      break;
    case(AuthExclusiveOptions::LDAP):
      break;
    case(AuthExclusiveOptions::GSSAPI):
      break;
    case(AuthExclusiveOptions::ALL):
      break;
  }
  return false;
}


}   //-- namespace auth_mechanisms

// ----------------------------- END-OF-FILE --------------------------------//
