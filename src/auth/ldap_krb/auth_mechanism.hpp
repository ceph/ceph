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

#ifndef AUTH_MECHANISM_HPP
#define AUTH_MECHANISM_HPP

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

#include "auth_options.hpp"


namespace auth_mechanisms {

//Possible states for the authentication
enum class AuthenticationStatus {
  NONE,
  HANDSHAKING,
  READY,
  ERROR,
};

class AuthMechanism {

  public:
    AuthMechanism() = default;
    explicit AuthMechanism(const AuthOptions&);
    virtual ~AuthMechanism() = 0;

    // Prepare and process command to be sent/received
    ////virtual int next_handshake_cmd(std::string&) = 0;
    ////virtual int process_handshake_cmd(std::string&) = 0;
    ////virtual int do_encode(std::string&) { return 0; }
    ////virtual int do_decode(std::string&) { return 0; }

    // Gets the status of the mechanism
    virtual AuthenticationStatus get_auth_status() const = 0;

  private:
    bool compare_auth_type(const std::string&) const;

  protected:
    AuthOptions m_auth_options;


};  //-- class AuthMechanism


}   //-- namespace auth_mechanisms

#endif    //-- AUTH_MECHANISM_HPP

// ----------------------------- END-OF-FILE --------------------------------//
