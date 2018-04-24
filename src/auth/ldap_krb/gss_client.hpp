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

#ifndef GSS_CLIENT_HPP
#define GSS_CLIENT_HPP

#ifndef __cplusplus
  #error "C++ Compiler is required for this library <gss_client.hpp>"
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

#include <netdb.h>
#include <sys/socket.h>

#include <array>
#include <functional>
#include <iostream>
#include <iterator>
#include <locale>
#include <memory>
#include <tuple>
#include <utility>

#include "gss_auth_mechanism.hpp"
#include "gss_utils.hpp"


namespace gss_client_auth {

class GSSClientAuthentication : public GSSMechanismBase
{

  public:
    GSSClientAuthentication() = default;
    ~GSSClientAuthentication() = default;

  private:

  protected:

};    //-- class GSSClientAuthentication

}   //-- namespace gss_client_auth

#endif    //-- GSS_CLIENT_HPP

// ----------------------------- END-OF-FILE --------------------------------//
