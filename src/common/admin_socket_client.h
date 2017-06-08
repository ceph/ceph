// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_ADMIN_SOCKET_CLIENT_H
#define CEPH_COMMON_ADMIN_SOCKET_CLIENT_H

#include <stdint.h>
#include <string>

/* This is a simple client that talks to an AdminSocket using blocking I/O.
 * We put a 5-second timeout on send and recv operations.
 */
class AdminSocketClient
{
public:
  AdminSocketClient(const std::string &path);
  std::string do_request(std::string request, std::string *result);
  std::string ping(bool *ok);
private:
  std::string m_path;
};

const char* get_rand_socket_path();

#endif
