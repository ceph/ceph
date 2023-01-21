// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <string>
#include <string_view>

#include <boost/asio/ip/host_name.hpp>

#include <fmt/format.h>

#include "common_tests.h"
#include "include/neorados/RADOS.hpp"

namespace asio = boost::asio;

std::string get_temp_pool_name(std::string_view prefix)
{
  static auto hostname = asio::ip::host_name();
  static auto num = 1ull;
  return fmt::format("{}{}-{}-{}", prefix, hostname, getpid(), num++);
}
