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


asio::awaitable<uint64_t> NeoRadosECTest::create_pool() {
  // Workaround for https://gcc.gnu.org/bugzilla/show_bug.cgi?id=110913
  std::vector<std::string> profile_set = {
    fmt::format(
      R"({{"prefix": "osd erasure-code-profile set", "name": "testprofile-{}", )"
      R"( "profile": [ "k=2", "m=1", "crush-failure-domain=osd"]}})",
      pool_name())
  };
  co_await rados().mon_command(std::move(profile_set), {}, nullptr, nullptr,
			       asio::use_awaitable);
  std::vector<std::string> pool_create = {
    fmt::format(
      R"({{"prefix": "osd pool create", "pool": "{}", "pool_type":"erasure", )"
      R"("pg_num":8, "pgp_num":8, "erasure_code_profile":"testprofile-{}"}})",
      pool_name(), pool_name())
  };
  auto c = rados().mon_command(std::move(pool_create), {}, nullptr, nullptr,
			       asio::use_awaitable);
  co_await std::move(c);

  co_return co_await rados().lookup_pool(pool_name(), asio::use_awaitable);
}

asio::awaitable<void> NeoRadosECTest::clean_pool() {
  co_await rados().delete_pool(pool().get_pool(), asio::use_awaitable);
  std::vector<std::string> profile_rm = {
    fmt::format(
      R"({{"prefix": "osd erasure-code-profile rm", "name": "testprofile-{}"}})",
      pool_name())
  };
  co_await rados().mon_command(std::move(profile_rm), {}, nullptr, nullptr, asio::use_awaitable);
  std::vector<std::string> rule_rm = {
    fmt::format(
      R"({{"prefix": "osd crush rule rm", "name":"{}"}})",
      pool_name())
  };
  co_await rados().mon_command(std::move(rule_rm), {}, nullptr, nullptr,
			       asio::use_awaitable);
  co_return;
}
