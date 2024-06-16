// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * See file COPYING for license information.
 *
 */

#include <array>
#include <coroutine>
#include <memory>
#include <string_view>
#include <utility>

#include <boost/asio/use_awaitable.hpp>

#include <boost/system/errc.hpp>

#include "include/neorados/RADOS.hpp"

#include "common/ceph_json.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace asio = boost::asio;
namespace sys = boost::system;

using namespace std::literals;

using neorados::ReadOp;
using neorados::WriteOp;

CORO_TEST_F(NeoRadosCls, DNE, NeoRadosTest)
{
  std::string_view oid = "obj";
  co_await execute(oid, WriteOp{}.create(true));
  // Call a bogus class
  co_await expect_error_code(
    execute(oid, ReadOp{}.exec("doesnotexistasdfasdf", "method", {})),
    sys::errc::operation_not_supported);

  // Call a bogus method on an existent class
  co_await expect_error_code(
    execute(oid, ReadOp{}.exec("lock", "doesnotexistasdfasdfasdf", {})),
    sys::errc::operation_not_supported);
  co_return;
}
