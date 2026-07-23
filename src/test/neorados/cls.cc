// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

namespace cls::bogus_classes {
struct ClassId {
  static constexpr auto name = "doesnotexistasdfasdf";
};
struct ClassIdLock { // This is a real class. We are adding a bogus method.
  static constexpr auto name = "doesnotexistasdfasdf";
};
namespace method {
constexpr auto bogus_class = ClsMethod<RdWrTag, ClassId>("method");
constexpr auto bogus_method = ClsMethod<RdWrTag, ClassIdLock>("doesnotexistasdfasdfasdf");
}
}

using namespace ::cls::bogus_classes;

CORO_TEST_F(NeoRadosCls, DNE, NeoRadosTest)
{
  std::string_view oid = "obj";
  co_await execute(oid, WriteOp{}.create(true));
  bufferlist bl1, bl2;
  // Call a bogus class
  co_await expect_error_code(
    execute(oid, WriteOp{}.exec(method::bogus_class, std::move(bl1))),
    sys::errc::operation_not_supported);

  // Call a bogus method on an existent class
  co_await expect_error_code(
    execute(oid, WriteOp{}.exec(method::bogus_method, std::move(bl2))),
    sys::errc::operation_not_supported);
  co_return;
}
