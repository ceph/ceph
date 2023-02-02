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

#include "neorados/cls/version.h"

#include <coroutine>
#include <memory>
#include <string_view>
#include <utility>

#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/redirect_error.hpp>

#include <boost/system/errc.hpp>
#include <boost/system/error_code.hpp>

#include "include/neorados/RADOS.hpp"

#include "cls/version/cls_version_types.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace asio = boost::asio;
namespace sys = boost::system;
namespace buffer = ceph::buffer;


CORO_TEST_F(neocls_handler_error, test_handler_error, NeoRadosTest)
{
  std::string_view oid = "obj";
  co_await create_obj(rados, oid, pool, asio::use_awaitable);

  neorados::ReadOp op;
  op.exec("version", "read", {},
          [](sys::error_code ec, const buffer::list& bl) {
            throw buffer::end_of_buffer{};
          });
  sys::error_code ec;
  co_await rados.execute(oid, pool, std::move(op), nullptr,
                         asio::redirect_error(asio::use_awaitable, ec));
  EXPECT_EQ(buffer::errc::end_of_buffer, ec);

  op = neorados::ReadOp{};
  op.exec("version", "read", {},
          [](sys::error_code ec, const buffer::list& bl) {
            throw std::exception();
          });
  co_await rados.execute(oid, pool, std::move(op), nullptr,
                         asio::redirect_error(asio::use_awaitable, ec));
  EXPECT_EQ(sys::errc::io_error, ec);

  co_return;
}
