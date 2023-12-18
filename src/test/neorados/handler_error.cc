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
  co_await create_obj(oid);

  {
    neorados::ReadOp op;
    op.exec("version", "read", {},
	    [](sys::error_code ec, const buffer::list& bl) {
	      throw buffer::end_of_buffer{};
	    });
    co_await expect_error_code(rados().execute(oid, pool(), std::move(op),
					       nullptr, asio::use_awaitable),
			       buffer::errc::end_of_buffer);
  }

  {
    neorados::ReadOp op;
    op.exec("version", "read", {},
	    [](sys::error_code ec, const buffer::list& bl) {
	      throw std::exception();
	    });
    co_await expect_error_code(rados().execute(oid, pool(), std::move(op),
					       nullptr, asio::use_awaitable),
			       sys::errc::io_error);
  }
  co_return;
}
