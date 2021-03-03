// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */


#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>

#include <gtest/gtest.h>

#include <spawn/spawn.hpp>

#include "common/async/bind_handler.h"
#include "common/async/forward_handler.h"

#include "common/async/expectify.h"

namespace ba = boost::asio;
namespace bs = boost::system;
namespace ca = ceph::async;
namespace s = spawn;

template<typename Executor, typename CompletionToken, typename... Args>
auto id(const Executor& executor, CompletionToken&& token,
	Args&& ...args)
{
  ba::async_completion<CompletionToken, void(Args...)> init(token);
  auto a = ba::get_associated_allocator(init.completion_handler);
  executor.post(ca::forward_handler(
		  ca::bind_handler(std::move(init.completion_handler),
				   std::forward<Args>(args)...)),
		a);
  return init.result.get();
}

TEST(Expectify, Void)
{
  ba::io_context c;
  s::spawn(c, [&](s::yield_context y) {
    ba::post(c.get_executor(), ca::expectify(y));
  });
  c.run();
}

TEST(Expectify, NoError)
{
  ba::io_context c;
  s::spawn(c, [&](s::yield_context y) {
    auto p = id(c.get_executor(), ca::expectify(y), bs::error_code{});
    EXPECT_TRUE(p);

    auto i = id(c.get_executor(), ca::expectify(y), bs::error_code{}, 5);
    ASSERT_EQ(5, *i);

    auto j = id(c.get_executor(), ca::expectify(y), 9, 3.5);
    ASSERT_EQ(9, std::get<0>(*j));
    ASSERT_EQ(3.5, std::get<1>(*j));
  });
  c.run();
}

TEST(Expectify, AnError)
{
  ba::io_context c;
  s::spawn(c, [&](s::yield_context y) {
    {
      auto p = id(c.get_executor(), ca::expectify(y),
                  bs::error_code{EDOM, bs::system_category()});
      EXPECT_EQ(bs::error_code(EDOM, bs::system_category()), p.error());
    }
    {
      auto p = id(c.get_executor(), ca::expectify(y),
                  bs::error_code{EDOM, bs::system_category()}, 5);
      EXPECT_EQ(bs::error_code(EDOM, bs::system_category()), p.error());
    }
  });
  c.run();
}
