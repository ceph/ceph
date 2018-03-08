// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "common/async/throttle.h"
#include <optional>
#include <gtest/gtest.h>
#include "global/global_context.h"

namespace ceph::async {

// return a lambda that can be used as a callback to capture its error code
auto capture(std::optional<boost::system::error_code>& opt_ec)
{
  return [&] (boost::system::error_code ec) { opt_ec = ec; };
}

TEST(AsyncThrottle, AsyncGet)
{
  boost::asio::io_context context;
  Throttle throttle(context.get_executor(), 1);

  std::optional<boost::system::error_code> ec1, ec2, ec3;
  throttle.async_get(1, capture(ec1));
  throttle.async_get(1, capture(ec2));
  throttle.async_get(0, capture(ec3));

  EXPECT_FALSE(ec1); // no callbacks until poll()
  EXPECT_FALSE(ec2);
  EXPECT_FALSE(ec3);

  ASSERT_NO_THROW(context.poll());
  EXPECT_FALSE(context.stopped());

  ASSERT_TRUE(ec1); // only the first callback
  EXPECT_EQ(boost::system::errc::success, *ec1);
  EXPECT_FALSE(ec2);
  EXPECT_FALSE(ec3); // third request for 0 waits behind second

  throttle.put(1); // unblock the second and third requests
  EXPECT_FALSE(ec2); // no callbacks until poll()
  EXPECT_FALSE(ec3);

  ASSERT_NO_THROW(context.poll());
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::system::errc::success, *ec2);
  ASSERT_TRUE(ec3);
  EXPECT_EQ(boost::system::errc::success, *ec3);
}

TEST(AsyncThrottle, AsyncGetMany)
{
  // enqueue enough throttle requests that we'd crash if it tried to execute
  // them all recursively, i.e. async_get() -> put() -> async_get() -> put()...
  // (this is why Throttle::put() uses post() instead of dispatch() for the
  // async_get() completions)
  constexpr size_t total = 10000;
  constexpr size_t max_concurrent = 10;
  size_t processed = 0;

  boost::asio::io_context context;
  Throttle throttle(context.get_executor(), max_concurrent);

  for (size_t i = 0; i < total; i++) {
    throttle.async_get(1, [&] (boost::system::error_code ec) {
        ASSERT_EQ(boost::system::errc::success, ec);
        processed++;
        throttle.put(1);
      });
  }

  ASSERT_NO_THROW(context.poll());
  EXPECT_TRUE(context.stopped());

  EXPECT_EQ(total, processed);
}

TEST(AsyncThrottle, AsyncSetMaximum)
{
  boost::asio::io_context context;
  Throttle throttle(context.get_executor(), 0);

  std::optional<boost::system::error_code> ec1, ec2, ec3;
  throttle.async_get(1, capture(ec1));
  throttle.async_get(1, capture(ec2));
  throttle.async_get(1, capture(ec3));

  std::optional<boost::system::error_code> ecmax1;
  throttle.async_set_maximum(2, capture(ecmax1));

  // no callbacks before poll()
  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);
  EXPECT_FALSE(ec3);
  EXPECT_FALSE(ecmax1);

  ASSERT_NO_THROW(context.poll());
  EXPECT_FALSE(context.stopped());

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::system::errc::success, *ec1);
  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::system::errc::success, *ec2);
  EXPECT_FALSE(ec3);
  ASSERT_TRUE(ecmax1);
  EXPECT_EQ(boost::system::errc::success, *ecmax1);

  std::optional<boost::system::error_code> ecmax2;
  throttle.async_set_maximum(0, capture(ecmax2));
  EXPECT_FALSE(ecmax2);

  ASSERT_NO_THROW(context.poll());
  EXPECT_FALSE(context.stopped());

  EXPECT_FALSE(ecmax2);

  throttle.put(2);
  EXPECT_FALSE(ecmax2);

  ASSERT_NO_THROW(context.poll());
  EXPECT_FALSE(context.stopped());

  ASSERT_TRUE(ecmax2);
  EXPECT_EQ(boost::system::errc::success, *ecmax2);
  EXPECT_FALSE(ec3);
}

TEST(AsyncThrottle, Cancel)
{
  boost::asio::io_context context;
  Throttle throttle(context.get_executor(), 1);

  std::optional<boost::system::error_code> ec1, ec2, ec3;
  throttle.async_get(1, capture(ec1));
  throttle.async_get(1, capture(ec2));
  throttle.async_get(1, capture(ec3));

  EXPECT_FALSE(ec1); // no callbacks until poll()
  EXPECT_FALSE(ec2);
  EXPECT_FALSE(ec3);

  ASSERT_NO_THROW(context.poll());
  EXPECT_FALSE(context.stopped());

  ASSERT_TRUE(ec1); // only the first callback
  EXPECT_EQ(boost::system::errc::success, *ec1);
  EXPECT_FALSE(ec2);
  EXPECT_FALSE(ec3);

  std::optional<boost::system::error_code> ecmax;
  throttle.async_set_maximum(0, capture(ecmax));
  EXPECT_FALSE(ecmax);

  ASSERT_NO_THROW(context.poll());
  EXPECT_FALSE(context.stopped());

  EXPECT_FALSE(ecmax);

  throttle.cancel();
  EXPECT_FALSE(ec2); // no callbacks until poll()
  EXPECT_FALSE(ec3);

  ASSERT_NO_THROW(context.poll());
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::system::errc::operation_canceled, *ec2);
  ASSERT_TRUE(ec3);
  EXPECT_EQ(boost::system::errc::operation_canceled, *ec3);
  ASSERT_TRUE(ecmax);
  EXPECT_EQ(boost::system::errc::operation_canceled, *ecmax);
}

TEST(AsyncThrottle, CancelOnDestruct)
{
  boost::asio::io_context context;
  std::optional<boost::system::error_code> ec;
  {
    Throttle throttle(context.get_executor(), 0);
    throttle.async_get(1, capture(ec));
  }
  EXPECT_FALSE(ec);

  ASSERT_NO_THROW(context.poll());
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec);
  EXPECT_EQ(boost::system::errc::operation_canceled, *ec);
}

// return a lambda from capture() that's bound to run on the given executor
template <typename Executor, typename ...Args>
auto bind_capture(const Executor& ex, Args& ...args)
{
  return boost::asio::bind_executor(ex, capture(args...));
}

TEST(AsyncThrottle, CrossExecutor)
{
  boost::asio::io_context throttle_context;
  Throttle throttle(throttle_context.get_executor(), 1);

  // create a separate execution context to use for all callbacks to test that
  // pending requests maintain executor work guards on both executors
  boost::asio::io_context callback_context;
  auto ex2 = callback_context.get_executor();

  std::optional<boost::system::error_code> ec1, ec2;
  throttle.async_get(1, bind_capture(ex2, ec1));
  throttle.async_get(1, bind_capture(ex2, ec2));

  ASSERT_NO_THROW(throttle_context.poll());
  EXPECT_FALSE(throttle_context.stopped()); // still has work

  EXPECT_FALSE(ec1);

  ASSERT_NO_THROW(callback_context.poll());
  EXPECT_FALSE(callback_context.stopped());

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::system::errc::success, *ec1);
  EXPECT_FALSE(ec2);

  throttle.cancel(); // cancel second request

  ASSERT_NO_THROW(throttle_context.poll());
  EXPECT_TRUE(throttle_context.stopped());

  EXPECT_FALSE(ec2);

  ASSERT_NO_THROW(callback_context.poll());
  EXPECT_TRUE(callback_context.stopped());

  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::system::errc::operation_canceled, *ec2);

  std::optional<boost::system::error_code> ecmax;
  throttle.async_set_maximum(0, bind_capture(ex2, ecmax));

  EXPECT_FALSE(ecmax);

  throttle_context.restart();
  ASSERT_NO_THROW(throttle_context.poll());
  EXPECT_FALSE(throttle_context.stopped());

  throttle.put(1);

  EXPECT_FALSE(ecmax);

  callback_context.restart();
  ASSERT_NO_THROW(callback_context.poll());
  EXPECT_TRUE(callback_context.stopped());

  ASSERT_TRUE(ecmax);
  EXPECT_EQ(boost::system::errc::success, *ecmax);
}

} // namespace ceph::async
