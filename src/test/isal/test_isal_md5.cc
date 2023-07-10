// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License version 2.1, as published by
 * the Free Software Foundation.  See file COPYING.
 *
 */

//#define BOOST_ASIO_ENABLE_HANDLER_TRACKING

#include "async_md5.h"
#include <optional>
#include <boost/asio.hpp>
#include <gtest/gtest.h>

using boost::system::error_code;

auto capture(std::optional<error_code>& e)
{
  return [&e] (error_code ec) { e = ec; };
}

TEST(isal_md5, flush)
{
  boost::asio::io_context ctx;
  const auto timeout = std::chrono::nanoseconds(0);
  auto mgr = ceph::async_md5::Batch{ctx.get_executor(), timeout};

  ceph::async_md5::Digest digest1;
  ceph::async_md5::Digest digest2;
  ceph::async_md5::Digest digest3;

  constexpr std::string_view buffer1 = "aaaaaaaa";
  constexpr std::string_view buffer2 = "bbbbbbbb";
  constexpr std::string_view buffer3 = "cccccccc";

  std::optional<error_code> ec1;
  std::optional<error_code> ec2;
  std::optional<error_code> ec3;

  mgr.async_hash(digest1, buffer1, true, capture(ec1));
  mgr.async_hash(digest2, buffer2, true, capture(ec2));
  mgr.async_hash(digest3, buffer3, true, capture(ec3));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped()); // expect outstanding work

  EXPECT_FALSE(ec1);
  EXPECT_FALSE(ec2);
  EXPECT_FALSE(ec3);

  mgr.async_flush(boost::asio::detached);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped()); // expect no work

  ASSERT_TRUE(ec1);
  EXPECT_FALSE(*ec1);
  // $ echo -n 'aaaaaaaa' | md5sum
  EXPECT_EQ("3dbe00a167653a1aaee01d93e77e730e", digest1.as_hex());
  ASSERT_TRUE(ec2);
  EXPECT_FALSE(*ec2);
  EXPECT_EQ("810247419084c82d03809fc886fedaad", digest2.as_hex());
  ASSERT_TRUE(ec3);
  EXPECT_FALSE(*ec3);
  EXPECT_EQ("bee397acc400449ea3a35ed3fc87fea1", digest3.as_hex());
}

TEST(isal_md5, timeout)
{
  boost::asio::io_context ctx;
  // specify a batch_timeout and verify that async_hash() eventually completes
  // without a full batch or manual flush
  const auto timeout = std::chrono::milliseconds(10);
  auto mgr = ceph::async_md5::Batch{ctx.get_executor(), timeout};

  constexpr std::string_view buffer = "aaaaaaaa";
  ceph::async_md5::Digest digest;
  std::optional<error_code> ec;
  mgr.async_hash(digest, buffer, true, capture(ec));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());

  EXPECT_FALSE(ec); // does not complete immediately

  ctx.run_one(); // wait for timer
  ctx.poll();
  ASSERT_TRUE(ctx.stopped());

  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  EXPECT_EQ("3dbe00a167653a1aaee01d93e77e730e", digest.as_hex());
}

TEST(isal_md5, batch)
{
  boost::asio::io_context ctx;
  const auto timeout = std::chrono::nanoseconds(0);
  auto mgr = ceph::async_md5::Batch{ctx.get_executor(), timeout};

  constexpr std::string_view buffer = "aaaaaaaa";

  // issue more than enough requests to fill a batch
  constexpr size_t digest_count = MD5_MAX_LANES + 1;

  std::array<ceph::async_md5::Digest, digest_count> digests;
  std::array<std::optional<error_code>, digest_count> ecs;
  for (size_t i = 0; i < digest_count; i++) {
    mgr.async_hash(digests[i], buffer, true, capture(ecs[i]));
  }

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());

  // verify that at least one batch completed
  for (size_t i = 0; i < MD5_MIN_LANES; i++) {
    ASSERT_TRUE(ecs[i]);
    EXPECT_FALSE(*ecs[i]);
    EXPECT_EQ("3dbe00a167653a1aaee01d93e77e730e", digests[i].as_hex());
  }

  // flush the rest
  mgr.async_flush(boost::asio::detached);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());

  for (size_t i = MD5_MIN_LANES; i < digest_count; i++) {
    ASSERT_TRUE(ecs[i]);
    EXPECT_FALSE(*ecs[i]);
    EXPECT_EQ("3dbe00a167653a1aaee01d93e77e730e", digests[i].as_hex());
  }
}

TEST(isal_md5, batch_cross_executor)
{
  // run the Batch on a 'server ctx'
  boost::asio::io_context sctx;
  const auto timeout = std::chrono::nanoseconds(0);
  auto mgr = ceph::async_md5::Batch{sctx.get_executor(), timeout};

  // use a 'client ctx' for the completions
  boost::asio::io_context cctx;
  auto cex = boost::asio::make_strand(cctx);

  constexpr std::string_view buffer = "aaaaaaaa";

  // issue many batches
  constexpr size_t digest_count = 64 * MD5_MAX_LANES + 1;

  std::array<ceph::async_md5::Digest, digest_count> digests;
  std::array<std::optional<error_code>, digest_count> ecs;
  for (size_t i = 0; i < digest_count; i++) {
    mgr.async_hash(digests[i], buffer, true,
                   boost::asio::bind_executor(cex, capture(ecs[i])));
  }

  sctx.poll();
  ASSERT_FALSE(sctx.stopped());

  // flush the rest
  mgr.async_flush(boost::asio::detached);

  sctx.poll();
  ASSERT_TRUE(sctx.stopped());

  // drain the completions
  cctx.poll();
  ASSERT_TRUE(cctx.stopped());

  for (size_t i = 0; i < digest_count; i++) {
    ASSERT_TRUE(ecs[i]);
    EXPECT_FALSE(*ecs[i]);
    EXPECT_EQ("3dbe00a167653a1aaee01d93e77e730e", digests[i].as_hex());
  }
}

auto do_request_hash(ceph::async_md5::Batch& mgr,
                     std::string_view buffer)
  -> boost::asio::awaitable<std::string>
{
  ceph::async_md5::Digest digest;
  // process the buffer one byte at a time
  for (char c : buffer) {
    co_await mgr.async_hash(digest, {&c, 1}, false,
                            boost::asio::use_awaitable);
  }
  // finalize the digest
  co_await mgr.async_hash(digest, "", true,
                          boost::asio::use_awaitable);
  co_return digest.as_hex();
}

TEST(isal_md5, batch_thread_pool)
{
  boost::asio::thread_pool ctx{16};

  // run the Batch on a strand executor
  const auto timeout = std::chrono::milliseconds(10);
  auto strand = boost::asio::make_strand(ctx);
  auto mgr = ceph::async_md5::Batch{strand, timeout};

  constexpr std::string_view buffer = "aaaaaaaa";

  // run enough concurrent requests to fill at least one batch
  constexpr size_t request_count = MD5_MAX_LANES;
  std::array<std::string, request_count> hex_digests;

  // spawn a coroutine for each request
  for (auto& out : hex_digests) {
    boost::asio::co_spawn(ctx, do_request_hash(mgr, buffer),
        [&out] (std::exception_ptr eptr, std::string&& result) mutable {
          if (eptr) {
            std::rethrow_exception(eptr);
          } else {
            out = std::move(result);
          }
        });
  }

  // drain the completions
  ctx.join();

  for (const auto& hex : hex_digests) {
    EXPECT_EQ("3dbe00a167653a1aaee01d93e77e730e", hex);
  }
}

TEST(isal_md5, multi_buffer)
{
  boost::asio::io_context ctx;
  const auto timeout = std::chrono::nanoseconds(0);
  auto mgr = ceph::async_md5::Batch{ctx.get_executor(), timeout};

  // calculate the hash digest over a sequence of buffers
  constexpr std::string_view buffers[] = {"aaa", "aaa", "aa"};
  ceph::async_md5::Digest digest;
  std::optional<error_code> ec;

  mgr.async_hash(digest, buffers[0], false, capture(ec));
  mgr.async_flush(boost::asio::detached);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());

  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  ec = std::nullopt;

  mgr.async_hash(digest, buffers[1], false, capture(ec));
  mgr.async_flush(boost::asio::detached);

  ctx.restart();
  ctx.poll();
  ASSERT_TRUE(ctx.stopped());

  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  ec = std::nullopt;

  mgr.async_hash(digest, buffers[2], true, capture(ec));
  mgr.async_flush(boost::asio::detached);

  ctx.restart();
  ctx.poll();
  ASSERT_TRUE(ctx.stopped());

  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  EXPECT_EQ("3dbe00a167653a1aaee01d93e77e730e", digest.as_hex());
}

TEST(isal_md5, empty_last_buffer)
{
  boost::asio::io_context ctx;
  const auto timeout = std::chrono::nanoseconds(0);
  auto mgr = ceph::async_md5::Batch{ctx.get_executor(), timeout};

  constexpr std::string_view buffer = "aaaaaaaa";
  ceph::async_md5::Digest digest;
  std::optional<error_code> ec;

  mgr.async_hash(digest, buffer, false, capture(ec));
  mgr.async_flush(boost::asio::detached);

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());

  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  ec = std::nullopt;

  mgr.async_hash(digest, std::string_view{}, true, capture(ec));
  mgr.async_flush(boost::asio::detached);

  ctx.restart();
  ctx.poll();
  ASSERT_TRUE(ctx.stopped());

  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  EXPECT_EQ("3dbe00a167653a1aaee01d93e77e730e", digest.as_hex());
}
