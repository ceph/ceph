// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/spawn.hpp>

#include <boost/system/system_error.hpp>

#include <gtest/gtest.h>

#include "common/async/context_pool.h"
#include "common/async/yield_context.h"

#include "common/dout.h"

#include "rgw/rgw_asio_thread.h"
#include "rgw/yield_completion.h"

namespace asio = boost::asio;
namespace sys = boost::system;

static auto cct = std::make_unique<CephContext>(CEPH_ENTITY_TYPE_ANY);
static NoDoutPrefix dp(cct.get(), ceph_subsys_rgw);

static asio::awaitable<void>
maybethrow(int code)
{
  if (code != 0) {
    throw sys::system_error{code, sys::generic_category()};
  }
  co_return;
}

TEST(HybridToken, HybridToken)
{
  ceph::async::io_context_pool io_context{3, [] { is_asio_thread = true; }};
  bool ran = false;
  asio::spawn(
      io_context,
      [&](asio::yield_context yc) {
        optional_yield y{yc};

        ASSERT_NO_THROW(
            asio::co_spawn(yc.get_executor(), maybethrow(0), rgw::maybe_yield(&dp, y)));
        ASSERT_NO_THROW(co_spawn(
            yc.get_executor(), maybethrow(0), rgw::maybe_yield(&dp, null_yield)));

        ASSERT_THROW(
            asio::co_spawn(yc.get_executor(), maybethrow(ENOENT), rgw::maybe_yield(&dp, y)),
            sys::system_error);
        ASSERT_THROW(
            asio::co_spawn(
                yc.get_executor(), maybethrow(ENOENT),
                rgw::maybe_yield(&dp, null_yield)),
            sys::system_error);

        {
          std::exception_ptr eptr;
          ASSERT_NO_THROW(co_spawn(
              yc.get_executor(), maybethrow(ENOENT), rgw::maybe_yield(&dp, y, eptr)));
          ASSERT_EQ(-ENOENT, ceph::from_exception(eptr));
        }
        {
          std::exception_ptr eptr;
          ASSERT_NO_THROW(co_spawn(
              yc.get_executor(), maybethrow(ENOENT), rgw::maybe_yield(&dp, null_yield, eptr)));
          ASSERT_EQ(-ENOENT, ceph::from_exception(eptr));
        }

        ran = true;
      },
      rgw::maybe_yield(&dp, null_yield));
  ASSERT_TRUE(ran);
}
