// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <coroutine>
#include <cstdint>
#include <utility>

#include <fmt/format.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/system/errc.hpp>

#include "include/neorados/RADOS.hpp"

#include "rgw/driver/rados/rgw_neorados.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace asio = boost::asio;
namespace sys = boost::system;
namespace buffer = ceph::buffer;

using rgw::neorados::write_obj;
using rgw::neorados::read_obj;
using rgw::neorados::VersionTracker;

using namespace std::literals;

struct EndyCodable {
  std::uint64_t a = 0;
  std::vector<std::uint32_t> b;

  void encode(buffer::list& bl) const {
    using ceph::encode;
    encode(a, bl);
    encode(b, bl);
  }

  void decode(buffer::list::const_iterator& bi) {
    using ceph::decode;
    decode(a, bi);
    decode(b, bi);
  }

  auto operator <=>(const EndyCodable&) const = default;
};
WRITE_CLASS_ENCODER(EndyCodable);

static const EndyCodable ref{ .a = 64, .b{1, 2, 3, 4, 5} };
static const EndyCodable ref2{ .a = 17, .b{5, 4, 3, 2, 1} };

CORO_TEST_F(RgwNeorados, RoundTrip, NeoRadosTest) {
  static constexpr auto obj = "obj";
  co_await write_obj(dpp(), rados(), obj, pool(), ref);
  auto res = co_await read_obj<EndyCodable>(dpp(), rados(), obj, pool());
  EXPECT_EQ(ref, res);
  co_return;
}

CORO_TEST_F(RgwNeorados, EmptyOnENOENT, NeoRadosTest) {
  {
    static constexpr auto obj = "obj";
    auto res = co_await read_obj<EndyCodable>(dpp(), rados(), obj, pool(),
                                              true);
    EXPECT_EQ(EndyCodable{}, res);
  }

  {
    rgw_pool otherpool;
    otherpool.name = get_temp_pool_name();
    rgw_raw_obj obj(otherpool, "oid");
    auto res = co_await read_obj<EndyCodable>(dpp(), rados(), std::move(obj),
                                              true);
    EXPECT_EQ(EndyCodable{}, res);
  }

  co_return;
}

CORO_TEST_F(RgwNeorados, Exclusive, NeoRadosTest) {
  static constexpr auto obj = "obj";
  co_await write_obj(dpp(), rados(), obj, pool(), ref, nullptr, true);
  co_await expect_error_code(
    write_obj(dpp(), rados(), obj, pool(), ref2, nullptr, true),
    sys::errc::file_exists);
  auto res = co_await read_obj<EndyCodable>(dpp(), rados(), obj, pool());
  EXPECT_EQ(ref, res);
  co_return;
}

CORO_TEST_F(RgwNeorados, OtherThanExclusive, NeoRadosTest) {
  static constexpr auto obj = "obj";
  co_await write_obj(dpp(), rados(), obj, pool(), ref);
  co_await write_obj(dpp(), rados(), obj, pool(), ref2);
  auto res = co_await read_obj<EndyCodable>(dpp(), rados(), obj, pool());
  EXPECT_EQ(ref2, res);
  co_return;
}

CORO_TEST_F(RgwNeorados, Version, NeoRadosTest) {
  static constexpr auto obj = "obj";
  VersionTracker v;
  v.new_write_version(rados().cct());
  co_await write_obj(dpp(), rados(), obj, pool(), ref, &v);
  auto vold = v;
  co_await write_obj(dpp(), rados(), obj, pool(), ref2, &v);
  co_await expect_error_code(write_obj(dpp(), rados(), obj, pool(), ref, &vold),
                             sys::errc::operation_canceled);
  co_await expect_error_code(
    read_obj<EndyCodable>(dpp(), rados(), obj, pool(), false, &vold),
    sys::errc::operation_canceled);

  co_return;
}

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
