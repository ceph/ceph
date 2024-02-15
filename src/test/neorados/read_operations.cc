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
#include <cstring>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string_view>
#include <utility>

#include <boost/asio/use_awaitable.hpp>

#include <boost/container/flat_map.hpp>

#include <boost/system/error_code.hpp>
#include <boost/system/errc.hpp>

#include <xxHash/xxhash.h>

#include "include/neorados/RADOS.hpp"

#include "osd/error_code.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace asio = boost::asio;
namespace ctnr = boost::container;
namespace hash_alg = neorados::hash_alg;
namespace sys = boost::system;

using namespace std::literals;

using neorados::cmp_op;
using neorados::ReadOp;
using neorados::WriteOp;

class ReadOpTest : public NeoRadosTest {
protected:
  static constexpr auto oid = "testobj"sv;
  static constexpr auto data = "testdata"sv;
  static constexpr std::size_t datalen = 16;

  auto write_object(std::string_view data, uint64_t* objver = nullptr) {
    return execute(oid, WriteOp{}.write_full(to_buffer_list(data)), objver);
  }

  auto remove_object() {
    return execute(oid, WriteOp{}.remove());
  }

  asio::awaitable<void> CoSetUp() override {
    co_await NeoRadosTest::CoSetUp();
    co_await write_object(data);
  }

  asio::awaitable<void> CoTearDown() override {
    co_await remove_object();
    co_await NeoRadosTest::CoTearDown();
  }

  auto assert_version(uint64_t v) {
    return execute(oid, ReadOp{}.assert_version(v));
  }

  auto setxattr(std::string_view xattr, buffer::list bl) {
    return execute(oid, WriteOp{}.setxattr(xattr, std::move(bl)));
  }

  auto cmpxattr(std::string_view xattr, cmp_op op, buffer::list bl) {
    return execute(oid, ReadOp{}.cmpxattr(xattr, op, std::move(bl)));
  }
};

CORO_TEST_F(NeoRadosReadOps, SetOpFlags, ReadOpTest) {
  sys::error_code ec;
  co_await execute(oid, ReadOp{}
		   .exec("rbd"sv, "get_id"sv, {}, nullptr, &ec)
                   .set_failok());
  EXPECT_EQ(sys::errc::io_error, ec);
  co_return;
}

CORO_TEST_F(NeoRadosReadOps, AssertExists, ReadOpTest) {
  co_await expect_error_code(execute("nonexistent"sv, ReadOp{}.assert_exists()),
			     sys::errc::no_such_file_or_directory);
  co_await execute(oid, ReadOp{}.assert_exists());

  co_return;
}

CORO_TEST_F(NeoRadosReadOps, AssertVersion, ReadOpTest) {
  std::uint64_t v = 0;
  // Write to the object a second time to guarantee that its
  // version number is greater than 0
  co_await write_object(data, &v);

  co_await expect_error_code(assert_version(v + 1),
			     sys::errc::value_too_large);
  co_await assert_version(v);

  co_return;
}

CORO_TEST_F(NeoRadosReadOps, CmpXattr, ReadOpTest) {
  using enum cmp_op;
  using sys::errc::operation_canceled;

  static constexpr auto xattr = "test"sv;

  co_await setxattr(xattr, filled_buffer_list(0xcc, datalen));

  // Equal value
  co_await cmpxattr(xattr, eq, filled_buffer_list(0xcc, datalen));
  co_await expect_error_code(cmpxattr(xattr, ne,
				      filled_buffer_list(0xcc, datalen)),
			     operation_canceled);
  co_await expect_error_code(cmpxattr(xattr, gt,
				      filled_buffer_list(0xcc, datalen)),
			     operation_canceled);
  co_await cmpxattr(xattr, gte, filled_buffer_list(0xcc, datalen));
  co_await expect_error_code(cmpxattr(xattr, lt,
				      filled_buffer_list(0xcc, datalen)),
			     operation_canceled);
  co_await cmpxattr(xattr, lte, filled_buffer_list(0xcc, datalen));

  // < value
  co_await expect_error_code(cmpxattr(xattr, eq,
				      filled_buffer_list(0xcb, datalen)),
			     operation_canceled);
  co_await cmpxattr(xattr, ne, filled_buffer_list(0xcb, datalen));
  co_await expect_error_code(cmpxattr(xattr, gt,
				      filled_buffer_list(0xcb, datalen)),
			     operation_canceled);
  co_await expect_error_code(cmpxattr(xattr, gte,
				      filled_buffer_list(0xcb, datalen)),
			     operation_canceled);
  co_await cmpxattr(xattr, lt, filled_buffer_list(0xcb, datalen));
  co_await cmpxattr(xattr, lte, filled_buffer_list(0xcb, datalen));

  // > value
  co_await expect_error_code(cmpxattr(xattr, eq,
				      filled_buffer_list(0xcd, datalen)),
			     operation_canceled);
  co_await cmpxattr(xattr, ne, filled_buffer_list(0xcd, datalen));
  co_await cmpxattr(xattr, gt, filled_buffer_list(0xcd, datalen));
  co_await cmpxattr(xattr, gte, filled_buffer_list(0xcd, datalen));
  co_await expect_error_code(cmpxattr(xattr, lt,
				      filled_buffer_list(0xcd, datalen)),
			     operation_canceled);
  co_await expect_error_code(cmpxattr(xattr, lte,
				      filled_buffer_list(0xcd, datalen)),
			     operation_canceled);

  // check that null bytes are compared correctly

  co_await setxattr(xattr, to_buffer_list("\x00\x00"sv));
  co_await expect_error_code(cmpxattr(xattr, eq,
				      to_buffer_list("\x00\xcc"sv)),
			     operation_canceled);
  co_await cmpxattr(xattr, ne, to_buffer_list("\x00\xcc"sv));
  co_await cmpxattr(xattr, gt, to_buffer_list("\x00\xcc"sv));
  co_await cmpxattr(xattr, gte, to_buffer_list("\x00\xcc"sv));
  co_await expect_error_code(cmpxattr(xattr, lt,
				      to_buffer_list("\x00\xcc"sv)),
			     operation_canceled);
  co_await expect_error_code(cmpxattr(xattr, lte,
				      to_buffer_list("\x00\xcc"sv)),
			     operation_canceled);

  co_await cmpxattr(xattr, eq, to_buffer_list("\x00\x00"sv));
  co_await expect_error_code(cmpxattr(xattr, ne,
				      to_buffer_list("\x00\x00"sv)),
			     operation_canceled);
  co_await expect_error_code(cmpxattr(xattr, gt,
				      to_buffer_list("\x00\x00"sv)),
			     operation_canceled);
  co_await cmpxattr(xattr, gte, to_buffer_list("\x00\x00"sv));
  co_await expect_error_code(cmpxattr(xattr, lt,
				      to_buffer_list("\x00\x00"sv)),
			     operation_canceled);
  co_await cmpxattr(xattr, lte, to_buffer_list("\x00\x00"sv));

  co_return;
}

CORO_TEST_F(NeoRadosReadOps, Read, ReadOpTest) {
  // Check that using read_ops returns the same data with
  // or without ec out params
  {
    buffer::list bl;
    co_await execute(oid, ReadOp{}.read(0, 0, &bl));
    EXPECT_TRUE((data.length() == bl.length()) &&
		(0 == std::memcmp(data.data(), bl.c_str(), data.length())));
  }
  {
    buffer::list bl;
    sys::error_code ec;
    co_await execute(oid, ReadOp{}.read(0, 0, &bl, &ec));
    EXPECT_TRUE((data.length() == bl.length()) &&
		(0 == std::memcmp(data.data(), bl.c_str(), data.length())));
    EXPECT_FALSE(ec);
  }

  {
    buffer::list bl;
    sys::error_code ec;
    co_await execute(oid, ReadOp{}
		     .read(0, 0, &bl, &ec)
		     .set_fadvise_dontneed());
    EXPECT_TRUE((data.length() == bl.length()) &&
		(0 == std::memcmp(data.data(), bl.c_str(), data.length())));
    EXPECT_FALSE(ec);
  }
  co_return;
}

inline std::uint32_t crc32c(uint32_t seed, std::string_view v) {
  return ceph_crc32c(
    seed, reinterpret_cast<const uint8_t*>(v.data()),
    uint32_t(v.size()));
}

CORO_TEST_F(NeoRadosReadOps, Checksum, ReadOpTest) {
  {
    static constexpr uint64_t seed = -1;
    std::vector<uint64_t> hash;

    co_await execute(oid, ReadOp{}
		     .checksum(hash_alg::xxhash64, seed, 0, 0, 0, &hash));
    EXPECT_EQ(1u, hash.size());
    EXPECT_EQ(XXH64(data.data(), data.size(), seed), hash[0]);
  }
  {
    static constexpr uint32_t seed = -1;
    std::vector<uint32_t> crc;
    co_await execute(oid, ReadOp{}
		     .checksum(hash_alg::crc32c, seed, 0, 0, 0, &crc));
    EXPECT_EQ(crc32c(seed, data), crc[0]);
  }
  {
    static constexpr uint32_t seed = -1;
    std::vector<uint32_t> hash;
    co_await execute(oid, ReadOp{}
		     .checksum(hash_alg::xxhash32, seed, 0, 0, 0, &hash));
    EXPECT_EQ(XXH32(data.data(), data.size(), seed), hash[0]);
  }

  {
    static constexpr uint32_t seed = -1;
    std::vector<uint32_t> crc;
    co_await execute(oid, ReadOp{}.checksum(hash_alg::crc32c, seed, 0,
					    data.length(), 4, &crc));
    EXPECT_EQ(crc32c(seed, data.substr(0, 4)), crc[0]);
    EXPECT_EQ(crc32c(seed, data.substr(4, 4)), crc[1]);
  }

  co_return;
}

CORO_TEST_F(NeoRadosReadOps, RWOrderedRead, ReadOpTest) {
  buffer::list bl;
  sys::error_code ec;
  ReadOp op;
  op.read(0, 0, &bl, &ec);
  op.set_fadvise_dontneed();
  op.order_reads_writes();
  co_await execute(oid, std::move(op));

  EXPECT_FALSE(ec);
  EXPECT_TRUE((data.length() == bl.length()) &&
	      (0 == std::memcmp(data.data(), bl.c_str(), data.length())));

  co_return;
}

CORO_TEST_F(NeoRadosReadOps, ShortRead, ReadOpTest) {
  constexpr auto read_len = data.size() * 2;
  buffer::list bl;
  // check that using read_ops returns the same data with
  // or without ec out params
  co_await execute(oid, ReadOp{}.read(0, read_len, &bl));
  EXPECT_TRUE((data.length() == bl.length()) &&
	      (0 == std::memcmp(data.data(), bl.c_str(), data.length())));

  sys::error_code ec;
  bl.clear();
  co_await execute(oid, ReadOp{}.read(0, read_len, &bl, &ec));
  EXPECT_FALSE(ec);
  EXPECT_TRUE((data.length() == bl.length()) &&
	      (0 == std::memcmp(data.data(), bl.c_str(), data.length())));
  co_return;
}

CORO_TEST_F(NeoRadosReadOps, Exec, ReadOpTest) {
  buffer::list bl;
  sys::error_code ec;
  co_await execute(oid,
		   ReadOp{}.exec("rbd"sv, "get_all_features"sv, {}, &bl, &ec));
  EXPECT_FALSE(ec);
  std::uint64_t features;
  EXPECT_EQ(sizeof(features), bl.length());
  auto it = bl.cbegin();
  ceph::decode(features, it);
  EXPECT_EQ(RBD_FEATURES_ALL, features);
  co_return;
}

CORO_TEST_F(NeoRadosReadOps, Stat, ReadOpTest) {
  std::uint64_t size = 1;
  sys::error_code ec;
  co_await expect_error_code(execute("nonexistent"sv,
				     ReadOp{}.stat(&size, nullptr, &ec)),
			     sys::errc::no_such_file_or_directory);
  EXPECT_EQ(sys::errc::io_error, ec);
  EXPECT_EQ(1u, size);

  const ceph::real_time ts{1'457'129'052 * 1s};
  auto bl = to_buffer_list(data);
  co_await execute(oid, WriteOp{}.write(0, std::move(bl)).set_mtime(ts));

  ceph::real_time ts2;
  ec.clear();
  co_await execute(oid, ReadOp{}.stat(&size, &ts2, &ec));
  EXPECT_FALSE(ec);
  EXPECT_EQ(data.size(), size);
  EXPECT_EQ(ts, ts2);

  co_await execute(oid, ReadOp{}.stat(nullptr, nullptr));

  co_await expect_error_code(execute("nonexistent"sv,
				     ReadOp{}.stat(nullptr, nullptr)),
			     sys::errc::no_such_file_or_directory);

  co_return;
}


CORO_TEST_F(NeoRadosReadOps, Omap, ReadOpTest) {
  const ctnr::flat_map<std::string, buffer::list> omap{
    {"bar"s, {}},
    {"foo"s, to_buffer_list("\0"sv)},
    {"test1"s, to_buffer_list("abc"sv)},
    {"test2"s, to_buffer_list("va\0lue"sv)}
  };

  co_await expect_error_code(
    execute("nonexistent"sv,
	    ReadOp{}.get_omap_vals({}, {}, 10, nullptr, nullptr)),
    sys::errc::no_such_file_or_directory);

  {
    ctnr::flat_map<std::string, buffer::list> omap2;
    bool truncated;
    sys::error_code ec;
    co_await execute(oid, ReadOp{}.get_omap_vals({}, {}, 10, &omap2,
						 &truncated, &ec));
    EXPECT_FALSE(ec);
    EXPECT_TRUE(omap2.empty());
    EXPECT_FALSE(truncated);
  }

  co_await execute(oid, WriteOp{}.set_omap(omap));

  // Check for readability
  {
    ctnr::flat_map<std::string, buffer::list> omap2;
    ctnr::flat_set<std::string> keys;
    bool truncated, truncated2;
    sys::error_code ec, ec2;

    co_await execute(oid, ReadOp{}
		     .get_omap_vals({}, {}, 10, &omap2, &truncated, &ec)
		     .get_omap_keys({}, 10, &keys, &truncated2, &ec2));
    EXPECT_FALSE(ec);
    EXPECT_FALSE(ec2);
    EXPECT_FALSE(truncated2);
    EXPECT_EQ(omap, omap2);
    EXPECT_FALSE(truncated);
    EXPECT_EQ(omap.size(), keys.size());
    EXPECT_TRUE(std::all_of(keys.begin(), keys.end(),
			    [&](const auto& s) {
			      return omap.contains(s);
			    }));
    EXPECT_TRUE(std::all_of(omap.begin(), omap.end(),
			    [&](const auto& kv) {
			      return keys.contains(kv.first);
			    }));
  }

  // Check iteration and truncation
  {
    std::unordered_set<std::string> keys;
    for (const auto& [key, value] : omap) {
      keys.insert(key);
    }
    bool truncated = true;
    std::optional<std::string> lastkey;
    while (truncated) {
      ctnr::flat_set<std::string> keys2;
      ctnr::flat_map<std::string, buffer::list> omap2;
      bool truncated2;
      ReadOp op;
      op.get_omap_vals(lastkey, {}, 1, &omap2, &truncated);
      op.get_omap_keys(lastkey, 1, &keys2, &truncated2);
      co_await execute(oid, std::move(op));
      EXPECT_EQ(1, std::ssize(keys2));
      EXPECT_EQ(1, std::ssize(omap2));
      EXPECT_EQ(truncated, truncated2);

      const auto& key = *keys2.begin();
      EXPECT_EQ(omap2.begin()->first, key);
      EXPECT_TRUE(keys.contains(key));
      EXPECT_EQ(omap.at(key), omap2[key]);
      keys.erase(key);
      lastkey = key;
    }
    EXPECT_TRUE(keys.empty());
  }

  // check omap_cmp finds all expected values
  {
    ReadOp op;
    for (const auto& [key, value] : omap) {
      op.cmp_omap({{key, cmp_op::eq, value}});
    }
    co_await execute(oid, std::move(op));
  }
  {
    std::vector<neorados::cmp_assertion> cmps;
    for (const auto& [key, value] : omap) {
      cmps.push_back({key,  cmp_op::eq, value});
    }
    co_await execute(oid, ReadOp{}.cmp_omap(cmps));
  }

  // try to remove keys with a guard that should fail
  {
    WriteOp op;
    auto key = (omap.begin() + 2)->first;
    op.cmp_omap({{key, cmp_op::lt,omap.at(key)}});
    op.rm_omap_keys({omap.begin()->first, (omap.begin() + 1)->first});
    co_await expect_error_code(execute(oid, std::move(op)),
			       sys::errc::operation_canceled);
  }
  // Verify the keys are still there, and then remove them
  {
    WriteOp op;
    op.cmp_omap({{omap.begin()->first, cmp_op::eq, omap.begin()->second}});
    op.cmp_omap({{(omap.begin() + 1)->first, cmp_op::eq,
		  {(omap.begin() + 1)->second}}});
    op.rm_omap_keys({omap.begin()->first, (omap.begin() + 1)->first});
    co_await execute(oid, std::move(op));

    ctnr::flat_map<std::string, buffer::list> omap2;
    const ctnr::flat_map omapcmp{omap.begin() + 2, omap.end()};
    bool trunc;
    co_await execute(oid, ReadOp{}.get_omap_vals({}, {}, 10, &omap2, &trunc));
    EXPECT_FALSE(trunc);
    EXPECT_EQ(omapcmp, omap2);
  }

  // clear the rest and check there are none left
  {
    co_await execute(oid, WriteOp{}.clear_omap());
    ctnr::flat_map<std::string, buffer::list> omap2;
    bool trunc;
    co_await execute(oid, ReadOp{}.get_omap_vals({}, {}, 10, &omap2, &trunc));
    EXPECT_FALSE(trunc);
    EXPECT_TRUE(omap2.empty());
  }
  co_return;
}

CORO_TEST_F(NeoRadosReadOps, OmapNuls, ReadOpTest) {
  const ctnr::flat_map<std::string, buffer::list> omap{
    {"1\0bar"s, to_buffer_list("_\0var"sv)},
    {"2baar\0"s, to_buffer_list("_vaar\0"sv)},
    {"3baa\0rr"s, to_buffer_list("__vaa\0rr"sv)}
  };

  co_await expect_error_code(
    execute("nonexistent"sv, ReadOp{}.get_omap_vals({}, {}, 10, nullptr, nullptr)),
    sys::errc::no_such_file_or_directory);
  {
    ctnr::flat_map<std::string, buffer::list> omap2;
    bool truncated;
    sys::error_code ec;
    co_await execute(oid, ReadOp{}
		     .get_omap_vals({}, {}, 10, &omap2, &truncated, &ec));
    EXPECT_FALSE(ec);
    EXPECT_TRUE(omap2.empty());
    EXPECT_FALSE(truncated);
  }

  co_await execute(oid, WriteOp{}.set_omap(omap));

  // Check for readability
  {
    ctnr::flat_map<std::string, buffer::list> omap2;
    ctnr::flat_set<std::string> keys;
    bool truncated, truncated2;
    sys::error_code ec, ec2;
    ReadOp op;
    op.get_omap_vals({}, {}, 10, &omap2, &truncated, &ec);
    op.get_omap_keys({}, 10, &keys, &truncated2, &ec2);
    co_await execute(oid, std::move(op));
    EXPECT_FALSE(ec);
    EXPECT_FALSE(ec2);
    EXPECT_FALSE(truncated2);
    EXPECT_EQ(omap, omap2);
    EXPECT_FALSE(truncated);
    EXPECT_EQ(omap.size(), keys.size());
    EXPECT_TRUE(std::all_of(keys.begin(), keys.end(),
			    [&](const auto& s) {
			      return omap.contains(s);
			    }));
    EXPECT_TRUE(std::all_of(omap.begin(), omap.end(),
			    [&](const auto& kv) {
			      return keys.contains(kv.first);
			    }));
  }

  // Check iteration and truncation
  {
    std::unordered_set<std::string> keys;
    for (const auto& [key, value] : omap) {
      keys.insert(key);
    }
    bool truncated = true;
    std::optional<std::string> lastkey;
    while (truncated) {
      ctnr::flat_set<std::string> keys2;
      ctnr::flat_map<std::string, buffer::list> omap2;
      bool truncated2;
      ReadOp op;
      op.get_omap_vals(lastkey, {}, 1, &omap2, &truncated);
      op.get_omap_keys(lastkey, 1, &keys2, &truncated2);
      co_await execute(oid, std::move(op));
      EXPECT_EQ(1, std::ssize(keys2));
      EXPECT_EQ(1, std::ssize(omap2));
      EXPECT_EQ(truncated, truncated2);

      const auto& key = *keys2.begin();
      EXPECT_EQ(omap2.begin()->first, key);
      EXPECT_TRUE(keys.contains(key));
      EXPECT_EQ(omap.at(key), omap2[key]);
      keys.erase(key);
      lastkey = key;
    }
    EXPECT_TRUE(keys.empty());
  }

  // check omap_cmp finds all expected values
  {
    ReadOp op;
    for (const auto& [key, value] : omap) {
      op.cmp_omap({{key, cmp_op::eq, value}});
    }
    co_await execute(oid, std::move(op));
  }
  {
    std::vector<neorados::cmp_assertion> cmps;
    for (const auto& [key, value] : omap) {
      cmps.push_back({key, cmp_op::eq, value});
    }
    co_await execute(oid, ReadOp{}.cmp_omap(cmps));
  }

  // try to remove keys with a guard that should fail
  {
    WriteOp op;
    auto key = (omap.begin() + 2)->first;
    op.cmp_omap({{key, cmp_op::lt, omap.at(key)}});
    op.rm_omap_keys({omap.begin()->first, (omap.begin() + 1)->first});
    co_await expect_error_code(execute(oid, std::move(op)),
			       sys::errc::operation_canceled);
  }
  // Verify the keys are still there, and then remove them
  {
    WriteOp op;
    op.cmp_omap({{omap.begin()->first, cmp_op::eq, omap.begin()->second}});
    op.cmp_omap({{(omap.begin() + 1)->first, cmp_op::eq,
		  (omap.begin() + 1)->second}});
    op.rm_omap_keys({omap.begin()->first, (omap.begin() + 1)->first});
    co_await execute(oid, std::move(op));

    ctnr::flat_map<std::string, buffer::list> omap2;
    const ctnr::flat_map omapcmp{omap.begin() + 2, omap.end()};
    bool trunc;
    co_await execute(oid, ReadOp{}.get_omap_vals({}, {}, 10, &omap2, &trunc));
    EXPECT_FALSE(trunc);
    EXPECT_EQ(omapcmp, omap2);
  }

  // clear the rest and check there are none left
  {
    co_await execute(oid, WriteOp{}.clear_omap());
    ctnr::flat_map<std::string, buffer::list> omap2;
    bool trunc;
    co_await execute(oid, ReadOp{}.get_omap_vals({}, {}, 10, &omap2, &trunc));
    EXPECT_FALSE(trunc);
    EXPECT_TRUE(omap2.empty());
  }
  co_return;
}

CORO_TEST_F(NeoRadosReadOps, GetXattrs, ReadOpTest) {
  const ctnr::flat_map<std::string, buffer::list> xattrs{
    {"bar"s, {}},
    {"foo"s, to_buffer_list("\0"sv)},
    {"test1"s, to_buffer_list("abc"sv)},
    {"test2"s, to_buffer_list("va\0lue"sv)}
  };

  {
    ctnr::flat_map<std::string, buffer::list> xattrs2;
    sys::error_code ec;
    co_await execute(oid, ReadOp{}.get_xattrs(&xattrs2, &ec));
    EXPECT_FALSE(ec);
    EXPECT_TRUE(xattrs2.empty());
  }

  {
    WriteOp op;
    for (const auto& [key, value] : xattrs) {
      op.setxattr(key, buffer::list{value});
    }
    co_await execute(oid, std::move(op));
  }

  {
    ctnr::flat_map<std::string, buffer::list> xattrs2;
    sys::error_code ec;
    co_await execute(oid, ReadOp{}.get_xattrs(&xattrs2, &ec));
    EXPECT_FALSE(ec);
    EXPECT_EQ(xattrs, xattrs2);
  }

  {
    ReadOp op;
    std::vector<buffer::list> bls;
    std::vector<sys::error_code> ecs;
    bls.reserve(xattrs.size());
    ecs.reserve(xattrs.size());
    for (const auto& [key, value] : xattrs) {
      bls.push_back({});
      ecs.push_back({});
      op.get_xattr(key, &bls.back(), &ecs.back());
    }

    co_await execute(oid, std::move(op));

    EXPECT_EQ(xattrs.size(), ecs.size());
    EXPECT_EQ(xattrs.size(), bls.size());
    for (auto i = 0; i < std::ssize(xattrs); ++i) {
      const auto& key = (xattrs.begin() + i)->first;
      EXPECT_FALSE(ecs[i]);
      EXPECT_EQ(xattrs.at(key), bls[i]);
    }
  }

  {
    ReadOp op;
    for (const auto& [key, value] : xattrs) {
      op.cmpxattr(key, cmp_op::eq, value);
    }
    co_await execute(oid, std::move(op));
  }

  co_return;
}

CORO_TEST_F(NeoRadosReadOps, CmpExt, ReadOpTest) {
  co_await execute(oid, WriteOp{}.write_full(to_buffer_list("\x01\x02\x03"sv)));
  uint64_t unmatch = 0;
  {
    buffer::list bl;
    ReadOp op;
    op.cmpext(0, to_buffer_list("\x01\x02\x03"sv), &unmatch);
    op.read(0, 0, &bl);
    co_await execute(oid, std::move(op));
    EXPECT_EQ(-1 , unmatch);
    EXPECT_EQ(to_buffer_list("\x01\x02\x03"sv), bl);
  }
  {
    buffer::list bl;
    ReadOp op;
    op.cmpext(0, to_buffer_list("\x00\x02\x03"sv), &unmatch);
    op.read(0, 0, &bl);
    co_await expect_error_code(execute(oid, std::move(op)),
			       osd_errc::cmpext_mismatch);
    EXPECT_EQ(0 , unmatch);
    EXPECT_EQ(0, bl.length());
  }
  {
    buffer::list bl;
    ReadOp op;
    op.cmpext(0, to_buffer_list("\x01\x00\x03"sv), &unmatch);
    op.read(0, 0, &bl);
    co_await expect_error_code(execute(oid, std::move(op)),
			       osd_errc::cmpext_mismatch);
    EXPECT_EQ(1, unmatch);
    EXPECT_EQ(0, bl.length());
  }
  {
    buffer::list bl;
    ReadOp op;
    op.cmpext(0, to_buffer_list("\x01\x02\x00"sv), &unmatch);
    op.read(0, 0, &bl);
    co_await expect_error_code(execute(oid, std::move(op)),
			       osd_errc::cmpext_mismatch);
    EXPECT_EQ(2, unmatch);
    EXPECT_EQ(0, bl.length());
  }
  {
    buffer::list bl;
    ReadOp op;
    op.cmpext(0, to_buffer_list("\x01\x02\x03\x04"sv), &unmatch);
    op.read(0, 0, &bl);
    co_await expect_error_code(execute(oid, std::move(op)),
			       osd_errc::cmpext_mismatch);
    EXPECT_EQ(3, unmatch);
    EXPECT_EQ(0, bl.length());
  }
  // Make sure other error codes work properly
  {
    buffer::list bl;
    ReadOp op;
    op.cmpext(0, to_buffer_list("\x01\x02\x03"sv), &unmatch);
    op.read(0, 0, &bl);
    co_await expect_error_code(execute("nonexistent"sv, std::move(op)),
			       sys::errc::no_such_file_or_directory);
    EXPECT_EQ(-1, unmatch);
    EXPECT_EQ(0, bl.length());
  }
  co_return;
}
