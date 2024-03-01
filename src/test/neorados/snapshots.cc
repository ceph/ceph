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
#include <cstdint>
#include <cstring>
#include <iostream>
#include <utility>
#include <vector>

#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/awaitable.hpp>

#include <boost/system/errc.hpp>

#include "include/neorados/RADOS.hpp"

#include "osd/error_code.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

using std::uint64_t;

namespace asio = boost::asio;
namespace sys = boost::system;

using namespace std::literals;

using neorados::ReadOp;
using neorados::WriteOp;

inline asio::awaitable<void> new_selfmanaged_snap(neorados::RADOS& rados,
						  std::vector<uint64_t>& snaps,
						  neorados::IOContext& ioc) {
  snaps.push_back(co_await rados.allocate_selfmanaged_snap(
		    ioc.get_pool(), asio::use_awaitable));
  std::reverse(snaps.begin(), snaps.end());
  ioc.set_write_snap_context({{snaps[0], snaps}});
  std::reverse(snaps.begin(), snaps.end());
  co_return;
}

inline asio::awaitable<void> rm_selfmanaged_snaps(neorados::RADOS& rados,
						  std::vector<uint64_t>& snaps,
						  neorados::IOContext& ioc) {
  std::reverse(snaps.begin(), snaps.end());
  for (auto snapid : snaps) {
    co_await rados.delete_selfmanaged_snap(ioc.get_pool(), snapid,
					   asio::use_awaitable);
  }
  snaps.clear();
}

static constexpr auto oid = "oid"sv;

CORO_TEST_F(NeoRadosSnapshots, SnapList, NeoRadosTest) {
  static const auto snap1 = "snap1"s;
  co_await create_obj(oid);
  EXPECT_FALSE(rados().get_self_managed_snaps_mode(pool()));
  co_await rados().create_pool_snap(pool(), snap1,
                                    asio::use_awaitable);
  EXPECT_FALSE(rados().get_self_managed_snaps_mode(pool()));

  auto snaps = rados().list_snaps(pool());
  EXPECT_EQ(1u, snaps.size());
  auto rid = rados().lookup_snap(pool(), snap1);
  EXPECT_EQ(rid, snaps[0]);
  co_await rados().delete_pool_snap(pool().get_pool(), snap1, asio::use_awaitable);
  EXPECT_FALSE(rados().get_self_managed_snaps_mode(pool()));
  co_return;
}

CORO_TEST_F(NeoRadosSnapshots, SnapRemove, NeoRadosTest) {
  static const auto snap1 = "snap1"s;
  co_await create_obj(oid);
  co_await rados().create_pool_snap(pool(), snap1,
                                    asio::use_awaitable);
  rados().lookup_snap(pool(), snap1);
  co_await rados().delete_pool_snap(pool().get_pool(), snap1, asio::use_awaitable);
  EXPECT_THROW(rados().lookup_snap(pool(), snap1);,
	       sys::system_error);

  co_return;
}

CORO_TEST_F(NeoRadosSnapshots, Rollback, NeoRadosTest) {
  static const auto snap1 = "snap1"s;
  const auto bl1 = filled_buffer_list(0xcc, 128);
  const auto bl2 = filled_buffer_list(0xdd, 128);

  co_await execute(oid, WriteOp{}.write(0, bl1));
  co_await rados().create_pool_snap(pool(), snap1, asio::use_awaitable);
  co_await execute(oid, WriteOp{}.write_full(bl2));

  auto resbl = co_await read(oid);
  EXPECT_EQ(bl2, resbl);

  co_await execute(oid, WriteOp{}.rollback(rados().lookup_snap(pool(), snap1)));

  resbl = co_await read(oid);
  EXPECT_EQ(bl1, resbl);

  co_return;
}

CORO_TEST_F(NeoRadosSnapshots, SnapGetName, NeoRadosTest) {
  static const auto snapfoo = "snapfoo"s;
  static const auto snapbar = "snapbar"s;
  co_await create_obj(oid);
  co_await rados().create_pool_snap(pool(), snapfoo, asio::use_awaitable);
  auto rid = rados().lookup_snap(pool(), snapfoo);
  EXPECT_EQ(snapfoo, rados().get_snap_name(pool(), rid));
  rados().get_snap_timestamp(pool(), rid);
  co_await rados().delete_pool_snap(pool().get_pool(), snapfoo, asio::use_awaitable);
  co_return;
}

CORO_TEST_F(NeoRadosSnapshots, SnapCreateRemove, NeoRadosTest) {
  // reproduces http://tracker.ceph.com/issues/10262
  static const auto snapfoo = "snapfoo"s;
  static const auto snapbar = "snapbar"s;
  const auto bl = to_buffer_list("foo"sv);
  co_await execute(oid, WriteOp{}.write_full(bl));
  co_await rados().create_pool_snap(pool(), snapfoo, asio::use_awaitable);
  co_await execute(oid, WriteOp{}.remove());
  co_await rados().create_pool_snap(pool(), snapbar, asio::use_awaitable);

  WriteOp op;
  op.create(false);
  op.remove();
  co_await execute(oid, std::move(op));
  co_await rados().delete_pool_snap(pool().get_pool(), snapfoo,
				    asio::use_awaitable);
  co_await rados().delete_pool_snap(pool().get_pool(), snapbar,
				    asio::use_awaitable);
  co_return;
}

CORO_TEST_F(NeoRadosSelfManagedSnaps, Snap, NeoRadosTest) {
  std::vector<uint64_t> my_snaps;
  EXPECT_FALSE(rados().get_self_managed_snaps_mode(pool()));
  auto ioc = pool();
  co_await new_selfmanaged_snap(rados(), my_snaps, ioc);
  EXPECT_TRUE(rados().get_self_managed_snaps_mode(pool()));


  const auto bl1 = filled_buffer_list(0xcc, 128);
  co_await execute(oid, WriteOp{}.write(0, bl1), ioc);

  co_await new_selfmanaged_snap(rados(), my_snaps, ioc);
  const auto bl2 = filled_buffer_list(0xdd, 128);
  co_await execute(oid, WriteOp{}.write(0, bl2), ioc);

  ioc.set_read_snap(my_snaps[1]);
  auto resbl = co_await read(oid, ioc);
  EXPECT_EQ(bl1, resbl);

  co_await rados().delete_selfmanaged_snap(ioc.get_pool(), my_snaps.back(),
					   asio::use_awaitable);
  my_snaps.pop_back();
  ioc.set_read_snap(neorados::snap_head);
  EXPECT_TRUE(rados().get_self_managed_snaps_mode(pool()));
  co_await execute(oid, WriteOp{}.remove());
  co_return;
}

CORO_TEST_F(NeoRadosSelfManagedSnaps, Rollback, NeoRadosTest) {
  SKIP_IF_CRIMSON();
  static constexpr auto len = 128u;
  std::vector<uint64_t> my_snaps;

  auto ioc = pool();
  auto readioc = pool();
  readioc.set_read_snap(neorados::snap_dir);

  co_await new_selfmanaged_snap(rados(), my_snaps, ioc);
  const auto bl1 = filled_buffer_list(0xcc, len);
  co_await execute(oid, WriteOp{}.write(0, bl1), ioc);
  co_await execute(oid, WriteOp{}.write(len, bl1), ioc);
  co_await execute(oid, WriteOp{}.write(len * 2, bl1), ioc);

  neorados::SnapSet ss;
  co_await execute(oid, ReadOp{}.list_snaps(&ss), readioc);
  EXPECT_EQ(1u, ss.clones.size());
  EXPECT_EQ(neorados::snap_head, ss.clones[0].cloneid);
  EXPECT_EQ(0u, ss.clones[0].snaps.size());
  EXPECT_EQ(0u, ss.clones[0].overlap.size());
  EXPECT_EQ(len * 3, ss.clones[0].size);

  co_await new_selfmanaged_snap(rados(), my_snaps, ioc);
  const auto bl2 = filled_buffer_list(0xdd, 128);
  // Once in the middle
  co_await execute(oid, WriteOp{}.write(len, bl2), ioc);
  // Once after the end
  co_await execute(oid, WriteOp{}.write(len * 3, bl1), ioc);


  co_await expect_error_code(execute(oid, ReadOp{}.list_snaps(&ss), ioc),
			     sys::errc::invalid_argument);
  co_await execute(oid, ReadOp{}.list_snaps(&ss), readioc);
  EXPECT_EQ(2u, ss.clones.size());
  EXPECT_EQ(my_snaps[1], ss.clones[0].cloneid);
  EXPECT_EQ(1u, ss.clones[0].snaps.size());
  EXPECT_EQ(my_snaps[1], ss.clones[0].snaps[0]);
  EXPECT_EQ(2u, ss.clones[0].overlap.size());
  EXPECT_EQ(0u, ss.clones[0].overlap[0].first);
  EXPECT_EQ(len, ss.clones[0].overlap[0].second);
  EXPECT_EQ(len * 2, ss.clones[0].overlap[1].first);
  EXPECT_EQ(len, ss.clones[0].overlap[1].second);
  EXPECT_EQ(len * 3, ss.clones[0].size);
  EXPECT_EQ(neorados::snap_head, ss.clones[1].cloneid);
  EXPECT_EQ(0u, ss.clones[1].snaps.size());
  EXPECT_EQ(0u, ss.clones[1].overlap.size());
  EXPECT_EQ(len * 4, ss.clones[1].size);

  co_await execute(oid, WriteOp{}.rollback(my_snaps[1]), ioc);

  auto resbl = co_await read(oid, 0, len);
  EXPECT_EQ(len, resbl.length());
  EXPECT_EQ(bl1, resbl);
  resbl = co_await read(oid, len, len);
  EXPECT_EQ(len, resbl.length());
  EXPECT_EQ(bl1, resbl);

  resbl = co_await read(oid, len * 2, len);
  EXPECT_EQ(len, resbl.length());
  EXPECT_EQ(bl1, resbl);

  resbl = co_await read(oid, len * 3, len);
  EXPECT_EQ(0u, resbl.length());

  co_await rm_selfmanaged_snaps(rados(), my_snaps, ioc);

  co_return;
}

CORO_TEST_F(NeoRadosSelfManagedSnaps, SnapOverlap, NeoRadosTest) {
  // WIP https://tracker.ceph.com/issues/58263
  SKIP_IF_CRIMSON();
  static constexpr auto len = 128u;
  std::vector<uint64_t> my_snaps;
  auto ioc = pool();
  auto readioc = pool();
  readioc.set_read_snap(neorados::snap_dir);

  co_await new_selfmanaged_snap(rados(), my_snaps, ioc);
  const auto bl1 = filled_buffer_list(0xcc, len);
  co_await execute(oid, WriteOp{}.write(0, bl1), ioc);
  co_await execute(oid, WriteOp{}.write(len * 2, bl1), ioc);
  co_await execute(oid, WriteOp{}.write(len * 4, bl1), ioc);
  co_await execute(oid, WriteOp{}.write(len * 6, bl1), ioc);
  co_await execute(oid, WriteOp{}.write(len * 8, bl1), ioc);

  neorados::SnapSet ss;
  co_await execute(oid, ReadOp{}.list_snaps(&ss), readioc);
  EXPECT_EQ(1u, ss.clones.size());
  EXPECT_EQ(neorados::snap_head, ss.clones[0].cloneid);
  EXPECT_EQ(0u, ss.clones[0].snaps.size());
  EXPECT_EQ(0u, ss.clones[0].overlap.size());
  EXPECT_EQ(9u * len, ss.clones[0].size);

  co_await new_selfmanaged_snap(rados(), my_snaps, ioc);
  const auto bl2 = filled_buffer_list(0xdd, len);
  co_await execute(oid, WriteOp{}.write(len * 1, bl2), ioc);
  co_await execute(oid, WriteOp{}.write(len * 3, bl2), ioc);
  co_await execute(oid, WriteOp{}.write(len * 5, bl2), ioc);
  co_await execute(oid, WriteOp{}.write(len * 7, bl2), ioc);
  co_await execute(oid, WriteOp{}.write(len * 9, bl2), ioc);

  co_await execute(oid, ReadOp{}.list_snaps(&ss), readioc);
  EXPECT_EQ(2u, ss.clones.size());
  EXPECT_EQ(my_snaps[1], ss.clones[0].cloneid);
  EXPECT_EQ(1u, ss.clones[0].snaps.size());
  EXPECT_EQ(my_snaps[1], ss.clones[0].snaps[0]);
  EXPECT_EQ(5u, ss.clones[0].overlap.size());
  EXPECT_EQ(0u, ss.clones[0].overlap[0].first);
  EXPECT_EQ(len, ss.clones[0].overlap[0].second);
  EXPECT_EQ(len * 2, ss.clones[0].overlap[1].first);
  EXPECT_EQ(len, ss.clones[0].overlap[1].second);
  EXPECT_EQ(len * 4, ss.clones[0].overlap[2].first);
  EXPECT_EQ(len, ss.clones[0].overlap[2].second);
  EXPECT_EQ(len * 6, ss.clones[0].overlap[3].first);
  EXPECT_EQ(len, ss.clones[0].overlap[3].second);
  EXPECT_EQ(len * 8, ss.clones[0].overlap[4].first);
  EXPECT_EQ(len, ss.clones[0].overlap[4].second);
  EXPECT_EQ(len * 9, ss.clones[0].size);
  EXPECT_EQ(neorados::snap_head, ss.clones[1].cloneid);
  EXPECT_EQ(0u, ss.clones[1].snaps.size());
  EXPECT_EQ(0u, ss.clones[1].overlap.size());
  EXPECT_EQ(len * 10, ss.clones[1].size);

  co_await new_selfmanaged_snap(rados(), my_snaps, ioc);

  const auto bl3 = filled_buffer_list(0xee, len);
  co_await execute(oid, WriteOp{}.write(len * 1, bl1), ioc);
  co_await execute(oid, WriteOp{}.write(len * 4, bl1), ioc);
  co_await execute(oid, WriteOp{}.write(len * 5, bl1), ioc);
  co_await execute(oid, WriteOp{}.write(len * 8, bl1), ioc);

  co_await execute(oid, ReadOp{}.list_snaps(&ss), readioc);

  EXPECT_EQ(3u, ss.clones.size());
  EXPECT_EQ(my_snaps[1], ss.clones[0].cloneid);
  EXPECT_EQ(1u, ss.clones[0].snaps.size());
  EXPECT_EQ(my_snaps[1], ss.clones[0].snaps[0]);
  EXPECT_EQ(5u, ss.clones[0].overlap.size());
  EXPECT_EQ(0u, ss.clones[0].overlap[0].first);
  EXPECT_EQ(len, ss.clones[0].overlap[0].second);
  EXPECT_EQ(len * 2, ss.clones[0].overlap[1].first);
  EXPECT_EQ(len, ss.clones[0].overlap[1].second);
  EXPECT_EQ(len * 4, ss.clones[0].overlap[2].first);
  EXPECT_EQ(len, ss.clones[0].overlap[2].second);
  EXPECT_EQ(len * 6, ss.clones[0].overlap[3].first);
  EXPECT_EQ(len, ss.clones[0].overlap[3].second);
  EXPECT_EQ(len * 8, ss.clones[0].overlap[4].first);
  EXPECT_EQ(len, ss.clones[0].overlap[4].second);
  EXPECT_EQ(len * 9, ss.clones[0].size);

  EXPECT_EQ(my_snaps[2], ss.clones[1].cloneid);
  EXPECT_EQ(1u, ss.clones[1].snaps.size());
  EXPECT_EQ(my_snaps[2], ss.clones[1].snaps[0]);
  EXPECT_EQ(4u, ss.clones[1].overlap.size());
  EXPECT_EQ(0u, ss.clones[1].overlap[0].first);
  EXPECT_EQ(len, ss.clones[1].overlap[0].second);
  EXPECT_EQ(len * 2, ss.clones[1].overlap[1].first);
  EXPECT_EQ(len * 2, ss.clones[1].overlap[1].second);
  EXPECT_EQ(len * 6, ss.clones[1].overlap[2].first);
  EXPECT_EQ(len * 2, ss.clones[1].overlap[2].second);
  EXPECT_EQ(len * 9, ss.clones[1].overlap[3].first);
  EXPECT_EQ(len, ss.clones[1].overlap[3].second);
  EXPECT_EQ(len * 10, ss.clones[1].size);

  EXPECT_EQ(neorados::snap_head, ss.clones[2].cloneid);
  EXPECT_EQ(0u, ss.clones[2].snaps.size());
  EXPECT_EQ(0u, ss.clones[2].overlap.size());
  EXPECT_EQ(len * 10, ss.clones[2].size);

  co_await rm_selfmanaged_snaps(rados(), my_snaps, ioc);

  co_return;
}

CORO_TEST_F(NeoRadosSelfManagedSnaps, Bug11677, NeoRadosTest) {
  std::vector<uint64_t> my_snaps;
  auto ioc = pool();

  co_await new_selfmanaged_snap(rados(), my_snaps, ioc);

  static constexpr auto len = 1 << 20; // 1 MiB
  auto buf = std::make_unique<char[]>(len);
  std::memset(buf.get(), 0xcc, len);

  buffer::list bl1;
  bl1.append(buf.get(), len);
  co_await execute(oid, WriteOp{}.write(0, bl1), ioc);

  co_await new_selfmanaged_snap(rados(), my_snaps, ioc);

  WriteOp op;
  op.assert_exists()
    .remove();
  co_await execute(oid, std::move(op), ioc);

  co_await rm_selfmanaged_snaps(rados(), my_snaps, ioc);

  co_return;
}

CORO_TEST_F(NeoRadosSelfManagedSnaps, OrderSnap, NeoRadosTest) {
  static constexpr auto len = 128u;
  std::vector<uint64_t> my_snaps;
  auto ioc = pool();
  const auto bl = filled_buffer_list(0xcc, len);

  co_await new_selfmanaged_snap(rados(), my_snaps, ioc);
  co_await execute(oid, WriteOp{}.write(0, bl).ordersnap(), ioc);

  co_await new_selfmanaged_snap(rados(), my_snaps, ioc);
  co_await execute(oid, WriteOp{}.write(0, bl).ordersnap(), ioc);

  my_snaps.pop_back();
  std::reverse(my_snaps.begin(), my_snaps.end());
  ioc.set_write_snap_context({{my_snaps[0], my_snaps}});
  std::reverse(my_snaps.begin(), my_snaps.end());

  co_await expect_error_code(execute(oid, WriteOp()
				     .write(0, bl).ordersnap(), ioc),
			     osd_errc::old_snapc);

  co_await execute(oid, WriteOp{}.write(0, bl), ioc);

  co_return;
}

CORO_TEST_F(NeoRadosSelfManagedSnaps, ReusePurgedSnap, NeoRadosTest) {
  static constexpr auto len = 128u;
  std::vector<uint64_t> my_snaps;
  auto ioc = pool();
  const auto bl = filled_buffer_list(0xcc, len);

  co_await new_selfmanaged_snap(rados(), my_snaps, ioc);
  EXPECT_TRUE(rados().get_self_managed_snaps_mode(pool()));
  co_await execute(oid, WriteOp{}.write(0, bl), ioc);

  co_await new_selfmanaged_snap(rados(), my_snaps, ioc);
  std::cout << "Deleting snap " << my_snaps.back() << " in pool "
	    << pool_name() << "." << std::endl;
  co_await rados().delete_selfmanaged_snap(ioc.get_pool(), my_snaps.back(),
					   asio::use_awaitable);
  std::cout << "Waiting for snaps to purge." << std::endl;
  co_await wait_for(15s);
  std::reverse(my_snaps.begin(), my_snaps.end());
  ioc.set_write_snap_context({{my_snaps[0], my_snaps}});
  std::reverse(my_snaps.begin(), my_snaps.end());

  co_await execute(oid, WriteOp()
		   .write(0, filled_buffer_list(0xdd, len)));


  co_return;
}
