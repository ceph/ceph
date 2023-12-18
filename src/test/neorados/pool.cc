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

#include <boost/system/detail/errc.hpp>
#include <coroutine>
#include <cstdint>
#include <unordered_set>
#include <utility>

#include <fmt/format.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/system/errc.hpp>

#include "include/neorados/RADOS.hpp"

#include "common/error_code.h"

#include "osdc/error_code.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace asio = boost::asio;
namespace sys = boost::system;

using namespace std::literals;

// We want to be able to test pool functionality, to clean up after
// ourselves, and not create a footgun where someone wipes out their
// entire Ceph cluster by running a test against it. So track all
// pools we create during a test and remove them after.
class NeoRadosPool : public CoroTest {
private:
  std::optional<neorados::RADOS> rados_;
  std::unique_ptr<DoutPrefix> dpp_;
  const std::string prefix_{
    fmt::format("Test framework: {}: ",
		testing::UnitTest::GetInstance()->
		current_test_info()->name())};

  std::unordered_set<std::string> created_pools;

protected:

  /// \brief Return reference to RADOS
  ///
  /// \warning This function should only be called from test bodies
  /// (i.e. after `CoSetUp()`)
  neorados::RADOS& rados() noexcept { return *rados_; }

  /// \brief Return DoutPrefixProvider*
  ///
  /// \warning This function should only be called from test bodies
  /// (i.e. after `CoSetUp()`)
  const DoutPrefixProvider* dpp() const noexcept { return dpp_.get(); }

  /// \brief Return prefix for this test run
  std::string_view prefix() const noexcept { return prefix_; }

  auto lookup_pool(std::string pname) {
    return rados().lookup_pool(pname, asio::use_awaitable);
  }

  // Create a pool and track it
  asio::awaitable<std::int64_t>
  create_pool(std::string pname, std::optional<int> crush_rule = std::nullopt) {
    co_await rados().create_pool(pname, crush_rule, asio::use_awaitable);
    created_pools.insert(pname);
    co_return co_await lookup_pool(pname);
  }

  auto delete_pool(std::string pname) {
    return rados().delete_pool(pname, asio::use_awaitable);
  }

  auto delete_pool(std::int64_t pid) {
    return rados().delete_pool(pid, asio::use_awaitable);
  }

public:

  /// \brief Create RADOS handle for the test
  boost::asio::awaitable<void> CoSetUp() override {
    rados_ = co_await neorados::RADOS::Builder{}
      .build(asio_context, boost::asio::use_awaitable);
    dpp_ = std::make_unique<DoutPrefix>(rados().cct(), 0, "NeoRadosPoolTest");
    co_return;
  }

  ~NeoRadosPool() override = default;

  /// \brief Delete pool used for testing
  boost::asio::awaitable<void> CoTearDown() override {
    for (const auto& name : created_pools) try {
	co_await delete_pool(name);
      } catch (const sys::system_error& e) {
	if (e.code() != osdc_errc::pool_dne) {
	  throw;
	}
      }
    co_return;
  }
};

CORO_TEST_F(NeoRadosPools, PoolList, NeoRadosPool) {
  const auto pname = get_temp_pool_name();
  co_await create_pool(pname);
  auto pools = co_await rados().list_pools(asio::use_awaitable);
  EXPECT_FALSE(pools.empty());
  EXPECT_TRUE(
    std::find_if(pools.begin(),
		 pools.end(),
		 [&pname](const auto& kv) {return kv.second == pname;})
    != pools.end());
  co_return;
}

CORO_TEST_F(NeoRadosPools, PoolLookup, NeoRadosPool) {
  const auto pname = get_temp_pool_name();
  const auto refpid = co_await create_pool(pname);
  auto respid = co_await lookup_pool(pname);
  EXPECT_EQ(refpid, respid);
  co_return;
}

CORO_TEST_F(NeoRadosPools, PoolLookupOtherInstance, NeoRadosPool) {
  auto rados2 = co_await neorados::RADOS::Builder{}
    .build(asio_context, asio::use_awaitable);
  const auto pname = get_temp_pool_name();
  const auto refpid = co_await create_pool(pname);
  auto respid = co_await rados2.lookup_pool(pname, asio::use_awaitable);
  EXPECT_EQ(refpid, respid);
  co_return;
}

CORO_TEST_F(NeoRadosPools, PoolDelete, NeoRadosPool) {
  const auto pname = get_temp_pool_name();
  co_await create_pool(pname);
  co_await delete_pool(pname);
  co_await expect_error_code(lookup_pool(pname),
			     sys::errc::no_such_file_or_directory);
  co_await create_pool(pname);
  co_return;
}

CORO_TEST_F(NeoRadosPools, PoolCreateDelete, NeoRadosPool) {
  const auto pname = get_temp_pool_name();
  co_await create_pool(pname);
  co_await expect_error_code(create_pool(pname), ceph::errc::exists);
  co_await delete_pool(pname);
  co_await expect_error_code(delete_pool(pname), ceph::errc::does_not_exist);

  co_return;
}

CORO_TEST_F(NeoRadosPools, PoolCreateWithCrushRule, NeoRadosPool) {
  const auto pname = get_temp_pool_name();
  co_await create_pool(pname, 0);
  co_await delete_pool(pname);
  co_return;
}
