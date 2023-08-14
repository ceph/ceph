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
#include <utility>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/system/errc.hpp>

#include "include/neorados/RADOS.hpp"

#include "common/error_code.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace asio = boost::asio;
namespace sys = boost::system;

// We want to be able to test pool functionality, to clean up after
// ourselves, and not create a footgun where someone wipes out their
// entire Ceph cluster by running a test against it.
///
/// So enforce our prefix on everything.
class NeoRadosPool : public CoroTest {
private:
  std::optional<neorados::RADOS> rados_;
  std::unique_ptr<DoutPrefix> dpp_;
  const std::string prefix_{std::string{"test framework "} +
			    testing::UnitTest::GetInstance()->
			    current_test_info()->name() +
			    std::string{": "}};

  /// \brief Return reference to RADOS
  ///
  /// \warning This function should only be called from test bodies
  /// (i.e. after `CoSetUp()`)
  neorados::RADOS& rados() noexcept { return *rados_; }

protected:

  /// \brief Return DoutPrefixProvider*
  ///
  /// \warning This function should only be called from test bodies
  /// (i.e. after `CoSetUp()`)
  const DoutPrefixProvider* dpp() const noexcept { return dpp_.get(); }

  /// \brief Return prefix for this test run
  std::string_view prefix() const noexcept { return prefix_; }

  /// \brief Get a name suitable for a temporary pool
  std::string get_temp_pool_name() const {
    return ::get_temp_pool_name(prefix());
  }

  // Create a pool, ensure it has our prefix
  asio::awaitable<std::int64_t>
  create_pool(std::string pname,std::optional<int> crush_rule = std::nullopt) {
    assert(pname.starts_with(prefix()));
    co_await rados().create_pool(pname, crush_rule, asio::use_awaitable);
    co_return co_await rados().lookup_pool(pname, asio::use_awaitable);
  }

  auto lookup_pool(std::string pname) {
    assert(pname.starts_with(prefix()));
    return rados().lookup_pool(pname, asio::use_awaitable);
  }

  auto delete_pool(std::string pname) {
    assert(pname.starts_with(prefix()));
    return rados().delete_pool(pname, asio::use_awaitable);
  }
  auto delete_pool(std::int64_t pid) {
    return rados().delete_pool(pid, asio::use_awaitable);
  }

  asio::awaitable<std::vector<std::pair<std::int64_t, std::string>>>
  list_pools() {
    auto pools = co_await rados().list_pools(asio::use_awaitable);
    std::erase_if(pools, [this](const std::pair<std::int64_t, std::string>& p) {
      return !p.second.starts_with(prefix());
    });
    co_return pools;
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
    auto pools = co_await rados().list_pools(asio::use_awaitable);
    for (const auto& [id, name] : pools) {
      co_await delete_pool(id);
    }
    co_return;
  }
};

CORO_TEST_F(NeoRadosPools, PoolList, NeoRadosPool) {
  const auto pname = get_temp_pool_name();
  co_await create_pool(pname);
  auto pools = co_await list_pools();
  EXPECT_EQ(1, pools.size());
  EXPECT_EQ(pname, pools.front().second);
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
