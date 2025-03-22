// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "rgw_datalog.h"

#include <string_view>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/system/errc.hpp>
#include <boost/system/error_code.hpp>

#include <fmt/format.h>

#include "include/neorados/RADOS.hpp"

#include "neorados/cls/sem_set.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

namespace asio = boost::asio;
namespace ss = neorados::cls::sem_set;

using neorados::WriteOp;

class DataLogTestBase : public CoroTest {
private:
  const std::string prefix_{std::string{"test framework "} +
			    testing::UnitTest::GetInstance()->
			    current_test_info()->name() +
			    std::string{": "}};

  std::optional<neorados::RADOS> rados_;
  neorados::IOContext pool_;
  const std::string pool_name_ = get_temp_pool_name(
    testing::UnitTest::GetInstance()->current_test_info()->name());
  std::unique_ptr<DoutPrefix> dpp_;

  boost::asio::awaitable<uint64_t> create_pool() {
    co_return co_await ::create_pool(rados(), pool_name(),
				     boost::asio::use_awaitable);
  }

  boost::asio::awaitable<void> clean_pool() {
    co_await rados().delete_pool(pool().get_pool(),
				boost::asio::use_awaitable);
  }

  virtual asio::awaitable<std::unique_ptr<RGWDataChangesLog>>
  create_datalog() = 0;

protected:

  std::unique_ptr<RGWDataChangesLog> datalog;

  neorados::RADOS& rados() noexcept { return *rados_; }
  const std::string& pool_name() const noexcept { return pool_name_; }
  const neorados::IOContext& pool() const noexcept { return pool_; }
  std::string_view prefix() const noexcept { return prefix_; }
  const DoutPrefixProvider* dpp() const noexcept { return dpp_.get(); }
  auto execute(std::string_view oid, neorados::WriteOp&& op,
	       std::uint64_t* ver = nullptr) {
    return rados().execute(oid, pool(), std::move(op),
			   boost::asio::use_awaitable, ver);
  }
  auto execute(std::string_view oid, neorados::ReadOp&& op,
	       std::uint64_t* ver = nullptr) {
    return rados().execute(oid, pool(), std::move(op), nullptr,
			   boost::asio::use_awaitable, ver);
  }
  auto execute(std::string_view oid, neorados::WriteOp&& op,
	       neorados::IOContext ioc, std::uint64_t* ver = nullptr) {
    return rados().execute(oid, std::move(ioc), std::move(op),
			   boost::asio::use_awaitable, ver);
  }
  auto execute(std::string_view oid, neorados::ReadOp&& op,
	       neorados::IOContext ioc, std::uint64_t* ver = nullptr) {
    return rados().execute(oid, std::move(ioc), std::move(op), nullptr,
			   boost::asio::use_awaitable, ver);
  }

  asio::awaitable<void>
  read_all_sems(int index,
		bc::flat_map<std::string, uint64_t>* out) {
    std::string cursor;
    do {
      try {
	co_await rados().execute(
	  datalog->get_sem_set_oid(index), datalog->loc,
	  neorados::ReadOp{}.exec(ss::list(datalog->sem_max_keys, cursor, out,
					   &cursor)),
	  nullptr, asio::use_awaitable);
      } catch (const sys::system_error& e) {
	if (e.code() == sys::errc::no_such_file_or_directory) {
	  break;
	} else {
	  throw;
	}
      }
    } while (!cursor.empty());
    co_return;
  }

  asio::awaitable<bc::flat_map<std::string, uint64_t>>
  read_all_sems_all_shards() {
    bc::flat_map<std::string, uint64_t> all_sems;

    for (auto i = 0; i < datalog->num_shards; ++i) {
      co_await read_all_sems(i, &all_sems);
    }
    co_return std::move(all_sems);
  }

  asio::awaitable<bc::flat_map<BucketGen, uint64_t>>
  read_all_log(const DoutPrefixProvider* dpp) {
    bc::flat_map<BucketGen, uint64_t> all_keys;

    RGWDataChangesLogMarker marker;
    do {
      std::vector<rgw_data_change_log_entry> entries;
      std::tie(entries, marker) =
	co_await datalog->list_entries(dpp, 1'000,
				       std::move(marker));
      for (const auto& entry : entries) {
	auto key = fmt::format("{}:{}", entry.entry.key, entry.entry.gen);
	all_keys[BucketGen{key}] += 1;
      }
    } while (marker);
    co_return std::move(all_keys);
  }

  asio::awaitable<void> add_entry(const DoutPrefixProvider* dpp,
                                  const BucketGen& bg) {
    RGWBucketInfo bi;
    bi.bucket = bg.shard.bucket;
    rgw::bucket_log_layout_generation gen;
    gen.gen = bg.gen;
    co_await datalog->add_entry(dpp, bi, gen, bg.shard.shard_id);
    co_return;
  }

  auto renew_entries(const DoutPrefixProvider* dpp) {
    return datalog->renew_entries(dpp);
  }

  auto oid(const BucketGen& bg) {
    return datalog->get_oid(0, datalog->choose_oid(bg.shard));
  }

  auto sem_set_oid(const BucketGen& bg) {
    return datalog->get_sem_set_oid(datalog->choose_oid(bg.shard));
  }

  auto loc() {
    return datalog->loc;
  }

  auto recover(const DoutPrefixProvider* dpp) {
    return datalog->recover(dpp, nullptr);
  }

  void add_to_cur_cycle(const BucketGen& bg) {
    std::unique_lock l(datalog->lock);
    datalog->cur_cycle.insert(bg);
  }

  void add_to_semaphores(const BucketGen& bg) {
    std::unique_lock l(datalog->lock);
    datalog->semaphores[datalog->choose_oid(bg.shard)].insert(bg.get_key());
  }

public:

  /// \brief Create RADOS handle and pool for the test
  boost::asio::awaitable<void> CoSetUp() override {
    rados_ = co_await neorados::RADOS::Builder{}
      .build(asio_context, boost::asio::use_awaitable);
    dpp_ = std::make_unique<DoutPrefix>(rados().cct(), 0, prefix().data());
    pool_.set_pool(co_await create_pool());
    datalog = co_await create_datalog();
    co_return;
  }

  ~DataLogTestBase() override = default;

  /// \brief Delete pool used for testing
  boost::asio::awaitable<void> CoTearDown() override {
    co_await datalog->shutdown();
    co_await clean_pool();
    co_return;
  }
};

class DataLogTest : public DataLogTestBase {
private:
  asio::awaitable<std::unique_ptr<RGWDataChangesLog>> create_datalog() override {
    auto datalog = std::make_unique<RGWDataChangesLog>(rados().cct(), true,
						       &rados());
    co_await datalog->start(dpp(), rgw_pool(pool_name()), false, true, false);
    co_return std::move(datalog);
  }
};

class DataLogWatchless : public DataLogTestBase {
private:
  asio::awaitable<std::unique_ptr<RGWDataChangesLog>> create_datalog() override {
    auto datalog = std::make_unique<RGWDataChangesLog>(rados().cct(), true,
						       &rados());
    co_await datalog->start(dpp(), rgw_pool(pool_name()), false, false, false);
    co_return std::move(datalog);
  }
};

class DataLogBulky : public DataLogTestBase {
private:
  asio::awaitable<std::unique_ptr<RGWDataChangesLog>> create_datalog() override {
    // Decrease max push/list and force everything into one shard so we
    // can test iterated increment/decrement/list code.
    auto datalog = std::make_unique<RGWDataChangesLog>(rados().cct(), true,
						       &rados(), 1, 7);
    co_await datalog->start(dpp(), rgw_pool(pool_name()), false, true, false);
    co_return std::move(datalog);
  }
};



const std::vector<BucketGen> ref{
  {{{"fred", "foo"}, 32}, 3},
  {{{"fred", "foo"}, 32}, 0},
  {{{"fred", "foo"}, 13}, 0},
  {{{"", "bar"}, 13}, 0},
  {{{"", "bar", "zardoz"}, 11}, 0}};

const auto bulky =
  []() {
    std::vector<BucketGen> ref;
    for (auto i = 0; i < 30; ++i) {
      ref.push_back({{{"", fmt::format("bucket{}", i)}, i}, 0});
      ref.push_back({{{fmt::format("tenant{}", i),
	               fmt::format("bucket{}", i)}, i}, 0});
      ref.push_back({{{fmt::format("tenant{}", i),
	               fmt::format("bucket{}", i),
	               fmt::format("instance{}", i)}, i}, 0});
    }
    return ref;
  }();

TEST(DataLogBG, TestRoundTrip) {
  for (const auto& bg : ref) {
    ASSERT_EQ(bg, BucketGen{bg.get_key()});
  }
}

CORO_TEST_F(DataLog, TestSem, DataLogTest) {
  for (const auto& bg : ref) {
    co_await add_entry(dpp(), bg);
    // Second send adds it to working set and creates the semaphore
    co_await add_entry(dpp(), bg);
    // Third should *not* increment the semaphore again.
    co_await add_entry(dpp(), bg);
  }
  auto sems = co_await read_all_sems_all_shards();
  for (const auto& bg : ref) {
    EXPECT_TRUE(sems.contains(bg.get_key()));
    EXPECT_EQ(1, sems[bg.get_key()]);
  }
  co_await renew_entries(dpp());
  sems.clear();
  sems = co_await read_all_sems_all_shards();
  EXPECT_TRUE(sems.empty());
  const auto log_entries = co_await read_all_log(dpp());
  for (const auto& bg : ref) {
    EXPECT_TRUE(log_entries.contains(bg));
  }
  co_return;
}

CORO_TEST_F(DataLog, SimpleRecovery, DataLogTest) {
  for (const auto& bg : ref) {
    co_await rados().execute(sem_set_oid(bg), loc(),
			     WriteOp{}.exec(ss::increment(bg.get_key())),
			     asio::use_awaitable);
  }
  co_await recover(dpp());
  auto sems = co_await read_all_sems_all_shards();
  EXPECT_TRUE(sems.empty());

  auto log_entries = co_await read_all_log(dpp());
  for (const auto& bg : ref) {
    EXPECT_TRUE(log_entries.contains(bg));
  }

  co_return;
}

CORO_TEST_F(DataLog, CycleRecovery, DataLogTest) {
  for (const auto& bg : ref) {
    co_await rados().execute(sem_set_oid(bg), loc(),
			     WriteOp{}.exec(ss::increment(bg.get_key())),
			     asio::use_awaitable);
  }
  add_to_cur_cycle(ref[0]);
  add_to_cur_cycle(ref[1]);
  co_await recover(dpp());
  auto sems = co_await read_all_sems_all_shards();
  for (const auto& bg : {ref[0], ref[1]}) {
    EXPECT_TRUE(sems.contains(bg.get_key()));
  }
  for (const auto& bg : {ref[2], ref[3], ref[4]}) {
    EXPECT_FALSE(sems.contains(bg.get_key()));
  }

  auto log_entries = co_await read_all_log(dpp());
  for (const auto& bg : ref) {
    EXPECT_TRUE(log_entries.contains(bg));
  }

  co_return;
}

CORO_TEST_F(DataLog, SemaphoresRecovery, DataLogTest) {
  for (const auto& bg : ref) {
    co_await rados().execute(sem_set_oid(bg), loc(),
			     WriteOp{}.exec(ss::increment(bg.get_key())),
			     asio::use_awaitable);
  }
  add_to_semaphores(ref[0]);
  add_to_semaphores(ref[1]);
  co_await recover(dpp());
  auto sems = co_await read_all_sems_all_shards();
  for (const auto& bg : {ref[0], ref[1]}) {
    EXPECT_TRUE(sems.contains(bg.get_key()));
  }
  for (const auto& bg : {ref[2], ref[3], ref[4]}) {
    EXPECT_FALSE(sems.contains(bg.get_key()));
  }

  const auto log_entries = co_await read_all_log(dpp());
  for (const auto& bg : ref) {
    EXPECT_EQ(1, log_entries.at(bg));
  }

  co_return;
}

CORO_TEST_F(DataLogWatchless, NotWatching, DataLogWatchless) {
  for (const auto& bg : ref) {
    co_await add_entry(dpp(), bg);
    // With watch down, we should bypass the data window and get two entries
    co_await add_entry(dpp(), bg);
  }
  auto sems = co_await read_all_sems_all_shards();
  EXPECT_TRUE(sems.empty());
  const auto log_entries = co_await read_all_log(dpp());
  for (const auto& bg : ref) {
    EXPECT_EQ(2, log_entries.at(bg));
  }
  co_return;
}

CORO_TEST_F(DataLogBulky, TestSemBulky, DataLogBulky) {
  for (const auto& bg : bulky) {
    co_await add_entry(dpp(), bg);
    // Second send adds it to working set and creates the semaphore
    co_await add_entry(dpp(), bg);
  }
  auto sems = co_await read_all_sems_all_shards();
  for (const auto& bg : bulky) {
    EXPECT_TRUE(sems.contains(bg.get_key()));
    EXPECT_EQ(1, sems[bg.get_key()]);
  }
  co_await renew_entries(dpp());
  sems.clear();
  sems = co_await read_all_sems_all_shards();
  EXPECT_TRUE(sems.empty());
  const auto log_entries = co_await read_all_log(dpp());
  for (const auto& bg : bulky) {
    EXPECT_TRUE(log_entries.contains(bg));
  }
  co_return;
}

CORO_TEST_F(DataLogBulky, BulkyRecovery, DataLogBulky) {
  for (const auto& bg : bulky) {
    co_await rados().execute(sem_set_oid(bg), loc(),
			     WriteOp{}.exec(ss::increment(bg.get_key())),
			     asio::use_awaitable);
  }
  co_await recover(dpp());
  auto sems = co_await read_all_sems_all_shards();
  EXPECT_TRUE(sems.empty());

  auto log_entries = co_await read_all_log(dpp());
  for (const auto& bg : bulky) {
    EXPECT_TRUE(log_entries.contains(bg));
  }

  co_return;
}

CORO_TEST_F(DataLogBulky, BulkyCycleRecovery, DataLogBulky) {
  for (const auto& bg : bulky) {
    co_await rados().execute(sem_set_oid(bg), loc(),
			     WriteOp{}.exec(ss::increment(bg.get_key())),
			     asio::use_awaitable);
  }
  for (auto i = 0u; i < bulky.size(); ++i) {
    if (i % 2 == 0) {
      add_to_cur_cycle(bulky[i]);
    }
  }
  co_await recover(dpp());
  auto sems = co_await read_all_sems_all_shards();
  for (auto i = 0u; i < bulky.size(); ++i) {
    if (i % 2 == 0) {
      EXPECT_TRUE(sems.contains(bulky[i].get_key()));
    } else {
      EXPECT_FALSE(sems.contains(bulky[i].get_key()));
    }
  }

  auto log_entries = co_await read_all_log(dpp());
  for (const auto& bg : bulky) {
    EXPECT_TRUE(log_entries.contains(bg));
  }
  co_return;
}

CORO_TEST_F(DataLogBulky, BulkySemaphoresRecovery, DataLogBulky) {
  for (const auto& bg : bulky) {
    co_await rados().execute(sem_set_oid(bg), loc(),
			     WriteOp{}.exec(ss::increment(bg.get_key())),
			     asio::use_awaitable);
  }
  for (auto i = 0u; i < bulky.size(); ++i) {
    if (i % 2 == 0) {
      add_to_semaphores(bulky[i]);
    }
  }
  co_await recover(dpp());
  auto sems = co_await read_all_sems_all_shards();
  for (auto i = 0u; i < bulky.size(); ++i) {
    if (i % 2 == 0) {
      EXPECT_TRUE(sems.contains(bulky[i].get_key()));
    } else {
      EXPECT_FALSE(sems.contains(bulky[i].get_key()));
    }
  }

  auto log_entries = co_await read_all_log(dpp());
  for (const auto& bg : bulky) {
    EXPECT_TRUE(log_entries.contains(bg));
  }
  co_return;
}
