// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
#include "rgw_log_backing.h"

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
      std::tie(entries, marker, std::ignore) =
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
    return datalog->recover(dpp);
  }

  void add_to_cur_cycle(const BucketGen& bg) {
    std::unique_lock l(datalog->lock);
    datalog->cur_cycle.insert(bg);
  }

  void add_to_semaphores(const BucketGen& bg) {
    std::unique_lock l(datalog->lock);
    datalog->semaphores[datalog->choose_oid(bg.shard)].insert(bg.get_key());
  }

  // Per-zone helpers (friendship doesn't inherit, so derived classes
  // must use these base-class methods to access private members)
  void add_to_zone_semaphores(const rgw_zone_id& zone, const BucketGen& bg) {
    std::unique_lock l(datalog->lock);
    auto it = datalog->zone_logs.find(zone);
    if (it != datalog->zone_logs.end()) {
      it->second.semaphores[datalog->choose_oid(bg.shard)].insert(bg.get_key());
    }
  }

  auto zone_sem_set_oid(const rgw_zone_id& zone, const BucketGen& bg) {
    return datalog->get_sem_set_oid(zone, datalog->choose_oid(bg.shard));
  }

  void set_legacy_writes_disabled(bool val) {
    datalog->legacy_writes_disabled_ = val;
  }

  // Set target zone IDs for per-zone datalog testing (accesses private
  // member via DataLogTestBase friendship with RGWDataChangesLog)
  static void set_target_zone_ids(RGWDataChangesLog& dl,
                                 std::vector<rgw_zone_id> ids) {
    dl.target_zone_ids_ = std::move(ids);
  }

  // Initialize per-zone backends in a test coroutine context.
  // Unlike the production init_zone_backends() which uses
  // async::use_blocked (and would deadlock inside a coroutine on the
  // same executor), this version uses co_await directly.
  static asio::awaitable<void>
  init_test_zone_backends(const DoutPrefixProvider* dpp_,
                         RGWDataChangesLog& dl) {
    auto defbacking = to_log_type(
      dl.cct->_conf.get_val<std::string>("rgw_default_data_log_backing"));
    ceph_assert(defbacking);
    for (const auto& zone_id : dl.target_zone_ids_) {
      auto zone_bes = co_await logback_generations::init<DataLogBackends>(
       dpp_, *dl.rados, dl.metadata_log_oid(zone_id), dl.loc,
       [&dl, zone_id](uint64_t gen_id, int shard) {
         return dl.get_oid(zone_id, gen_id, shard);
       }, dl.num_shards, *defbacking, dl, zone_id);
      ZoneLog zlog;
      zlog.zone_id = zone_id;
      zlog.bes = std::move(zone_bes);
      zlog.semaphores.resize(dl.num_shards);
      dl.zone_logs.emplace(zone_id, std::move(zlog));
    }
  }

  asio::awaitable<bc::flat_map<std::string, uint64_t>>
  read_all_zone_sems(const rgw_zone_id& zone) {
    bc::flat_map<std::string, uint64_t> all_sems;
    for (auto i = 0; i < datalog->num_shards; ++i) {
      std::string cursor;
      do {
       try {
         co_await rados().execute(
           datalog->get_sem_set_oid(zone, i), datalog->loc,
           neorados::ReadOp{}.exec(ss::list(datalog->sem_max_keys, cursor,
                                            &all_sems, &cursor)),
           nullptr, asio::use_awaitable);
       } catch (const sys::system_error& e) {
         if (e.code() == sys::errc::no_such_file_or_directory) {
           break;
         } else {
           throw;
         }
       }
      } while (!cursor.empty());
    }
    co_return std::move(all_sems);
  }

  asio::awaitable<bc::flat_map<BucketGen, uint64_t>>
  read_all_zone_log(const DoutPrefixProvider* dpp, const rgw_zone_id& zone) {
    bc::flat_map<BucketGen, uint64_t> all_keys;
    for (auto shard = 0; shard < datalog->num_shards; ++shard) {
      std::string marker;
      bool truncated = true;
      while (truncated) {
       auto [entries, outmarker, trunc] =
         co_await datalog->list_entries(dpp, zone, shard, 1'000, marker);
       truncated = trunc;
       marker = std::move(outmarker);
       for (const auto& entry : entries) {
         auto key = fmt::format("{}:{}", entry.entry.key, entry.entry.gen);
         all_keys[BucketGen{key}] += 1;
       }
      }
    }
    co_return std::move(all_keys);
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
    co_await datalog->async_shutdown();
    co_await clean_pool();
    co_return;
  }
};

class DataLogTest : public DataLogTestBase {
private:
  asio::awaitable<std::unique_ptr<RGWDataChangesLog>> create_datalog() override {
    auto datalog = std::make_unique<RGWDataChangesLog>(rados().cct(), true,
						       rados());
    co_await datalog->start(dpp(), rgw_pool(pool_name()),
                           false, true, false);
    co_return std::move(datalog);
  }
};

class DataLogWatchless : public DataLogTestBase {
private:
  asio::awaitable<std::unique_ptr<RGWDataChangesLog>> create_datalog() override {
    auto datalog = std::make_unique<RGWDataChangesLog>(rados().cct(), true,
						       rados());
    co_await datalog->start(dpp(), rgw_pool(pool_name()),
                           false, false, false);
    co_return std::move(datalog);
  }
};

class DataLogBulky : public DataLogTestBase {
private:
  asio::awaitable<std::unique_ptr<RGWDataChangesLog>> create_datalog() override {
    // Decrease max push/list and force everything into one shard so we
    // can test iterated increment/decrement/list code.
    auto datalog = std::make_unique<RGWDataChangesLog>(rados().cct(), true,
						       rados(), 1, 7);
    co_await datalog->start(dpp(), rgw_pool(pool_name()),
                           false, true, false);
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

// --- Multi-zone datalog tests ---

static const rgw_zone_id zone_a{"zone-a-id"};
static const rgw_zone_id zone_b{"zone-b-id"};

class DataLogMultiZone : public DataLogTestBase {
private:
  asio::awaitable<std::unique_ptr<RGWDataChangesLog>> create_datalog() override {
    auto datalog = std::make_unique<RGWDataChangesLog>(rados().cct(), true,
                                                      rados());
    set_target_zone_ids(*datalog, {zone_a, zone_b});
    co_await datalog->start(dpp(), rgw_pool(pool_name()),
                           false, true, false);
    co_await init_test_zone_backends(dpp(), *datalog);
    co_return std::move(datalog);
  }

  asio::awaitable<void> CoSetUp() override {
    co_await DataLogTestBase::CoSetUp();
    // Enable per-zone writes (feature enabled = legacy writes disabled)
    set_legacy_writes_disabled(true);
  }
};

// Verify that get_zone_ids() returns the configured zones
CORO_TEST_F(DataLogMultiZone, ZoneIds, DataLogMultiZone) {
  auto ids = datalog->get_zone_ids();
  ASSERT_EQ(2u, ids.size());
  // map iteration order is sorted by key
  std::sort(ids.begin(), ids.end(),
           [](const auto& a, const auto& b) { return a.id < b.id; });
  EXPECT_EQ(zone_a, ids[0]);
  EXPECT_EQ(zone_b, ids[1]);
  co_return;
}

// Verify add_entry writes to per-zone backends only (not legacy) when
// the per_zone_datalog feature is enabled.
CORO_TEST_F(DataLogMultiZone, PerZoneWriteOnly, DataLogMultiZone) {
  for (const auto& bg : ref) {
    co_await add_entry(dpp(), bg);
    co_await add_entry(dpp(), bg);
  }
  co_await renew_entries(dpp());

  // Legacy log should be empty (feature enabled = legacy writes disabled)
  auto legacy_entries = co_await read_all_log(dpp());
  EXPECT_TRUE(legacy_entries.empty())
    << "Legacy log should be empty when per_zone_datalog is enabled";

  // Verify zone_a log has entries
  auto zone_a_entries = co_await read_all_zone_log(dpp(), zone_a);
  for (const auto& bg : ref) {
    EXPECT_TRUE(zone_a_entries.contains(bg))
      << "Zone A log missing entry for " << bg;
  }

  // Verify zone_b log has entries
  auto zone_b_entries = co_await read_all_zone_log(dpp(), zone_b);
  for (const auto& bg : ref) {
    EXPECT_TRUE(zone_b_entries.contains(bg))
      << "Zone B log missing entry for " << bg;
  }
  co_return;
}

// Verify that semaphores are created for per-zone backends only
// (legacy writes are disabled in DataLogMultiZone)
CORO_TEST_F(DataLogMultiZone, PerZoneSemaphores, DataLogMultiZone) {
  for (const auto& bg : ref) {
    co_await add_entry(dpp(), bg);
    co_await add_entry(dpp(), bg);
    co_await add_entry(dpp(), bg);
  }

  // Legacy semaphores should be empty (legacy writes disabled)
  auto legacy_sems = co_await read_all_sems_all_shards();
  EXPECT_TRUE(legacy_sems.empty())
    << "Legacy semaphores should be empty when legacy writes are disabled";

  // Per-zone semaphores should be populated
  auto zone_a_sems = co_await read_all_zone_sems(zone_a);
  for (const auto& bg : ref) {
    EXPECT_TRUE(zone_a_sems.contains(bg.get_key()))
      << "Zone A semaphore missing for " << bg;
    EXPECT_EQ(1, zone_a_sems[bg.get_key()]);
  }

  auto zone_b_sems = co_await read_all_zone_sems(zone_b);
  for (const auto& bg : ref) {
    EXPECT_TRUE(zone_b_sems.contains(bg.get_key()))
      << "Zone B semaphore missing for " << bg;
    EXPECT_EQ(1, zone_b_sems[bg.get_key()]);
  }

  // After renew, per-zone semaphores should be cleared
  co_await renew_entries(dpp());
  legacy_sems = co_await read_all_sems_all_shards();
  EXPECT_TRUE(legacy_sems.empty());
  zone_a_sems = co_await read_all_zone_sems(zone_a);
  EXPECT_TRUE(zone_a_sems.empty());
  zone_b_sems = co_await read_all_zone_sems(zone_b);
  EXPECT_TRUE(zone_b_sems.empty());
  co_return;
}

// Verify per-zone list_entries returns zone-specific entries
CORO_TEST_F(DataLogMultiZone, PerZoneListEntries, DataLogMultiZone) {
  for (const auto& bg : ref) {
    co_await add_entry(dpp(), bg);
    co_await add_entry(dpp(), bg);
  }
  co_await renew_entries(dpp());

  // Each zone should have entries for all ref entries
  for (const auto& zone : {zone_a, zone_b}) {
    auto entries = co_await read_all_zone_log(dpp(), zone);
    EXPECT_EQ(ref.size(), entries.size())
      << "Zone " << zone << " has wrong number of entries";
    for (const auto& bg : ref) {
      EXPECT_TRUE(entries.contains(bg))
       << "Zone " << zone << " missing entry for " << bg;
    }
  }
  co_return;
}

// Verify per-zone trim only affects the specified zone
CORO_TEST_F(DataLogMultiZone, PerZoneTrimIndependent, DataLogMultiZone) {
  for (const auto& bg : ref) {
    co_await add_entry(dpp(), bg);
    co_await add_entry(dpp(), bg);
  }
  co_await renew_entries(dpp());

  // Get the current marker for zone_a shard 0
  auto zone_a_info = co_await datalog->get_info(dpp(), zone_a, 0);
  if (!zone_a_info.marker.empty()) {
    // Trim zone_a shard 0 up to its current marker
    co_await datalog->trim_entries(dpp(), zone_a, 0, zone_a_info.marker);
  }

  // Zone_a shard 0 should be trimmed (list returns empty or fewer entries)
  auto [zone_a_entries, zone_a_marker, zone_a_trunc] =
    co_await datalog->list_entries(dpp(), zone_a, 0, 1'000, {});

  // Zone_b should still have all entries (untouched by zone_a trim)
  auto zone_b_entries = co_await read_all_zone_log(dpp(), zone_b);
  for (const auto& bg : ref) {
    EXPECT_TRUE(zone_b_entries.contains(bg))
      << "Zone B should still have entry for " << bg << " after zone A trim";
  }

  // Legacy writes are disabled in DataLogMultiZone, so legacy log should
  // remain empty and be unaffected by per-zone trims
  auto legacy_entries = co_await read_all_log(dpp());
  EXPECT_TRUE(legacy_entries.empty())
    << "Legacy datalog should remain empty when legacy writes are disabled";
  co_return;
}

// Verify per-zone get_info works
CORO_TEST_F(DataLogMultiZone, PerZoneGetInfo, DataLogMultiZone) {
  for (const auto& bg : ref) {
    co_await add_entry(dpp(), bg);
    co_await add_entry(dpp(), bg);
  }
  co_await renew_entries(dpp());

  // get_info for each zone should return valid info
  for (const auto& zone : {zone_a, zone_b}) {
    bool found_nonempty = false;
    for (auto i = 0; i < datalog->num_shards; ++i) {
      auto info = co_await datalog->get_info(dpp(), zone, i);
      if (!info.marker.empty()) {
       found_nonempty = true;
      }
    }
    EXPECT_TRUE(found_nonempty)
      << "Zone " << zone << " should have at least one non-empty shard";
  }
  co_return;
}

// Verify per-zone recovery works
CORO_TEST_F(DataLogMultiZone, PerZoneRecovery, DataLogMultiZone) {
  // Manually add semaphores to per-zone sem_sets (simulating crash)
  for (const auto& bg : ref) {
    co_await rados().execute(zone_sem_set_oid(zone_a, bg), loc(),
                            WriteOp{}.exec(ss::increment(bg.get_key())),
                            asio::use_awaitable);
    co_await rados().execute(zone_sem_set_oid(zone_b, bg), loc(),
                            WriteOp{}.exec(ss::increment(bg.get_key())),
                            asio::use_awaitable);
    // Also add to legacy sem_set for completeness
    co_await rados().execute(sem_set_oid(bg), loc(),
                            WriteOp{}.exec(ss::increment(bg.get_key())),
                            asio::use_awaitable);
  }

  co_await recover(dpp());

  // All semaphores should be cleared after recovery
  auto legacy_sems = co_await read_all_sems_all_shards();
  EXPECT_TRUE(legacy_sems.empty()) << "Legacy semaphores not cleared";

  auto zone_a_sems = co_await read_all_zone_sems(zone_a);
  EXPECT_TRUE(zone_a_sems.empty()) << "Zone A semaphores not cleared";

  auto zone_b_sems = co_await read_all_zone_sems(zone_b);
  EXPECT_TRUE(zone_b_sems.empty()) << "Zone B semaphores not cleared";

  // All backends should have recovered entries
  auto legacy_entries = co_await read_all_log(dpp());
  for (const auto& bg : ref) {
    EXPECT_TRUE(legacy_entries.contains(bg))
      << "Legacy missing recovered entry for " << bg;
  }

  auto zone_a_entries = co_await read_all_zone_log(dpp(), zone_a);
  for (const auto& bg : ref) {
    EXPECT_TRUE(zone_a_entries.contains(bg))
      << "Zone A missing recovered entry for " << bg;
  }

  auto zone_b_entries = co_await read_all_zone_log(dpp(), zone_b);
  for (const auto& bg : ref) {
    EXPECT_TRUE(zone_b_entries.contains(bg))
      << "Zone B missing recovered entry for " << bg;
  }
  co_return;
}

// --- Legacy writes disabled tests ---

class DataLogLegacyDisabled : public DataLogTestBase {
private:
  asio::awaitable<std::unique_ptr<RGWDataChangesLog>> create_datalog() override {
    auto datalog = std::make_unique<RGWDataChangesLog>(rados().cct(), true,
                                                      rados());
    set_target_zone_ids(*datalog, {zone_a, zone_b});
    co_await datalog->start(dpp(), rgw_pool(pool_name()),
                           false, true, false);
    co_await init_test_zone_backends(dpp(), *datalog);
    co_return std::move(datalog);
  }

  // Called after base CoSetUp creates the datalog
  asio::awaitable<void> CoSetUp() override {
    co_await DataLogTestBase::CoSetUp();
    // Simulate per_zone_datalog feature enabled
    set_legacy_writes_disabled(true);
  }
};

// When legacy writes are disabled, entries should only appear in per-zone logs
CORO_TEST_F(DataLogLegacyDisabled, NoLegacyWrites, DataLogLegacyDisabled) {
  for (const auto& bg : ref) {
    co_await add_entry(dpp(), bg);
    co_await add_entry(dpp(), bg);
  }
  co_await renew_entries(dpp());

  // Legacy log should be empty (no writes)
  auto legacy_entries = co_await read_all_log(dpp());
  EXPECT_TRUE(legacy_entries.empty())
    << "Legacy log should be empty when legacy writes are disabled";

  // Per-zone logs should still have entries
  auto zone_a_entries = co_await read_all_zone_log(dpp(), zone_a);
  for (const auto& bg : ref) {
    EXPECT_TRUE(zone_a_entries.contains(bg))
      << "Zone A should have entry for " << bg;
  }

  auto zone_b_entries = co_await read_all_zone_log(dpp(), zone_b);
  for (const auto& bg : ref) {
    EXPECT_TRUE(zone_b_entries.contains(bg))
      << "Zone B should have entry for " << bg;
  }
  co_return;
}

// Verify invalid zone_id throws
CORO_TEST_F(DataLogMultiZone, InvalidZoneThrows, DataLogMultiZone) {
  rgw_zone_id bad_zone{"nonexistent-zone"};
  bool caught = false;
  try {
    co_await datalog->list_entries(dpp(), bad_zone, 0, 100, {});
  } catch (const sys::system_error& e) {
    caught = true;
    EXPECT_EQ(ENOENT, e.code().value());
  }
  EXPECT_TRUE(caught) << "Expected ENOENT for invalid zone_id";
  co_return;
}
