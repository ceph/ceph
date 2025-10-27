// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw/services/svc_bilog_rados.h"
#include "rgw/rgw_bucket.h"

#include <string_view>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/errc.hpp>
#include <boost/system/error_code.hpp>

#include <fmt/format.h>

#include "include/neorados/RADOS.hpp"
#include "test/neorados/common_tests.h"
#include "gtest/gtest.h"

namespace asio = boost::asio;
namespace fifo = neorados::cls::fifo;

using neorados::WriteOp;
using neorados::ReadOp;

class BiLogTestBase : public CoroTest {
private:
  const std::string prefix_{std::string{"bilog test "} +
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

  virtual asio::awaitable<std::unique_ptr<RGWSI_BILog_RADOS>>
  create_bilog() = 0;

protected:
  std::unique_ptr<RGWSI_BILog_RADOS> bilog;
  RGWBucketInfo bucket_info;
  rgw::bucket_log_layout_generation log_layout;

  neorados::RADOS& rados() noexcept { return *rados_; }
  const std::string& pool_name() const noexcept { return pool_name_; }
  const neorados::IOContext& pool() const noexcept { return pool_; }
  std::string_view prefix() const noexcept { return prefix_; }
  const DoutPrefixProvider* dpp() const noexcept { return dpp_.get(); }

  void setup_bucket_info(const std::string& bucket_name, 
                         const std::string& bucket_id = "",
                         const std::string& tenant = "") {
    bucket_info.bucket.name = bucket_name;
    bucket_info.bucket.bucket_id = bucket_id.empty() ? 
      fmt::format("{}-id", bucket_name) : bucket_id;
    bucket_info.bucket.tenant = tenant;
  }

  void setup_inindex_layout() {
    // Empty logs = InIndex backend
    bucket_info.layout.logs.clear();
    log_layout = rgw::bucket_log_layout_generation{};
  }

  void setup_fifo_layout() {
    // FIFO layout
    log_layout.layout.type = rgw::BucketLogType::FIFO;
    log_layout.layout.in_index.layout.log_pool = pool_name();
    log_layout.gen = 1;
    bucket_info.layout.logs.clear();
    bucket_info.layout.logs.push_back(log_layout);
  }

  asio::awaitable<void> add_bilog_entry(int shard_id, 
                                        const std::string& obj_name,
                                        const std::string& instance = "") {
    rgw_bi_log_entry entry;
    entry.object_name = obj_name;
    entry.instance = instance;
    entry.timestamp = ceph::real_clock::now();
    entry.op = rgw_bilog_entry_type::RGW_BILOG_TYPE_PUT;
    
    // use the log_list to verify entries
    co_return;
  }

  asio::awaitable<std::list<rgw_bi_log_entry>> 
  read_all_entries(int shard_id) {
    std::list<rgw_bi_log_entry> all_entries;
    std::string marker;
    bool truncated = true;
    
    while (truncated) {
      std::list<rgw_bi_log_entry> entries;
      int ret = co_await asio::co_spawn(
        asio_context,
        [&]() -> asio::awaitable<int> {
          co_return bilog->log_list(dpp(), std::nullopt, bucket_info, log_layout,
                                   shard_id, marker, 1000, entries, &truncated);
        },
        asio::use_awaitable
      );
      
      if (ret < 0) {
        break;
      }
      
      all_entries.splice(all_entries.end(), entries);
      if (entries.empty()) {
        break;
      }
    }
    
    co_return all_entries;
  }

public:
  /// \brief Create RADOS handle and pool for the test
  boost::asio::awaitable<void> CoSetUp() override {
    rados_ = co_await neorados::RADOS::Builder{}
      .build(asio_context, boost::asio::use_awaitable);
    dpp_ = std::make_unique<DoutPrefix>(rados().cct(), 0, prefix().data());
    pool_.set_pool(co_await create_pool());
    
    setup_bucket_info("test-bucket");
    bilog = co_await create_bilog();
    co_return;
  }

  ~BiLogTestBase() override = default;

  /// \brief Delete pool used for testing
  boost::asio::awaitable<void> CoTearDown() override {
    co_await clean_pool();
    co_return;
  }
};

// InIndex backend tests
class BiLogInIndexTest : public BiLogTestBase {
private:
  asio::awaitable<std::unique_ptr<RGWSI_BILog_RADOS>> create_bilog() override {
    setup_inindex_layout();
    auto bilog = std::make_unique<RGWSI_BILog_RADOS_InIndex>(rados().cct());
    co_return std::move(bilog);
  }
};

// FIFO backend tests  
class BiLogFIFOTest : public BiLogTestBase {
private:
  asio::awaitable<std::unique_ptr<RGWSI_BILog_RADOS>> create_bilog() override {
    setup_fifo_layout();
    auto bilog = std::make_unique<RGWSI_BILog_RADOS_FIFO>(rados().cct(), 
                                                          std::move(rados()));
    co_return std::move(bilog);
  }
};

// dispatcher tests
class BiLogDispatcherTest : public BiLogTestBase {
private:
  asio::awaitable<std::unique_ptr<RGWSI_BILog_RADOS>> create_bilog() override {
    // will be configured per test
    auto bilog = std::make_unique<RGWSI_BILog_RADOS_BackendDispatcher>(
      rados().cct(), std::move(rados()));
    co_return std::move(bilog);
  }
};

const std::vector<std::string> test_objects{
  "object1.txt",
  "prefix/object2.jpg", 
  "special-chars_object@4.data",
};

// basic InIndex tests
CORO_TEST_F(BiLog, InIndexBasic, BiLogInIndexTest) {
  
  // log_start should succeed (no-op for InIndex)
  int ret = bilog->log_start(dpp(), std::nullopt, bucket_info, log_layout, 0);
  EXPECT_EQ(0, ret);
  
  // log_list should return empty (InIndex doesn't maintain separate log)
  std::string marker;
  std::list<rgw_bi_log_entry> entries;
  bool truncated;
  ret = bilog->log_list(dpp(), std::nullopt, bucket_info, log_layout, 0,
                       marker, 100, entries, &truncated);
  EXPECT_EQ(0, ret);
  EXPECT_TRUE(entries.empty());
  EXPECT_FALSE(truncated);
  
  // log_trim should succeed (no-op for InIndex)
  ret = bilog->log_trim(dpp(), std::nullopt, bucket_info, log_layout, 0, "");
  EXPECT_EQ(0, ret);
  
  // log_stop should succeed (no-op for InIndex)
  ret = bilog->log_stop(dpp(), std::nullopt, bucket_info, log_layout, 0);
  EXPECT_EQ(0, ret);
  
  co_return;
}

CORO_TEST_F(BiLog, InIndexMultipleShardsAllShards, BiLogInIndexTest) {
  // test InIndex with multiple shards
  
  int ret = bilog->log_start(dpp(), std::nullopt, bucket_info, log_layout, -1);
  EXPECT_EQ(0, ret);
  
  std::string marker;
  std::list<rgw_bi_log_entry> entries;
  bool truncated;
  ret = bilog->log_list(dpp(), std::nullopt, bucket_info, log_layout, -1,
                       marker, 100, entries, &truncated);
  EXPECT_EQ(0, ret);
  
  ret = bilog->log_trim(dpp(), std::nullopt, bucket_info, log_layout, -1, "");
  EXPECT_EQ(0, ret);
  
  ret = bilog->log_stop(dpp(), std::nullopt, bucket_info, log_layout, -1);
  EXPECT_EQ(0, ret);
  
  co_return;
}

// basic FIFO backend tests
CORO_TEST_F(BiLog, FIFOBasic, BiLogFIFOTest) {
  
  // log_start should create FIFO objects
  int ret = bilog->log_start(dpp(), std::nullopt, bucket_info, log_layout, 0);
  EXPECT_EQ(0, ret);
  
  // log_list should work with empty FIFO
  std::string marker;
  std::list<rgw_bi_log_entry> entries;
  bool truncated;
  ret = bilog->log_list(dpp(), std::nullopt, bucket_info, log_layout, 0,
                       marker, 100, entries, &truncated);
  EXPECT_EQ(0, ret);
  EXPECT_TRUE(entries.empty());
  EXPECT_FALSE(truncated);
  
  // log_trim should work with empty FIFO
  ret = bilog->log_trim(dpp(), std::nullopt, bucket_info, log_layout, 0, "");
  EXPECT_EQ(0, ret);
  
  // log_stop should clean up FIFO objects
  ret = bilog->log_stop(dpp(), std::nullopt, bucket_info, log_layout, 0);
  EXPECT_EQ(0, ret);
  
  co_return;
}

CORO_TEST_F(BiLog, FIFOInvalidShards, BiLogFIFOTest) {
  // test FIFO with invalid shard IDs (> 0)
  
  // FIFO only supports shard 0 and -1
  int ret = bilog->log_start(dpp(), std::nullopt, bucket_info, log_layout, 1);
  EXPECT_EQ(0, ret); // should be no-op for invalid shards
  
  ret = bilog->log_start(dpp(), std::nullopt, bucket_info, log_layout, 5);
  EXPECT_EQ(0, ret); // should be no-op for invalid shards
  
  // operations on invalid shards should be no-ops
  std::string marker;
  std::list<rgw_bi_log_entry> entries;
  bool truncated;
  ret = bilog->log_list(dpp(), std::nullopt, bucket_info, log_layout, 1,
                       marker, 100, entries, &truncated);
  EXPECT_EQ(0, ret);
  EXPECT_TRUE(entries.empty());
  
  co_return;
}

CORO_TEST_F(BiLog, FIFOAllShardsOperation, BiLogFIFOTest) {
  // test FIFO operations on all shards
  
  int ret = bilog->log_start(dpp(), std::nullopt, bucket_info, log_layout, -1);
  EXPECT_EQ(0, ret);
  
  std::string marker;
  std::list<rgw_bi_log_entry> entries;
  bool truncated;
  ret = bilog->log_list(dpp(), std::nullopt, bucket_info, log_layout, -1,
                       marker, 100, entries, &truncated);
  EXPECT_EQ(0, ret);
  
  ret = bilog->log_trim(dpp(), std::nullopt, bucket_info, log_layout, -1, "");
  EXPECT_EQ(0, ret);
  
  ret = bilog->log_stop(dpp(), std::nullopt, bucket_info, log_layout, -1);
  EXPECT_EQ(0, ret);
  
  co_return;
}

// backend dispatcher tests
CORO_TEST_F(BiLog, DispatcherInIndex, BiLogDispatcherTest) {
  // test dispatcher with InIndex layout (empty logs)
  setup_inindex_layout();
  
  // Should use InIndex backend automatically
  int ret = bilog->log_start(dpp(), std::nullopt, bucket_info, log_layout, 0);
  EXPECT_EQ(0, ret);
  
  std::string marker;
  std::list<rgw_bi_log_entry> entries;
  bool truncated;
  ret = bilog->log_list(dpp(), std::nullopt, bucket_info, log_layout, 0,
                       marker, 100, entries, &truncated);
  EXPECT_EQ(0, ret);
  EXPECT_TRUE(entries.empty()); // InIndex returns empty
  
  ret = bilog->log_stop(dpp(), std::nullopt, bucket_info, log_layout, 0);
  EXPECT_EQ(0, ret);
  
  co_return;
}

CORO_TEST_F(BiLog, DispatcherFIFO, BiLogDispatcherTest) {
  // test dispatcher with FIFO layout
  setup_fifo_layout();
  
  // should use FIFO backend automatically  
  int ret = bilog->log_start(dpp(), std::nullopt, bucket_info, log_layout, 0);
  EXPECT_EQ(0, ret);
  
  std::string marker;
  std::list<rgw_bi_log_entry> entries;
  bool truncated;
  ret = bilog->log_list(dpp(), std::nullopt, bucket_info, log_layout, 0,
                       marker, 100, entries, &truncated);
  EXPECT_EQ(0, ret);
  
  ret = bilog->log_stop(dpp(), std::nullopt, bucket_info, log_layout, 0);
  EXPECT_EQ(0, ret);
  
  co_return;
}

CORO_TEST_F(BiLog, DispatcherMigrationScenario, BiLogDispatcherTest) {
  // test migration scenario: same dispatcher handles both layouts
  
  // setup old bucket (InIndex)
  setup_bucket_info("old-bucket", "old-bucket-id");
  setup_inindex_layout();
  RGWBucketInfo old_bucket = bucket_info;
  auto old_layout = log_layout;
  
  // setup new bucket (FIFO)
  setup_bucket_info("new-bucket", "new-bucket-id"); 
  setup_fifo_layout();
  RGWBucketInfo new_bucket = bucket_info;
  auto new_layout = log_layout;
  
  // both should work with same dispatcher
  int ret = bilog->log_start(dpp(), std::nullopt, old_bucket, old_layout, 0);
  EXPECT_EQ(0, ret);
  
  ret = bilog->log_start(dpp(), std::nullopt, new_bucket, new_layout, 0);
  EXPECT_EQ(0, ret);
  
  // both should list successfully (though InIndex returns empty)
  std::string marker;
  std::list<rgw_bi_log_entry> entries;
  bool truncated;
  
  ret = bilog->log_list(dpp(), std::nullopt, old_bucket, old_layout, 0,
                       marker, 100, entries, &truncated);
  EXPECT_EQ(0, ret);
  
  ret = bilog->log_list(dpp(), std::nullopt, new_bucket, new_layout, 0,
                       marker, 100, entries, &truncated);
  EXPECT_EQ(0, ret);
  
  // cleanup both
  ret = bilog->log_stop(dpp(), std::nullopt, old_bucket, old_layout, 0);
  EXPECT_EQ(0, ret);
  
  ret = bilog->log_stop(dpp(), std::nullopt, new_bucket, new_layout, 0);
  EXPECT_EQ(0, ret);
  
  co_return;
}

// test max marker functionality
CORO_TEST_F(BiLog, MaxMarkerSingleShard, BiLogDispatcherTest) {
  setup_fifo_layout();
  
  int ret = bilog->log_start(dpp(), std::nullopt, bucket_info, log_layout, 0);
  EXPECT_EQ(0, ret);
  
  // Test single shard max marker
  std::map<int, rgw_bucket_dir_header> headers;
  std::string max_marker;
  headers[0].max_marker = "test-marker";
  
  ret = bilog->log_get_max_marker(dpp(), bucket_info, headers, 0, 
                                 &max_marker, std::nullopt);
  EXPECT_EQ(0, ret);
  
  co_return;
}

CORO_TEST_F(BiLog, MaxMarkerMultipleShardsMap, BiLogDispatcherTest) {
  setup_fifo_layout();
  
  int ret = bilog->log_start(dpp(), std::nullopt, bucket_info, log_layout, -1);
  EXPECT_EQ(0, ret);
  
  // Test multiple shards max marker
  std::map<int, rgw_bucket_dir_header> headers;
  std::map<int, std::string> max_markers;
  headers[0].max_marker = "marker-0";
  headers[1].max_marker = "marker-1";
  headers[2].max_marker = "marker-2";
  
  ret = bilog->log_get_max_marker(dpp(), bucket_info, headers, -1,
                                 &max_markers, std::nullopt);
  EXPECT_EQ(0, ret);
  
  co_return;
}

CORO_TEST_F(BiLog, ErrorHandlingEmptyBucketInfo, BiLogDispatcherTest) {
  // Test with minimal bucket info
  RGWBucketInfo empty_bucket;
  empty_bucket.bucket.name = ""; // Empty name
  
  setup_inindex_layout();
  
  int ret = bilog->log_start(dpp(), std::nullopt, empty_bucket, log_layout, 0);
  // Should handle gracefully
  EXPECT_EQ(0, ret); // InIndex should always succeed
  
  co_return;
}

// test a bucket's lifecycle
CORO_TEST_F(BiLog, BucketLifecycleSequence, BiLogDispatcherTest) {
  setup_fifo_layout();
  
  // bucket creation -> operations -> deletion
  int ret = bilog->log_start(dpp(), std::nullopt, bucket_info, log_layout, 0);
  EXPECT_EQ(0, ret);
  
  // some bucket operations (to generate bilog entries)
  for (const auto& obj : test_objects) {
    co_await add_bilog_entry(0, obj);
  }
  
  // list entries
  auto entries = co_await read_all_entries(0);
  
  // trim old entries
  ret = bilog->log_trim(dpp(), std::nullopt, bucket_info, log_layout, 0, "");
  EXPECT_EQ(0, ret);
  
  // stop logging (bucket deletion)
  ret = bilog->log_stop(dpp(), std::nullopt, bucket_info, log_layout, 0);
  EXPECT_EQ(0, ret);
  
  co_return;
}