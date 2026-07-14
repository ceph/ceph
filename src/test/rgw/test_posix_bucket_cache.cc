// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "bucket_cache.h"
#include <iostream>
#include <fstream>
#include <filesystem>
#include <string>
#include <string_view>
#include <random>
#include <ranges>
#include <thread>
#include <stdint.h>

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/format.h>

#include <gtest/gtest.h>
#include "common/common_init.h"
#include "global/global_init.h"

using namespace std::chrono_literals;

namespace {

  namespace sf = std::filesystem;

  static const std::string bucket_root = "bucket_root";
  static const std::string database_root = "lmdb_root";
  static const std::string tdir1 = "tdir1";
  static const std::string tdir2 = "tdir2";

  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<> dist_1m(1, 1000000);
} // anonymous ns

class BucketCacheFixtureBase {
protected:
  static constexpr std::string_view bucket1_marker = ""; // start at the beginning

  DoutPrefixProvider* dpp{nullptr};
  std::vector<std::string> bvec;

  class MockSalDriver
  {
    std::vector<const char *> args;

  public:
    boost::intrusive_ptr<CephContext> cct;
    MockSalDriver() {
      /* Proceed with environment setup */
      cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                        CODE_ENVIRONMENT_UTILITY,
                        CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
    }
    /* called by BucketCache layer when a new object is discovered
     * by inotify or similar */
    int mint_listing_entry(
      const std::string& bucket, rgw_bucket_dir_entry& bde /* OUT */) {

      return 0;
    }
    CephContext *ctx(void) { return cct.get(); }
  }; /* MockSalDriver */

  class MockSalBucket
  {
    std::string name;
  public:
    MockSalBucket(const std::string& name)
      : name(name)
      {}
    const std::string& get_name() {
      return name;
    }

    using fill_cache_cb_t = file::listing::fill_cache_cb_t;

    int fill_cache(const DoutPrefixProvider* dpp, optional_yield y, fill_cache_cb_t cb) {
      sf::path rp{bucket_root};
      sf::path bp{rp / name};
      if (! (sf::exists(rp) && sf::is_directory(rp))) {
	std::cerr << fmt::format("{} bucket {} invalid", __func__, name)
		  << std::endl;
	exit(1);
      }
      for (const auto& dir_entry : sf::directory_iterator{bp}) {
	rgw_bucket_dir_entry bde{};
	auto fname = dir_entry.path().filename().string();
	bde.key.name = fname;
	cb(dpp, bde);
      }
      return 0;
    } /* fill_cache */

    /* default LMDB sort order, mirroring POSIXBucket::lmdb_cmp() */
    static MDB_cmp_func* lmdb_cmp() { return nullptr; }
  }; /* MockSalBucket */

  using BucketCache = file::listing::BucketCache<MockSalDriver, MockSalBucket>;
  // keep them in base class, so we don't have to initialize for every fixture
  static MockSalDriver sal_driver;
  static BucketCache* bucket_cache;

  static std::vector<std::string> setup_buckets() {
    int nbuckets = 5;
    int nfiles = 10;


    std::vector<std::string> bvec;
    for (int ix = 0; ix < nbuckets; ++ix) {
      bvec.push_back(fmt::format("recyle_{}", ix));
    }

    for (auto& bucket : bvec) {
      sf::path tp{sf::path{bucket_root} / bucket};
      sf::remove_all(tp);
      sf::create_directory(tp);

      std::string fbase{"file_"};
      for (int ix = 0; ix < nfiles; ++ix) {
      retry:
	auto n = dist_1m(mt);
	sf::path ttp{tp / fmt::format("{}{}", fbase, n)};
	if (sf::exists(ttp)) {
	  goto retry;
	} else {
  	std::ofstream ofs(ttp);
	  ofs << "data for " << ttp << std::endl;
	  ofs.close();
	}
      } /* for buckets */
    }
    return bvec;
  }
};

BucketCacheFixtureBase::MockSalDriver BucketCacheFixtureBase::sal_driver;
BucketCacheFixtureBase::BucketCache* BucketCacheFixtureBase::bucket_cache = nullptr;

namespace sf = std::filesystem;

auto func = [](const rgw_bucket_dir_entry& bde) -> bool
  {
    //std::cout << fmt::format("called back with {}", bde.key.name) << std::endl;
    return true;
  };

class BucketCacheFixtureDefault : public testing::Test, protected BucketCacheFixtureBase {
  static void setup_dir1() {
    sf::path tp{sf::path{bucket_root} / tdir1};
    sf::remove_all(tp);
    sf::create_directory(tp);

    /* generate 100K unique files in random order */
    std::string fbase{"file_"};
    for (int ix = 0; ix < 100000; ++ix) {
    retry:
      auto n = dist_1m(mt);
      sf::path ttp{tp / fmt::format("{}{}", fbase, n)};
      if (sf::exists(ttp)) {
	goto retry;
      } else {
	std::ofstream ofs(ttp);
	ofs << "data for " << ttp << std::endl;
	ofs.close();
      }
    } /* for 100K */
  }

  static void setup_dir2() {
    sf::path tp{sf::path{bucket_root} / tdir2};
    sf::remove_all(tp);
    sf::create_directory(tp);
    /* generate no objects in tdir2 */
  }

protected:
  virtual ~BucketCacheFixtureDefault() = default;

  static void SetUpTestSuite() {
    setup_dir1();
    setup_dir2();

    // default tuning
    bucket_cache = new BucketCache{&sal_driver, bucket_root, database_root};
  }

  static void TearDownTestSuite() {
    delete bucket_cache;
    bucket_cache = nullptr;
  }
};

TEST_F(BucketCacheFixtureDefault, ListTDir1)
{
  MockSalBucket sb{tdir1};
  std::string marker{bucket1_marker};
  (void) bucket_cache->list_bucket(dpp, null_yield, &sb, marker, func);
}

TEST_F(BucketCacheFixtureDefault, ListEmpty)
{
  MockSalBucket sb{tdir2};
  std::string marker{bucket1_marker};
  (void) bucket_cache->list_bucket(dpp, null_yield, &sb, marker, func);
}

TEST_F(BucketCacheFixtureDefault, ListThreads) /* clocked at 21ms on lemon, and yes,
				* it did list 100K entries per thread */
{
  auto nthreads = 15;
  std::vector<std::thread> threads;

  auto func = [](const rgw_bucket_dir_entry& bde) -> int
    {
      //std::cout << fmt::format("called back with {}", bde.key.name) << std::endl;
      return 0;
    };

  MockSalBucket sb{tdir1};
  std::string marker{bucket1_marker};

  for (int ix = 0; ix < nthreads; ++ix) {
    threads.push_back(std::thread([&]() {
      (void) bucket_cache->list_bucket(dpp, null_yield, &sb, marker, func);
    }));
  }
  for (auto& t : threads) {
    t.join();
  }
}

class BucketCacheFixtureRecycle1 : public testing::Test, protected BucketCacheFixtureBase {
protected:
  static std::vector<std::string> bvec;

  static void SetUpTestSuite() {
    bvec = setup_buckets();
    bucket_cache = new BucketCache{&sal_driver, bucket_root, database_root, 2, 1, 1, 1};
  }

  static void TearDownTestSuite() {
    delete bucket_cache;
    bucket_cache = nullptr;
  }
};

std::vector<std::string> BucketCacheFixtureRecycle1::bvec;

TEST_F(BucketCacheFixtureRecycle1, ListNRecycle1)
{
  /* 5 buckets through a cache with max_buckets=2 (lane_hiwat=2):
   * evict_block fires when q.size() > 2, so buckets 4 and 5 each
   * trigger an eviction → recycle_count=2 */
  for (auto& bucket : bvec) {
    MockSalBucket sb{bucket};
    std::string marker{bucket1_marker};
    (void) bucket_cache->list_bucket(dpp, null_yield, &sb, marker, func);
  }
  auto total_evictions = bucket_cache->recycle_count + bucket_cache->cleanup_count;
  ASSERT_GE(total_evictions, 2);
}

class BucketCacheFixtureRecyclePartitions1 : public testing::Test, protected BucketCacheFixtureBase {
protected:
  static std::vector<std::string> bvec;

  static void SetUpTestSuite() {
    bvec = setup_buckets();
    bucket_cache = new BucketCache{&sal_driver, bucket_root, database_root, 2, 1, 5 /* max partitions */, 1};
  }
  static void TearDownTestSuite() {
    delete bucket_cache;
    bucket_cache = nullptr;
  }
};

std::vector<std::string> BucketCacheFixtureRecyclePartitions1::bvec;

TEST_F(BucketCacheFixtureRecyclePartitions1, ListNRecyclePartitions1)
{
  /* same as ListNRecycle1 but with 5 cache partitions mapped to 1 lru
   * lane — verifies partition independence */
  for (auto& bucket : bvec) {
    MockSalBucket sb{bucket};
    std::string marker{bucket1_marker};
    (void) bucket_cache->list_bucket(dpp, null_yield, &sb, marker, func);
  }
  auto total_evictions = bucket_cache->recycle_count + bucket_cache->cleanup_count;
  ASSERT_GE(total_evictions, 2);
}

class BucketCacheFixtureMarker1 : public testing::Test, protected BucketCacheFixtureBase {
protected:
  static void SetUpTestSuite() {
    int nfiles = 20;
    std::string bucket{"marker1"};

    sf::path tp{sf::path{bucket_root} / bucket};
    sf::remove_all(tp);
    sf::create_directory(tp);

    std::string fbase{"file_"};
    for (int ix = 0; ix < nfiles; ++ix) {
      sf::path ttp{tp / fmt::format("{}{}", fbase, ix)};
      std::ofstream ofs(ttp);
      ofs << "data for " << ttp << std::endl;
      ofs.close();
    }
    bucket_cache = new BucketCache{&sal_driver, bucket_root, database_root};
  }

  static void TearDownTestSuite() {
    delete bucket_cache;
    bucket_cache = nullptr;
  }
};

TEST_F(BucketCacheFixtureMarker1, ListMarker1)
{
  std::string bucket{"marker1"};
  std::string marker{"file_18"}; // midpoint+1
  std::vector<std::string> names;

  auto f = [&](const rgw_bucket_dir_entry& bde) -> int {
    //std::cout << fmt::format("called back with {}", bde.key.name) << std::endl;
    names.push_back(bde.key.name);
    return true;
  };

  MockSalBucket sb{bucket};
  (void) bucket_cache->list_bucket(dpp, null_yield, &sb, marker, f);

  ASSERT_EQ(names.size(), 10);
  ASSERT_EQ(*names.begin(), "file_18");
  ASSERT_EQ(*names.rbegin(), "file_9");
}

class BucketCacheFixtureInotify1 : public testing::Test, protected BucketCacheFixtureBase {
protected:
  void SetUp() override {
    sf::path tp{sf::path{bucket_root} / "inotify1"};
    sf::remove_all(tp);
    sf::create_directory(tp);
    bucket_cache = new BucketCache{&sal_driver, bucket_root, database_root};
  }

  void TearDown() override {
    delete bucket_cache;
    bucket_cache = nullptr;
    sf::path tp{sf::path{bucket_root} / "inotify1"};
    sf::remove_all(tp);
    sf::create_directory(tp);
  }

  static void create_files(std::string bucket, std::string fbase, int nfiles) {
    sf::path tp{sf::path{bucket_root} / "inotify1"};

    for (int ix = 0; ix < nfiles; ++ix) {
      sf::path ttp{tp / fmt::format("{}{}", fbase, ix)};
      std::ofstream ofs(ttp);
      ofs << "data for " << ttp << std::endl;
      ofs.close();
      ASSERT_TRUE(sf::exists(ttp));
    }
  }

  static void remove_files(std::string bucket, std::string fbase, int fstart, int fend) {
    sf::path tp{sf::path{bucket_root} / "inotify1"};
    for (int ix = fstart; ix < fend; ++ix) {
      sf::path ttp{tp / fmt::format("{}{}", fbase, ix)};
      sf::remove(ttp);
      ASSERT_FALSE(sf::exists(ttp));
    }
  }

};

TEST_F(BucketCacheFixtureInotify1, ListInotify1)
{
  std::string bucket{"inotify1"};
  std::string marker{""};
  std::vector<std::string> names;
  int nfiles{20};

  auto f = [&](const rgw_bucket_dir_entry& bde) -> int {
    //std::cout << fmt::format("called back with {}", bde.key.name) << std::endl;
    names.push_back(bde.key.name);
    return true;
  };

  create_files(bucket, "file_", nfiles);

  MockSalBucket sb{bucket};

  (void) bucket_cache->list_bucket(dpp, null_yield, &sb, marker, f);
  ASSERT_EQ(names.size(), nfiles);
} /* ListInotify1 */

TEST_F(BucketCacheFixtureInotify1, UpdateInotify1)
{
  std::string bucket{"inotify1"};

  sf::path tp{sf::path{bucket_root} / bucket};

  create_files(bucket, "file_", 20);

  /* add some */
  create_files(bucket, "upfile_", 10);

  /* remove some */
  remove_files(bucket, "file_", 5, 10);
} /* SetupInotify1 */

#if 0
TEST_F(BucketCacheFixtureInotify1, List2Inotify1)
{
  std::string bucket{"inotify1"};
  std::string marker{""};
  std::vector<std::string> names;
  int timeout = 50;

  auto f = [&](const rgw_bucket_dir_entry& bde) -> int {
    //std::cout << fmt::format("called back with {}", bde.key.name) << std::endl;
    names.push_back(bde.key.name);
    return true;
  };

  create_files(bucket, "file_", 20);

  MockSalBucket sb{bucket};

  /* Do a timed backoff up to ~20 seconds to pass on CI */
  while (timeout < 16000) {
    names.clear();
    (void)bucket_cache->list_bucket(dpp, null_yield, &sb, marker, f);
    if (names.size() == 20)
      break;
    std::cout << fmt::format("waiting for {}ms for cache sync size={}", timeout, names.size()) << std::endl;
    std::this_thread::sleep_for(1000ms);
    timeout *= 2;
  }
  ASSERT_EQ(names.size(), 20);

  /* Add some */
  sf::path tp{sf::path{bucket_root} / bucket};
  timeout = 50;

  create_files(bucket, "upfile_", 10);

  /* Do a timed backoff up to ~20 seconds to pass on CI */
  while (timeout < 16000) {
    names.clear();
    (void)bucket_cache->list_bucket(dpp, null_yield, &sb, marker, f);
    if (names.size() == 30)
      break;
    std::cout << fmt::format("waiting for {}ms for cache sync size={}", timeout, names.size()) << std::endl;
    std::this_thread::sleep_for(1000ms);
    timeout *= 2;
  }
  ASSERT_EQ(names.size(), 30);

  /* remove some */
  timeout = 50;

  remove_files(bucket, "file_", 5, 10);

  /* Do a timed backoff up to ~20 seconds to pass on CI */
  while (timeout < 16000) {
    names.clear();
    (void)bucket_cache->list_bucket(dpp, null_yield, &sb, marker, f);
    if (names.size() == 25)
      break;
    std::cout << fmt::format("waiting for {}ms for cache sync size={}", timeout, names.size()) << std::endl;
    std::this_thread::sleep_for(1000ms);
    timeout *= 2;
  }
  ASSERT_EQ(names.size(), 25);
} /* List2Inotify1 */
#endif

class BucketCacheFixtureDbiRecycle : public testing::Test, protected BucketCacheFixtureBase {
protected:
  static constexpr int n_buckets = 60;
  static constexpr int n_files = 5;
  static constexpr uint32_t max_buckets = 4;

  static std::vector<std::string> bvec;

  static void SetUpTestSuite() {
    bvec.clear();
    for (int ix = 0; ix < n_buckets; ++ix) {
      std::string bname = fmt::format("dbi_recycle_{}", ix);
      bvec.push_back(bname);
      sf::path tp{sf::path{bucket_root} / bname};
      sf::remove_all(tp);
      sf::create_directory(tp);
      for (int jx = 0; jx < n_files; ++jx) {
	sf::path fp{tp / fmt::format("obj_{}", jx)};
	std::ofstream ofs(fp);
	ofs << "data" << std::endl;
	ofs.close();
      }
    }
    bucket_cache = new BucketCache{
      &sal_driver, bucket_root, database_root,
      max_buckets,
      1 /* max_lanes */,
      1 /* max_partitions */,
      1 /* lmdb_count */};
  }

  static void TearDownTestSuite() {
    delete bucket_cache;
    bucket_cache = nullptr;
  }
};

std::vector<std::string> BucketCacheFixtureDbiRecycle::bvec;

TEST_F(BucketCacheFixtureDbiRecycle, DbiHandlesBounded)
{
  /* Push n_buckets (60) unique buckets through a cache with max_buckets=4.
   * max_dbs_per_partition = (4/1)*5/4 + 16 = 21.
   * Without DBI recycling, this would exhaust DBI slots after ~21 buckets
   * and all subsequent list_bucket calls would fail with -EIO.
   * With recycling, all 60 should succeed. */
  for (auto& bucket : bvec) {
    MockSalBucket sb{bucket};
    std::string marker{bucket1_marker};
    int ret = bucket_cache->list_bucket(dpp, null_yield, &sb, marker, func);
    ASSERT_EQ(ret, 0) << "list_bucket failed for " << bucket;
  }

  /* evictions must have happened (via reclaim or hiwat cleanup) */
  auto total_evictions = bucket_cache->recycle_count + bucket_cache->cleanup_count;
  std::cout << fmt::format("DbiRecycle: recycle_count={} cleanup_count={} total_evictions={}",
    bucket_cache->recycle_count.load(), bucket_cache->cleanup_count.load(),
    total_evictions) << std::endl;
  ASSERT_GT(total_evictions, 0);

  /* DBI handles stay bounded: map + free pool <= max_dbs_per_partition */
  auto dbi_total = bucket_cache->lmdbs.total_dbi_map_size()
    + bucket_cache->lmdbs.total_free_dbis();
  uint32_t max_dbs = (max_buckets / 1) * 5 / 4 + 16;
  std::cout << fmt::format("DbiRecycle: dbi_map={} free_dbis={} total={} max_dbs={}",
    bucket_cache->lmdbs.total_dbi_map_size(),
    bucket_cache->lmdbs.total_free_dbis(),
    dbi_total, max_dbs) << std::endl;
  ASSERT_LE(dbi_total, max_dbs)
    << "DBI handles exceeded max_dbs_per_partition"
    << " (map=" << bucket_cache->lmdbs.total_dbi_map_size()
    << " free=" << bucket_cache->lmdbs.total_free_dbis() << ")";

  /* re-list an early bucket that was evicted — must succeed and
   * return the correct number of objects after re-fill */
  {
    std::vector<std::string> names;
    auto collect = [&](const rgw_bucket_dir_entry& bde) -> bool {
      names.push_back(bde.key.name);
      return true;
    };
    MockSalBucket sb{bvec[0]};
    std::string marker{bucket1_marker};
    int ret = bucket_cache->list_bucket(dpp, null_yield, &sb, marker, collect);
    ASSERT_EQ(ret, 0) << "re-list of evicted bucket failed";
    ASSERT_EQ(names.size(), n_files)
      << "re-list returned wrong count after eviction+refill";
  }
}

int main (int argc, char *argv[])
{

  sf::path br{sf::path{bucket_root}};
  sf::create_directory(br);
  sf::remove_all(br);
  sf::create_directory(br);

  sf::path lr{sf::path{database_root}};
  sf::create_directory(lr);
  sf::remove_all(lr);
  sf::create_directory(lr);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
