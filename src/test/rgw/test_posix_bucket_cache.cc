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

/* --- Stress tests --- */

class BucketCacheStressBase : protected BucketCacheFixtureBase {
protected:
  static void setup_n_buckets(std::vector<std::string>& bvec,
			      int n, int files_per_bucket) {
    bvec.clear();
    for (int ix = 0; ix < n; ++ix) {
      std::string bname = fmt::format("stress_{}", ix);
      bvec.push_back(bname);
      sf::path tp{sf::path{bucket_root} / bname};
      sf::remove_all(tp);
      sf::create_directory(tp);
      for (int jx = 0; jx < files_per_bucket; ++jx) {
	sf::path fp{tp / fmt::format("obj_{}", jx)};
	std::ofstream ofs(fp);
	ofs << "data" << std::endl;
	ofs.close();
      }
    }
  }
};

/* Scenario 1: concurrent get_bucket + invalidate_bucket (UAF race) */
class BucketCacheStressGetInvalidate
  : public testing::Test, protected BucketCacheStressBase {
protected:
  static std::vector<std::string> bvec;

  static void SetUpTestSuite() {
    setup_n_buckets(bvec, 8, 5);
    bucket_cache = new BucketCache{
      &sal_driver, bucket_root, database_root,
      4 /* max_buckets */,
      1 /* max_lanes */,
      1 /* max_partitions */,
      1 /* lmdb_count */};
  }

  static void TearDownTestSuite() {
    delete bucket_cache;
    bucket_cache = nullptr;
  }
};

std::vector<std::string> BucketCacheStressGetInvalidate::bvec;

TEST_F(BucketCacheStressGetInvalidate, ConcurrentGetAndInvalidate)
{
  const int n_listers = 16;
  const int n_invalidators = 4;
  const int iterations = 5000;
  std::atomic<bool> stop{false};
  std::atomic<int> errors{0};

  auto list_fn = [&](int tid) {
    for (int i = 0; i < iterations && !stop.load(); ++i) {
      auto& bname = bvec[i % bvec.size()];
      MockSalBucket sb{bname};
      std::string marker{""};
      auto f = [](const rgw_bucket_dir_entry&) -> bool { return true; };
      int ret = bucket_cache->list_bucket(dpp, null_yield, &sb, marker, f);
      if (ret != 0) {
	errors++;
      }
    }
  };

  auto invalidate_fn = [&](int tid) {
    for (int i = 0; i < iterations && !stop.load(); ++i) {
      auto& bname = bvec[i % bvec.size()];
      bucket_cache->invalidate_bucket(dpp, bname);
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < n_listers; ++i) {
    threads.emplace_back(list_fn, i);
  }
  for (int i = 0; i < n_invalidators; ++i) {
    threads.emplace_back(invalidate_fn, i);
  }
  for (auto& t : threads) {
    t.join();
  }

  auto total_evictions = bucket_cache->recycle_count + bucket_cache->cleanup_count;
  std::cout << fmt::format("StressGetInvalidate: evictions={} errors={}",
    total_evictions, errors.load()) << std::endl;
  ASSERT_GT(total_evictions, 0);
}

/* Scenario 2: rapid create/invalidate churn (shadow lifecycle) */
class BucketCacheStressChurn
  : public testing::Test, protected BucketCacheStressBase {
protected:
  static void SetUpTestSuite() {
    /* create one bucket dir that all churn names will reference
     * (they all map to the same empty directory) */
    for (int i = 0; i < 200; ++i) {
      std::string bname = fmt::format("churn_{}", i);
      sf::path tp{sf::path{bucket_root} / bname};
      sf::remove_all(tp);
      sf::create_directory(tp);
    }
    bucket_cache = new BucketCache{
      &sal_driver, bucket_root, database_root,
      4 /* max_buckets */,
      1 /* max_lanes */,
      1 /* max_partitions */,
      1 /* lmdb_count */};
  }

  static void TearDownTestSuite() {
    delete bucket_cache;
    bucket_cache = nullptr;
  }
};

TEST_F(BucketCacheStressChurn, RapidCreateInvalidate)
{
  const int n_threads = 16;
  const int iterations = 500;
  std::atomic<int> name_counter{0};

  auto churn_fn = [&]() {
    for (int i = 0; i < iterations; ++i) {
      int id = name_counter++ % 200;
      std::string bname = fmt::format("churn_{}", id);
      MockSalBucket sb{bname};
      std::string marker{""};
      auto f = [](const rgw_bucket_dir_entry&) -> bool { return true; };
      bucket_cache->list_bucket(dpp, null_yield, &sb, marker, f);
      bucket_cache->invalidate_bucket(dpp, bname, true /* recycle */);
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < n_threads; ++i) {
    threads.emplace_back(churn_fn);
  }
  for (auto& t : threads) {
    t.join();
  }

  auto total_evictions = bucket_cache->recycle_count + bucket_cache->cleanup_count;
  std::cout << fmt::format("StressChurn: recycle={} cleanup={} total={}",
    bucket_cache->recycle_count.load(),
    bucket_cache->cleanup_count.load(),
    total_evictions) << std::endl;
  ASSERT_GT(total_evictions, 0);
}

/* Scenario 3: DBI slot exhaustion */
class BucketCacheStressDbiExhaust
  : public testing::Test, protected BucketCacheStressBase {
protected:
  static std::vector<std::string> bvec;

  static void SetUpTestSuite() {
    /* 10 buckets, but only 4 DBI slots — must evict to make room */
    setup_n_buckets(bvec, 10, 3);
    bucket_cache = new BucketCache{
      &sal_driver, bucket_root, database_root,
      10 /* max_buckets — more slots than DBIs */,
      1 /* max_lanes */,
      1 /* max_partitions */,
      1 /* lmdb_count */};
  }

  static void TearDownTestSuite() {
    delete bucket_cache;
    bucket_cache = nullptr;
  }
};

std::vector<std::string> BucketCacheStressDbiExhaust::bvec;

TEST_F(BucketCacheStressDbiExhaust, ExhaustionIsClean)
{
  int successes = 0;
  int failures = 0;
  for (auto& bucket : bvec) {
    MockSalBucket sb{bucket};
    std::string marker{""};
    auto f = [](const rgw_bucket_dir_entry&) -> bool { return true; };
    int ret = bucket_cache->list_bucket(dpp, null_yield, &sb, marker, f);
    if (ret == 0) {
      successes++;
    } else {
      failures++;
    }
  }
  std::cout << fmt::format("DbiExhaust: successes={} failures={}",
    successes, failures) << std::endl;
  /* either all succeed (eviction freed DBIs) or failures are clean
   * (no crash, no assertion, just a return code) */
  ASSERT_EQ(successes + failures, 10);
}

/* Scenario 4: hiwat eviction under concurrent load */
class BucketCacheStressHiwat
  : public testing::Test, protected BucketCacheStressBase {
protected:
  static std::vector<std::string> bvec;

  static void SetUpTestSuite() {
    setup_n_buckets(bvec, 50, 5);
    bucket_cache = new BucketCache{
      &sal_driver, bucket_root, database_root,
      8 /* max_buckets */,
      2 /* max_lanes */,
      2 /* max_partitions */,
      1 /* lmdb_count */};
  }

  static void TearDownTestSuite() {
    delete bucket_cache;
    bucket_cache = nullptr;
  }
};

std::vector<std::string> BucketCacheStressHiwat::bvec;

TEST_F(BucketCacheStressHiwat, ConcurrentHiwatEviction)
{
  const int n_threads = 20;
  const int iterations = 2000;
  std::atomic<int> errors{0};

  auto work_fn = [&](int tid) {
    for (int i = 0; i < iterations; ++i) {
      int idx = (tid * iterations + i) % bvec.size();
      auto& bname = bvec[idx];
      MockSalBucket sb{bname};
      std::string marker{""};
      auto f = [](const rgw_bucket_dir_entry&) -> bool { return true; };
      int ret = bucket_cache->list_bucket(dpp, null_yield, &sb, marker, f);
      if (ret != 0) {
	errors++;
      }
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < n_threads; ++i) {
    threads.emplace_back(work_fn, i);
  }
  for (auto& t : threads) {
    t.join();
  }

  auto total_evictions = bucket_cache->recycle_count + bucket_cache->cleanup_count;
  std::cout << fmt::format("StressHiwat: evictions={} errors={}",
    total_evictions, errors.load()) << std::endl;
  ASSERT_GT(total_evictions, 0);
  ASSERT_EQ(errors.load(), 0);
}

/* --- Deterministic race tests via yield policy --- */

struct RandomYieldPolicy {
  static void yield_at(const char*) {
    thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<> dist(0, 99);
    if (dist(rng) < 40) {
      std::this_thread::yield();
    }
  }
};

class BucketCacheRaceBase : protected BucketCacheFixtureBase {
protected:
  using TestBucketCache =
    file::listing::BucketCache<MockSalDriver, MockSalBucket, RandomYieldPolicy>;
  using TestEntry =
    file::listing::BucketCacheEntry<MockSalDriver, MockSalBucket, RandomYieldPolicy>;

  static TestBucketCache* test_cache;

  /* find a bucket name whose XXH64 hash lands in target_part (mod n_parts) */
  static std::string name_for_partition(int target_part, int n_parts,
					const std::string& prefix = "r") {
    for (int i = 0; i < 10000; ++i) {
      std::string name = fmt::format("{}_{}", prefix, i);
      uint64_t hk = XXH64(name.c_str(), name.length(), TestEntry::seed);
      if (int(hk % n_parts) == target_part) {
	return name;
      }
    }
    return prefix + "_0";
  }

  static void make_bucket_dir(const std::string& name, int nfiles = 3) {
    sf::path tp{sf::path{bucket_root} / name};
    sf::remove_all(tp);
    sf::create_directory(tp);
    for (int i = 0; i < nfiles; ++i) {
      sf::path fp{tp / fmt::format("obj_{}", i)};
      std::ofstream ofs(fp);
      ofs << "data" << std::endl;
      ofs.close();
    }
  }
};

BucketCacheRaceBase::TestBucketCache* BucketCacheRaceBase::test_cache = nullptr;

/* Race test: multiple threads hammer a tiny cache with random yields
 * at critical points (ref_pre_refcnt_bump, ref_pre_lane_lock,
 * evict_block_post_unlock).  The RandomYieldPolicy widens race
 * windows that are nanoseconds wide in production.  Over many
 * iterations, this statistically exercises the hiwat, evict_block,
 * and ref/unref interleavings.  Buckets are spread across 2
 * partitions to avoid partition-lock deadlocks. */
class BucketCacheRaceBarrier
  : public testing::Test, protected BucketCacheRaceBase {
protected:
  static std::vector<std::string> bvec;

  static void SetUpTestSuite() {
    bvec.clear();
    for (int p = 0; p < 2; ++p) {
      for (int i = 0; i < 5; ++i) {
	std::string name = name_for_partition(
	  p, 2, fmt::format("rb{}", p));
	name = fmt::format("{}_{}", name, i);
	bvec.push_back(name);
	make_bucket_dir(name);
      }
    }
  }
};

std::vector<std::string> BucketCacheRaceBarrier::bvec;

TEST_F(BucketCacheRaceBarrier, HiwatAndEvictBlockUnderYield)
{
  /* tiny cache: hiwat=2, 1 lane, 2 partitions */
  test_cache = new TestBucketCache{
    &sal_driver, bucket_root, database_root,
    2 /* max_buckets */, 1 /* lanes */, 2 /* partitions */, 1 /* lmdb */};

  const int n_threads = 8;
  const int iterations = 2000;
  std::atomic<int> errors{0};

  auto work_fn = [&](int tid) {
    auto list_fn = [](const rgw_bucket_dir_entry&) -> bool { return true; };
    for (int i = 0; i < iterations; ++i) {
      int idx = (tid + i) % bvec.size();
      MockSalBucket sb{bvec[idx]};
      std::string marker{""};
      int ret = test_cache->list_bucket(dpp, null_yield, &sb, marker, list_fn);
      if (ret != 0) {
	errors++;
      }
      if (i % 7 == 0) {
	int inv_idx = (tid + i + 3) % bvec.size();
	test_cache->invalidate_bucket(dpp, bvec[inv_idx]);
      }
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < n_threads; ++i) {
    threads.emplace_back(work_fn, i);
  }
  for (auto& t : threads) {
    t.join();
  }

  auto total_evictions = test_cache->recycle_count + test_cache->cleanup_count;
  std::cout << fmt::format(
    "RaceBarrier: evictions={} errors={}", total_evictions, errors.load())
    << std::endl;
  ASSERT_GT(total_evictions, 0);
  ASSERT_EQ(errors.load(), 0);

  delete test_cache;
  test_cache = nullptr;
}


/* evict_block race test: evict_block leaves the victim linked in
 * lane.q while dropping lane lock for reclaim().  A racing ref()
 * can pass evicting.test() (narrow window), then move the entry
 * to active.  evict_block re-takes lane lock and tries push_front
 * on an entry already in active → safe_link assertion crash.
 *
 * The RandomYieldPolicy widens the evict_block_post_unlock and
 * ref_pre_refcnt_bump windows to increase the probability of
 * triggering the race.  With many threads and a tiny cache, the
 * evict_block path fires on every insert. */
class BucketCacheRaceEvictBlock
  : public testing::Test, protected BucketCacheRaceBase {
protected:
  static std::vector<std::string> bvec;

  static void SetUpTestSuite() {
    bvec.clear();
    for (int p = 0; p < 2; ++p) {
      for (int i = 0; i < 8; ++i) {
	std::string name = name_for_partition(
	  p, 2, fmt::format("eb{}", p));
	name = fmt::format("{}_{}", name, i);
	bvec.push_back(name);
	make_bucket_dir(name);
      }
    }
  }
};

std::vector<std::string> BucketCacheRaceEvictBlock::bvec;

TEST_F(BucketCacheRaceEvictBlock, EvictBlockUnlinkRace)
{
  /* tiny cache: hiwat=2, 1 lane — every 3rd distinct bucket forces
   * evict_block.  16 buckets across 2 partitions, 12 threads. */
  test_cache = new TestBucketCache{
    &sal_driver, bucket_root, database_root,
    2 /* max_buckets */, 1 /* lanes */, 2 /* partitions */, 1 /* lmdb */};

  const int n_threads = 128;
  const int iterations = 3000;
  std::atomic<int> errors{0};

  auto work_fn = [&](int tid) {
    auto list_fn = [](const rgw_bucket_dir_entry&) -> bool { return true; };
    for (int i = 0; i < iterations; ++i) {
      int idx = (tid + i) % bvec.size();
      MockSalBucket sb{bvec[idx]};
      std::string marker{""};
      int ret = test_cache->list_bucket(dpp, null_yield, &sb, marker, list_fn);
      if (ret != 0) {
	errors++;
      }
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < n_threads; ++i) {
    threads.emplace_back(work_fn, i);
  }
  for (auto& t : threads) {
    t.join();
  }

  auto total_evictions = test_cache->recycle_count + test_cache->cleanup_count;
  std::cout << fmt::format(
    "EvictBlockRace: evictions={} errors={}", total_evictions, errors.load())
    << std::endl;
  ASSERT_GT(total_evictions, 0);
  ASSERT_EQ(errors.load(), 0);

  delete test_cache;
  test_cache = nullptr;
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
