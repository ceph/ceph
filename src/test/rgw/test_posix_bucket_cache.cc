// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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

  class MockSalDriver
  {
  public:
    /* called by BucketCache layer when a new object is discovered
     * by inotify or similar */
    int mint_listing_entry(
      const std::string& bucket, rgw_bucket_dir_entry& bde /* OUT */) {

      return 0;
    }
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
    bucket_cache = new BucketCache{&sal_driver, bucket_root, database_root, 1, 1, 1, 1};
  }

  static void TearDownTestSuite() {
    delete bucket_cache;
    bucket_cache = nullptr;
  }
};

std::vector<std::string> BucketCacheFixtureRecycle1::bvec;

TEST_F(BucketCacheFixtureRecycle1, ListNRecycle1)
{
  /* the effect is to allocate a Bucket cache entry once, then recycle n-1 times */
  for (auto& bucket : bvec) {
    MockSalBucket sb{bucket};
    std::string marker{bucket1_marker};
    (void) bucket_cache->list_bucket(dpp, null_yield, &sb, marker, func);
  }
  ASSERT_EQ(bucket_cache->recycle_count, 4);
}

class BucketCacheFixtureRecyclePartitions1 : public testing::Test, protected BucketCacheFixtureBase {
protected:
  static std::vector<std::string> bvec;

  static void SetUpTestSuite() {
    bvec = setup_buckets();
    bucket_cache = new BucketCache{&sal_driver, bucket_root, database_root, 1, 1, 5 /* max partitions */, 1};
  }
  static void TearDownTestSuite() {
    delete bucket_cache;
    bucket_cache = nullptr;
  }
};

std::vector<std::string> BucketCacheFixtureRecyclePartitions1::bvec;

TEST_F(BucketCacheFixtureRecyclePartitions1, ListNRecyclePartitions1)
{
  /* the effect is to allocate a Bucket cache entry once, then recycle
   * n-1 times--in addition, 5 cache partitions are mapped to 1 lru
   * lane--verifying independence */
  for (auto& bucket : bvec) {
    MockSalBucket sb{bucket};
    std::string marker{bucket1_marker};
    (void) bucket_cache->list_bucket(dpp, null_yield, &sb, marker, func);
  }
  ASSERT_EQ(bucket_cache->recycle_count, 4);
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
  static void SetUpTestSuite() {
    int nfiles = 20;
    std::string bucket{"inotify1"};

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

TEST_F(BucketCacheFixtureInotify1, ListInotify1)
{
  std::string bucket{"inotify1"};
  std::string marker{""};
  std::vector<std::string> names;

  auto f = [&](const rgw_bucket_dir_entry& bde) -> int {
    //std::cout << fmt::format("called back with {}", bde.key.name) << std::endl;
    names.push_back(bde.key.name);
    return true;
  };

  MockSalBucket sb{bucket};

  (void) bucket_cache->list_bucket(dpp, null_yield, &sb, marker, f);
  ASSERT_EQ(names.size(), 20);
} /* ListInotify1 */

TEST_F(BucketCacheFixtureInotify1, UpdateInotify1)
{
  int nfiles = 10;
  std::string bucket{"inotify1"};

  sf::path tp{sf::path{bucket_root} / bucket};

  /* add some */
  std::string fbase{"upfile_"};
  for (int ix = 0; ix < nfiles; ++ix) {
    sf::path ttp{tp / fmt::format("{}{}", fbase, ix)};
    std::ofstream ofs(ttp);
    ofs << "data for " << ttp << std::endl;
    ofs.close();
  }

  /* remove some */
  fbase = "file_";
  for (int ix = 5; ix < 10; ++ix) {
    sf::path ttp{tp / fmt::format("{}{}", fbase, ix)};
    sf::remove(ttp);
  }
} /* SetupInotify1 */

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

  MockSalBucket sb{bucket};

  /* Do a timed backoff up to ~20 seconds to pass on CI */
  while (timeout < 16000) {
    (void)bucket_cache->list_bucket(dpp, null_yield, &sb, marker, f);
    if (names.size() == 25)
      break;
    std::cout << fmt::format("waiting for {}ms for cache sync", timeout) << std::endl;
    std::this_thread::sleep_for(1000ms);
    timeout *= 2;
    names.clear();
  }
  ASSERT_EQ(names.size(), 25);

  /* check these */
  sf::path tp{sf::path{bucket_root} / bucket};

  std::string fbase{"upfile_"};
  for (int ix = 0; ix < 10; ++ix) {
    sf::path ttp{tp / fmt::format("{}{}", fbase, ix)};
    ASSERT_TRUE(sf::exists(ttp));
  }

  /* remove some */
  fbase = "file_";
  for (int ix = 5; ix < 10; ++ix) {
    sf::path ttp{tp / fmt::format("{}{}", fbase, ix)};
    ASSERT_FALSE(sf::exists(ttp));
  }
} /* List2Inotify1 */

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
