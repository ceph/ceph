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

using namespace file::listing;
using namespace std::chrono_literals;

namespace {
  std::string bucket_root = "bucket_root";
  std::string database_root = "lmdb_root";
  uint32_t max_buckets = 100;
  std::string bucket1_name = "stanley";
  std::string bucket1_marker = ""; // start at the beginning

  std::random_device rd;
  std::mt19937 mt(rd());

  std::string tdir1{"tdir1"};
  std::uniform_int_distribution<> dist_1m(1, 1000000);
  BucketCache* bc{nullptr};
  std::vector<std::string> bvec;
} // anonymous ns

namespace sf = std::filesystem;

TEST(BucketCache, SetupTDir1)
{
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
} /* SetupTDir1 */

TEST(BucketCache, InitBucketCache)
{
  bc = new BucketCache{bucket_root, database_root}; // default tuning
}

auto func = [](const std::string_view& k) -> int 
  {
    //std::cout << fmt::format("called back with {}", k) << std::endl;
    return 0;
  };

TEST(BucketCache, ListTDir1)
{
  bc->list_bucket(tdir1, bucket1_marker, func);
}

TEST(BucketCache, ListTDir2)
{
  bc->list_bucket(tdir1, bucket1_marker, func);
}

TEST(BucketCache, ListTDir3)
{
  bc->list_bucket(tdir1, bucket1_marker, func);
}

TEST(BucketCache, ListThreads) /* clocked at 21ms on lemon, and yes,
				* it did list 100K entries per thread */
{
  auto nthreads = 15;
  std::vector<std::thread> threads;

  auto func = [](const std::string_view& k) -> int 
    {
      //std::cout << fmt::format("called back with {}", k) << std::endl;
      return 0;
    };

  for (int ix = 0; ix < nthreads; ++ix) {
    threads.push_back(std::thread([&]() {
      bc->list_bucket(tdir1, bucket1_marker, func);
    }));
  }
  for (auto& t : threads) {
    t.join();
  }
}

TEST(BucketCache, SetupRecycle1)
{
  int nbuckets = 5;
  int nfiles = 10;

  bvec = [&]() {
    std::vector<std::string> v;
    for (int ix = 0; ix < nbuckets; ++ix) {
      v.push_back(fmt::format("recyle_{}", ix));
    }
    return v;
  }();

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
} /* SetupRecycle1 */

TEST(BucketCache, InitBucketCacheRecycle1)
{
  bc = new BucketCache{bucket_root, database_root, 1, 1, 1, 1};
}

TEST(BucketCache, ListNRecycle1)
{
  /* the effect is to allocate a Bucket cache entry once, then recycle n-1 times */
  for (auto& bucket : bvec) {
    bc->list_bucket(bucket, bucket1_marker, func);
  }
  ASSERT_EQ(bc->recycle_count, 4);
}

TEST(BucketCache, TearDownBucketCacheRecycle1)
{
  delete bc;
  bc = nullptr;
}

TEST(BucketCache, InitBucketCacheRecyclePartitions1)
{
  bc = new BucketCache{bucket_root, database_root, 1, 1, 5 /* max partitions */, 1};
}

TEST(BucketCache, ListNRecyclePartitions1)
{
  /* the effect is to allocate a Bucket cache entry once, then recycle
   * n-1 times--in addition, 5 cache partitions are mapped to 1 lru
   * lane--verifying independence */
  for (auto& bucket : bvec) {
    bc->list_bucket(bucket, bucket1_marker, func);
  }
  ASSERT_EQ(bc->recycle_count, 4);
}

TEST(BucketCache, TearDownBucketCacheRecyclePartitions1)
{
  delete bc;
  bc = nullptr;
}

TEST(BucketCache, SetupMarker1)
{
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
} /* SetupMarker1 */

TEST(BucketCache, InitBucketCacheMarker1)
{
  bc = new BucketCache{bucket_root, database_root};
}

TEST(BucketCache, ListMarker1)
{
  std::string bucket{"marker1"};
  std::string marker{"file_18"}; // midpoint+1
  std::vector<std::string> names;

  auto f = [&](const std::string_view& k) -> int {
    //std::cout << fmt::format("called back with {}", k) << std::endl;
    names.push_back(std::string{k});
    return 0;
  };

  bc->list_bucket(bucket, marker, f);

  ASSERT_EQ(names.size(), 10);
  ASSERT_EQ(*names.begin(), "file_18");
  ASSERT_EQ(*names.rbegin(), "file_9");
}

TEST(BucketCache, TearDownBucketCacheMarker1)
{
  delete bc;
  bc = nullptr;
}

TEST(BucketCache, SetupInotify1)
{
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
} /* SetupInotify1 */

TEST(BucketCache, InitBucketCacheInotify1)
{
  bc = new BucketCache{bucket_root, database_root};
}

TEST(BucketCache, ListInotify1)
{
  std::string bucket{"inotify1"};
  std::string marker{""};
  std::vector<std::string> names;

  auto f = [&](const std::string_view& k) -> int {
    //std::cout << fmt::format("called back with {}", k) << std::endl;
    names.push_back(std::string{k});
    return 0;
  };

  bc->list_bucket(bucket, marker, f);
  ASSERT_EQ(names.size(), 20);
} /* ListInotify1 */

TEST(BucketCache, UpdateInotify1)
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

  /* this step is async, temporally consistent */
  std::cout << "waiting 50ms for cache sync" << std::endl;
  std::this_thread::sleep_for(50ms);
} /* SetupInotify1 */

TEST(BucketCache, List2Inotify1)
{
  std::string bucket{"inotify1"};
  std::string marker{""};
  std::vector<std::string> names;

  auto f = [&](const std::string_view& k) -> int {
    //std::cout << fmt::format("called back with {}", k) << std::endl;
    names.push_back(std::string{k});
    return 0;
  };

  bc->list_bucket(bucket, marker, f);
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

TEST(BucketCache, TearDownInotify1)
{
  delete bc;
  bc = nullptr;
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
