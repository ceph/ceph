#include <list>
#include <string>
#include <vector>
#include <iterator>
#include <algorithm>

#include "gtest/gtest.h"

#include "include/rados/librados.hpp"
#include "test/librados/test_cxx.h"
#include "test/librados/testcase_cxx.h"

using namespace librados;

using LibRadosPoolsPP = RadosTestPP;

TEST_P(LibRadosPoolsPP, PoolListPP)
{
  const std::string sentinel = "not-a-pool";

  std::list<std::string> legacy_names {sentinel};
  std::vector<std::string> names {sentinel};

  ASSERT_EQ(0, cluster.pool_list(legacy_names));
  ASSERT_EQ(0, cluster.pool_list(names));
  EXPECT_EQ(names,
            std::vector<std::string>(std::begin(legacy_names),
                                     std::end(legacy_names)));
  EXPECT_NE(std::end(names), std::ranges::find(names, pool_name));
  EXPECT_EQ(std::end(names), std::ranges::find(names, sentinel));

  using pool_entry = std::pair<int64_t, std::string>;
  const pool_entry sentinel_pool {-1, sentinel};
  std::list<pool_entry> legacy_pools {sentinel_pool};
  std::vector<pool_entry> pools {sentinel_pool};

  ASSERT_EQ(0, cluster.pool_list2(legacy_pools));
  ASSERT_EQ(0, cluster.pool_list(pools));
  EXPECT_EQ(pools,
            std::vector<pool_entry>(std::next(std::begin(legacy_pools)),
                                    std::end(legacy_pools)));
  EXPECT_EQ(sentinel_pool, legacy_pools.front());
  EXPECT_EQ(std::end(pools), std::ranges::find(pools, sentinel_pool));
  EXPECT_NE(std::end(pools), std::ranges::find(
                                pools, pool_name, &pool_entry::second));
}

TEST(LibRadosPoolsPP, PoolListErrorPreservesOutput)
{
  Rados disconnected;
  ASSERT_EQ(0, disconnected.init(nullptr));

  const std::vector<std::string> expected_names {"untouched"};
  auto names = expected_names;
  EXPECT_EQ(-ENOTCONN, disconnected.pool_list(names));
  EXPECT_EQ(expected_names, names);

  using pool_entry = std::pair<int64_t, std::string>;
  const std::vector<pool_entry> expected_pools {{-1, "untouched"}};
  auto pools = expected_pools;
  EXPECT_EQ(-ENOTCONN, disconnected.pool_list(pools));
  EXPECT_EQ(expected_pools, pools);
}

INSTANTIATE_TEST_SUITE_P_REPLICA(LibRadosPoolsPP);
