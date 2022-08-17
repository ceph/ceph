#include "common/perf_counters_cache_key.h"
#include <gtest/gtest.h>

namespace ceph::perf_counters {

TEST(PerfCounters, CacheKey)
{
  EXPECT_EQ(make_cache_key(""),
            std::string_view("\0", 1));

  EXPECT_EQ(make_cache_key("perf"),
            std::string_view("perf\0", 5));

  EXPECT_EQ(make_cache_key("perf", {{"",""}}),
            std::string_view("perf\0\0\0", 7));

  EXPECT_EQ(make_cache_key("perf", {{"",""}, {"",""}}),
            std::string_view("perf\0\0\0\0\0", 9));

  EXPECT_EQ(make_cache_key("perf", {{"a","b"}}),
            std::string_view("perf\0a\0b\0", 9));

  EXPECT_EQ(make_cache_key("perf", {{"y","z"}, {"a","b"}}),
            std::string_view("perf\0a\0b\0y\0z\0", 13));

  EXPECT_EQ(make_cache_key("perf", {{"a","b"}, {"a","c"}}),
            std::string_view("perf\0a\0b\0a\0c\0", 13));

  EXPECT_EQ(make_cache_key("perf", {{"a","z"}, {"a","b"}}),
            std::string_view("perf\0a\0b\0a\0z\0", 13));

  EXPECT_EQ(make_cache_key("perf", {{"d",""}, {"c",""}, {"b",""}, {"a",""}}),
            std::string_view("perf\0a\0\0b\0\0c\0\0d\0\0", 17));

  // error: ‘ceph::perf_counters::make_cache_key(...)’ is not a constant
  // expression because it refers to a result of ‘operator new’
#if 0
  constexpr auto constexpr_key = make_cache_key("perf", {
      {"d",""},{"c",""},{"b",""},{"a",""}
    });
  EXPECT_EQ(constexpr_key, std::string_view("perf\0a\0\0b\0\0c\0\0d\0\0", 17));
#endif
}

} // namespace ceph::perf_counters
