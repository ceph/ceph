#include "common/perf_counters_key.h"
#include <gtest/gtest.h>

namespace ceph::perf_counters {

TEST(PerfCounters, key_create)
{
  EXPECT_EQ(key_create(""),
            std::string_view("\0", 1));

  EXPECT_EQ(key_create("perf"),
            std::string_view("perf\0", 5));

  EXPECT_EQ(key_create("perf", {{"",""}}),
            std::string_view("perf\0\0\0", 7));

  EXPECT_EQ(key_create("perf", {{"","a"}, {"",""}}),
            std::string_view("perf\0\0a\0", 8));

  EXPECT_EQ(key_create("perf", {{"a","b"}}),
            std::string_view("perf\0a\0b\0", 9));

  EXPECT_EQ(key_create("perf", {{"y","z"}, {"a","b"}}),
            std::string_view("perf\0a\0b\0y\0z\0", 13));

  EXPECT_EQ(key_create("perf", {{"a","b"}, {"a","c"}}),
            std::string_view("perf\0a\0b\0", 9));

  EXPECT_EQ(key_create("perf", {{"a","z"}, {"a","b"}}),
            std::string_view("perf\0a\0z\0", 9));

  EXPECT_EQ(key_create("perf", {{"d",""}, {"c",""}, {"b",""}, {"a",""}}),
            std::string_view("perf\0a\0\0b\0\0c\0\0d\0\0", 17));
}

TEST(PerfCounters, key_insert)
{
  EXPECT_EQ(key_insert("", {{"",""}}),
            std::string_view("\0\0\0", 3));

  EXPECT_EQ(key_insert("", {{"",""}, {"",""}}),
            std::string_view("\0\0\0", 3));

  EXPECT_EQ(key_insert(std::string_view{"\0\0\0", 3}, {{"",""}}),
            std::string_view("\0\0\0", 3));

  EXPECT_EQ(key_insert(std::string_view{"\0", 1}, {{"",""}}),
            std::string_view("\0\0\0", 3));

  EXPECT_EQ(key_insert("", {{"a","b"}}),
            std::string_view("\0a\0b\0", 5));

  EXPECT_EQ(key_insert(std::string_view{"\0", 1}, {{"a","b"}}),
            std::string_view("\0a\0b\0", 5));

  EXPECT_EQ(key_insert("a", {{"",""}}),
            std::string_view("a\0\0\0", 4));

  EXPECT_EQ(key_insert(std::string_view{"a\0", 2}, {{"",""}}),
            std::string_view("a\0\0\0", 4));

  EXPECT_EQ(key_insert(std::string_view{"p\0", 2}, {{"a","b"}}),
            std::string_view("p\0a\0b\0", 6));

  EXPECT_EQ(key_insert(std::string_view{"p\0a\0a\0", 6}, {{"a","b"}}),
            std::string_view("p\0a\0b\0", 6));

  EXPECT_EQ(key_insert(std::string_view{"p\0a\0z\0", 6}, {{"a","b"}}),
            std::string_view("p\0a\0b\0", 6));

  EXPECT_EQ(key_insert(std::string_view{"p\0z\0z\0", 6}, {{"a","b"}}),
            std::string_view("p\0a\0b\0z\0z\0", 10));

  EXPECT_EQ(key_insert(std::string_view{"p\0b\0b\0", 6},
                       {{"a","a"}, {"c","c"}}),
            std::string_view("p\0a\0a\0b\0b\0c\0c\0", 14));

  EXPECT_EQ(key_insert(std::string_view{"p\0a\0a\0b\0b\0c\0c\0", 14},
                       {{"z","z"}, {"b","z"}}),
            std::string_view("p\0a\0a\0b\0z\0c\0c\0z\0z\0", 18));
}

TEST(PerfCounters, key_name)
{
  EXPECT_EQ(key_name(""),
            "");
  EXPECT_EQ(key_name({"\0", 1}),
            "");
  EXPECT_EQ(key_name({"perf\0", 5}),
            "perf");
  EXPECT_EQ(key_name({"perf\0\0\0", 7}),
            "perf");
}

TEST(PerfCounters, key_labels)
{
  {
    auto labels = key_labels("");
    EXPECT_EQ(labels.begin(), labels.end());
  }
  {
    auto labels = key_labels({"\0", 1});
    EXPECT_EQ(labels.begin(), labels.end());
  }
  {
    auto labels = key_labels({"perf\0", 5});
    EXPECT_EQ(labels.begin(), labels.end());
  }
  {
    auto labels = key_labels({"\0\0\0", 3});
    ASSERT_EQ(1, std::distance(labels.begin(), labels.end()));
    EXPECT_EQ(label_pair("", ""), *labels.begin());
  }
  {
    auto labels = key_labels({"\0a\0b\0", 5});
    ASSERT_EQ(1, std::distance(labels.begin(), labels.end()));
    EXPECT_EQ(label_pair("a", "b"), *labels.begin());
    EXPECT_EQ(std::next(labels.begin()), labels.end());
  }
  {
    auto labels = key_labels({"\0a\0b\0c\0d\0", 9});
    ASSERT_EQ(2, std::distance(labels.begin(), labels.end()));
    EXPECT_EQ(label_pair("a", "b"), *labels.begin());
    EXPECT_EQ(label_pair("c", "d"), *std::next(labels.begin()));
    EXPECT_EQ(std::next(labels.begin(), 2), labels.end());
  }
}

} // namespace ceph::perf_counters
