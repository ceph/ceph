// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab expandtab

#include <string.h>
#include <errno.h>
#include <stdlib.h>

#include <gtest/gtest.h>

#include "common/options.h"

using namespace std;

TEST(Option, validate_min_max)
{
  auto opt = Option{"foo", Option::TYPE_MILLISECS, Option::LEVEL_ADVANCED}
    .set_default(42)
    .set_min_max(10, 128);
  struct test_t {
    unsigned new_val;
    int expected_retval;
  };
  test_t tests[] =
    {{9, -EINVAL},
     {10, 0},
     {11, 0},
     {128, 0},
     {1024, -EINVAL}
  };
  for (auto& test : tests) {
    Option::value_t new_value = std::chrono::milliseconds{test.new_val};
    std::string err;
    GTEST_ASSERT_EQ(test.expected_retval, opt.validate(new_value, &err));
  }
}

TEST(Option, parse)
{
  auto opt = Option{"foo", Option::TYPE_MILLISECS, Option::LEVEL_ADVANCED}
    .set_default(42)
    .set_min_max(10, 128);
  struct test_t {
    string new_val;
    int expected_retval;
    unsigned expected_parsed_val;
  };
  test_t tests[] =
    {{"9", -EINVAL, 0},
     {"10", 0, 10},
     {"11", 0, 11},
     {"128", 0, 128},
     {"1024", -EINVAL, 0}
  };
  for (auto& test : tests) {
    Option::value_t parsed_val;
    std::string err;
    GTEST_ASSERT_EQ(test.expected_retval,
                    opt.parse_value(test.new_val, &parsed_val, &err));
    if (test.expected_retval == 0) {
      Option::value_t expected_parsed_val =
        std::chrono::milliseconds{test.expected_parsed_val};
      GTEST_ASSERT_EQ(parsed_val, expected_parsed_val);
    }
  }
}

TEST(Option, validator_runs_after_parsing)
{
  bool called = false;
  auto opt = Option{"foo", Option::TYPE_INT, Option::LEVEL_ADVANCED}
    .set_validator([&](const Option::value_t &value, std::string *err) {
      called = true;
      if (std::get<int64_t>(value) % 2 != 0) {
        *err = "value must be even";
        return -EDOM;
      }
      return 0;
    });
  Option::value_t parsed;
  std::string err;
  EXPECT_EQ(opt.parse_value("invalid", &parsed, &err), -EINVAL);
  EXPECT_FALSE(called);
  ASSERT_EQ(opt.parse_value("2K", &parsed, &err), 0);
  EXPECT_TRUE(called);
  EXPECT_EQ(std::get<int64_t>(parsed), 2000);
  EXPECT_EQ(opt.parse_value("3", &parsed, &err), -EDOM);
  EXPECT_EQ(err, "value must be even");
}

TEST(Option, validator_normalizes_before_enum_check)
{
  auto opt = Option{"foo", Option::TYPE_STR, Option::LEVEL_ADVANCED}
    .set_enum_allowed({"blue"})
    .set_validator([](Option::value_t &value, std::string*) {
      auto &str = std::get<std::string>(value);
      if (str == "BLUE") {
        str = "blue";
      }
      return 0;
    });
  Option::value_t parsed;
  std::string err, normalized;
  ASSERT_EQ(opt.parse_value("BLUE", &parsed, &err, &normalized), 0);
  EXPECT_EQ(normalized, "blue");
  EXPECT_EQ(opt.parse_value("green", &parsed, &err), -EINVAL);
  EXPECT_NE(err.find("permitted values"), std::string::npos);
}

/*
 * Local Variables:
 * compile-command: "cd ../../../build ;
 *   ninja unittest_option && bin/unittest_option
 *   "
 * End:
 */
