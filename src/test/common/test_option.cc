// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include <string.h>
#include <errno.h>
#include <stdlib.h>

#include <gtest/gtest.h>

#include "common/options.h"

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

/*
 * Local Variables:
 * compile-command: "cd ../../../build ;
 *   ninja unittest_option && bin/unittest_option
 *   "
 * End:
 */
