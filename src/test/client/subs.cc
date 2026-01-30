#include <iostream>
#include <errno.h>
#include "TestPlodClient.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "gtest/gtest-spi.h"
#include "gmock/gmock-matchers.h"
#include "gmock/gmock-more-matchers.h"

TEST_F(TestPlodClient, CheckMonSubscribed) {
  ASSERT_EQ(mc->check_monmap_subed(), true);
  ASSERT_EQ(mc->check_config_subed(), true);
}
