// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/stddev.h"
#include "gtest/gtest.h"

TEST(Stddev, Normal) {
  Stddev stddev;
  for (int i = 1; i <= 10; i++) {
    stddev.enter(i);
  }
  EXPECT_DOUBLE_EQ(3.0276503540974917, stddev.value());
}

TEST(Stddev, Empty) {
  Stddev stddev;
  EXPECT_DOUBLE_EQ(0, stddev.value());
}

TEST(Stddev, Single) {
  Stddev stddev;
  stddev.enter(1);
  EXPECT_DOUBLE_EQ(0, stddev.value());
}

TEST(Stddev, Equal) {
  Stddev stddev;
  for (int i = 1; i <= 10; i++) {
    stddev.enter(1);
  }
  EXPECT_DOUBLE_EQ(0, stddev.value());
}
