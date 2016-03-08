// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include <iostream>
#include "mon/mon_types.h"

#include "gtest/gtest.h"

TEST(mon_features, supported_v_persistent) {

  mon_feature_t supported = ceph::features::mon::get_supported();
  mon_feature_t persistent = ceph::features::mon::get_persistent();

  ASSERT_EQ(supported.intersection(persistent), persistent);
  ASSERT_TRUE(supported.contains_all(persistent));

  mon_feature_t diff = supported.diff(persistent);
  ASSERT_TRUE((persistent | diff) == supported);
  ASSERT_TRUE((supported & persistent) == persistent);
}

TEST(mon_features, binary_ops) {

  mon_feature_t FEATURE_NONE(0ULL);
  mon_feature_t FEATURE_A((1ULL << 1));
  mon_feature_t FEATURE_B((1ULL << 2));
  mon_feature_t FEATURE_C((1ULL << 3));
  mon_feature_t FEATURE_D((1ULL << 4));

  mon_feature_t FEATURE_ALL(
      FEATURE_A | FEATURE_B |
      FEATURE_C | FEATURE_D
  );

  mon_feature_t foo(FEATURE_A|FEATURE_B);
  mon_feature_t bar(FEATURE_C|FEATURE_D);

  ASSERT_EQ(foo, FEATURE_A|FEATURE_B);
  ASSERT_EQ(bar, FEATURE_C|FEATURE_D);

  ASSERT_NE(foo, FEATURE_C);
  ASSERT_NE(bar, FEATURE_B);
  ASSERT_NE(foo, FEATURE_NONE);
  ASSERT_NE(bar, FEATURE_NONE);

  ASSERT_FALSE(foo.empty());
  ASSERT_FALSE(bar.empty());
  ASSERT_TRUE(FEATURE_NONE.empty());

  ASSERT_EQ((foo ^ bar), FEATURE_ALL);
  ASSERT_EQ((foo & bar), FEATURE_NONE);

  mon_feature_t baz = foo;
  ASSERT_EQ(baz, foo);

  baz |= bar;
  ASSERT_EQ(baz, FEATURE_ALL);
  baz ^= foo;
  ASSERT_EQ(baz, bar);

  baz |= FEATURE_A;
  ASSERT_EQ(baz & FEATURE_C, FEATURE_C);
  ASSERT_EQ(baz & (FEATURE_A|FEATURE_D), (FEATURE_A|FEATURE_D));
  ASSERT_EQ((baz ^ foo), FEATURE_B|FEATURE_C|FEATURE_D);
}

TEST(mon_features, set_funcs) {

  mon_feature_t FEATURE_NONE(0ULL);
  mon_feature_t FEATURE_A((1ULL << 1));
  mon_feature_t FEATURE_B((1ULL << 2));
  mon_feature_t FEATURE_C((1ULL << 3));
  mon_feature_t FEATURE_D((1ULL << 4));

  mon_feature_t FEATURE_ALL(
      FEATURE_A | FEATURE_B |
      FEATURE_C | FEATURE_D
  );

  mon_feature_t foo(FEATURE_A|FEATURE_B);
  mon_feature_t bar(FEATURE_C|FEATURE_D);

  ASSERT_TRUE(FEATURE_ALL.contains_all(foo));
  ASSERT_TRUE(FEATURE_ALL.contains_all(bar));
  ASSERT_TRUE(FEATURE_ALL.contains_all(foo|bar));

  ASSERT_EQ(foo.diff(bar), foo);
  ASSERT_EQ(bar.diff(foo), bar);
  ASSERT_EQ(FEATURE_ALL.diff(foo), bar);
  ASSERT_EQ(FEATURE_ALL.diff(bar), foo);

  ASSERT_TRUE(foo.contains_any(FEATURE_A|bar));
  ASSERT_TRUE(bar.contains_any(FEATURE_ALL));
  ASSERT_TRUE(FEATURE_ALL.contains_any(foo));

  mon_feature_t FEATURE_X((1ULL << 10));

  ASSERT_FALSE(FEATURE_ALL.contains_any(FEATURE_X));
  ASSERT_FALSE(FEATURE_ALL.contains_all(FEATURE_X));
  ASSERT_EQ(FEATURE_ALL.diff(FEATURE_X), FEATURE_ALL);

  ASSERT_EQ(foo.intersection(FEATURE_ALL), foo);
  ASSERT_EQ(bar.intersection(FEATURE_ALL), bar);
}
