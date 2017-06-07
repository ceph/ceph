// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 * Copyright 2013 Inktank
 */

#include "gtest/gtest.h"
#include "osd/HitSet.h"
#include <iostream>

class HitSetTestStrap {
public:
  HitSet *hitset;

  explicit HitSetTestStrap(HitSet *h) : hitset(h) {}

  void fill(unsigned count) {
    char buf[50];
    for (unsigned i = 0; i < count; ++i) {
      sprintf(buf, "hitsettest_%u", i);
      hobject_t obj(object_t(buf), "", 0, i, 0, "");
      hitset->insert(obj);
    }
    EXPECT_EQ(count, hitset->insert_count());
  }
  void verify_fill(unsigned count) {
    char buf[50];
    for (unsigned i = 0; i < count; ++i) {
      sprintf(buf, "hitsettest_%u", i);
      hobject_t obj(object_t(buf), "", 0, i, 0, "");
      EXPECT_TRUE(hitset->contains(obj));
    }
  }

};

class BloomHitSetTest : public testing::Test, public HitSetTestStrap {
public:

  BloomHitSetTest() : HitSetTestStrap(new HitSet(new BloomHitSet)) {}

  void rebuild(double fp, uint64_t target, uint64_t seed) {
    BloomHitSet::Params *bparams = new BloomHitSet::Params(fp, target, seed);
    HitSet::Params param(bparams);
    HitSet new_set(param);
    *hitset = new_set;
  }

  BloomHitSet *get_hitset() { return static_cast<BloomHitSet*>(hitset->impl.get()); }
};

TEST_F(BloomHitSetTest, Params) {
  BloomHitSet::Params params(0.01, 100, 5);
  EXPECT_EQ(.01, params.get_fpp());
  EXPECT_EQ((unsigned)100, params.target_size);
  EXPECT_EQ((unsigned)5, params.seed);
  params.set_fpp(0.1);
  EXPECT_EQ(0.1, params.get_fpp());

  bufferlist bl;
  params.encode(bl);
  BloomHitSet::Params p2;
  bufferlist::iterator iter = bl.begin();
  p2.decode(iter);
  EXPECT_EQ(0.1, p2.get_fpp());
  EXPECT_EQ((unsigned)100, p2.target_size);
  EXPECT_EQ((unsigned)5, p2.seed);
}

TEST_F(BloomHitSetTest, Construct) {
  ASSERT_EQ(hitset->impl->get_type(), HitSet::TYPE_BLOOM);
  // success!
}

TEST_F(BloomHitSetTest, Rebuild) {
  rebuild(0.1, 100, 1);
  ASSERT_EQ(hitset->impl->get_type(), HitSet::TYPE_BLOOM);
}

TEST_F(BloomHitSetTest, InsertsMatch) {
  rebuild(0.1, 100, 1);
  fill(50);
  /*
   *  the approx unique count is atrocious on bloom filters. Empirical
   *  evidence suggests the current test will produce a value of 62
   *  regardless of hitset size
   */
  EXPECT_TRUE(hitset->approx_unique_insert_count() >= 50 &&
              hitset->approx_unique_insert_count() <= 62);
  verify_fill(50);
  EXPECT_FALSE(hitset->is_full());
}

TEST_F(BloomHitSetTest, FillsUp) {
  rebuild(0.1, 20, 1);
  fill(20);
  verify_fill(20);
  EXPECT_TRUE(hitset->is_full());
}

TEST_F(BloomHitSetTest, RejectsNoMatch) {
  rebuild(0.001, 100, 1);
  fill(100);
  verify_fill(100);
  EXPECT_TRUE(hitset->is_full());

  char buf[50];
  int matches = 0;
  for (int i = 100; i < 200; ++i) {
    sprintf(buf, "hitsettest_%d", i);
    hobject_t obj(object_t(buf), "", 0, i, 0, "");
    if (hitset->contains(obj))
      ++matches;
  }
  // we set a 1 in 1000 false positive; allow one in our 100
  EXPECT_LT(matches, 2);
}

class ExplicitHashHitSetTest : public testing::Test, public HitSetTestStrap {
public:

  ExplicitHashHitSetTest() : HitSetTestStrap(new HitSet(new ExplicitHashHitSet)) {}

  ExplicitHashHitSet *get_hitset() { return static_cast<ExplicitHashHitSet*>(hitset->impl.get()); }
};

TEST_F(ExplicitHashHitSetTest, Construct) {
  ASSERT_EQ(hitset->impl->get_type(), HitSet::TYPE_EXPLICIT_HASH);
  // success!
}

TEST_F(ExplicitHashHitSetTest, InsertsMatch) {
  fill(50);
  verify_fill(50);
  EXPECT_EQ((unsigned)50, hitset->approx_unique_insert_count());
  EXPECT_FALSE(hitset->is_full());
}

TEST_F(ExplicitHashHitSetTest, RejectsNoMatch) {
  fill(100);
  verify_fill(100);
  EXPECT_FALSE(hitset->is_full());

  char buf[50];
  int matches = 0;
  for (int i = 100; i < 200; ++i) {
    sprintf(buf, "hitsettest_%d", i);
    hobject_t obj(object_t(buf), "", 0, i, 0, "");
    if (hitset->contains(obj)) {
      ++matches;
    }
  }
  EXPECT_EQ(matches, 0);
}

class ExplicitObjectHitSetTest : public testing::Test, public HitSetTestStrap {
public:

  ExplicitObjectHitSetTest() : HitSetTestStrap(new HitSet(new ExplicitObjectHitSet)) {}

  ExplicitObjectHitSet *get_hitset() { return static_cast<ExplicitObjectHitSet*>(hitset->impl.get()); }
};

TEST_F(ExplicitObjectHitSetTest, Construct) {
  ASSERT_EQ(hitset->impl->get_type(), HitSet::TYPE_EXPLICIT_OBJECT);
  // success!
}

TEST_F(ExplicitObjectHitSetTest, InsertsMatch) {
  fill(50);
  verify_fill(50);
  EXPECT_EQ((unsigned)50, hitset->approx_unique_insert_count());
  EXPECT_FALSE(hitset->is_full());
}

TEST_F(ExplicitObjectHitSetTest, RejectsNoMatch) {
  fill(100);
  verify_fill(100);
  EXPECT_FALSE(hitset->is_full());

  char buf[50];
  int matches = 0;
  for (int i = 100; i < 200; ++i) {
    sprintf(buf, "hitsettest_%d", i);
    hobject_t obj(object_t(buf), "", 0, i, 0, "");
    if (hitset->contains(obj)) {
      ++matches;
    }
  }
  EXPECT_EQ(matches, 0);
}
