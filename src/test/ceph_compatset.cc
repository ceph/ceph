// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <fstream>
#include <iostream>
#include <errno.h>
#include <sys/stat.h>
#include <signal.h>
#include <ctype.h>
#include <boost/scoped_ptr.hpp>
#include <string>

#include "include/types.h"
#include "include/compat.h"

//#undef assert
//#define	assert(foo) if (!(foo)) abort();

#include "include/CompatSet.h"

#include "gtest/gtest.h"
#include <vector>

TEST(CephCompatSet, AllSet) {
  CompatSet::FeatureSet compat;
  CompatSet::FeatureSet ro;
  CompatSet::FeatureSet incompat;

  EXPECT_DEATH(compat.insert(CompatSet::Feature(0, "test")), "");
  EXPECT_DEATH(compat.insert(CompatSet::Feature(64, "test")), "");

  for (int i = 1; i < 64; i++) {
    stringstream cname;
    cname << string("c") << i;
    compat.insert(CompatSet::Feature(i,cname.str().c_str()));
    stringstream roname;
    roname << string("r") << i;
    ro.insert(CompatSet::Feature(i,roname.str().c_str()));
    stringstream iname;
    iname << string("i") << i;
    incompat.insert(CompatSet::Feature(i,iname.str().c_str()));
  }
  CompatSet tcs(compat, ro, incompat);

  //cout << tcs << std::endl;

  //Due to a workaround for a bug bit 0 is always set even though it is
  //not a legal feature.
  EXPECT_EQ(tcs.compat.mask, (uint64_t)0xffffffffffffffff);
  EXPECT_EQ(tcs.ro_compat.mask, (uint64_t)0xffffffffffffffff);
  EXPECT_EQ(tcs.incompat.mask, (uint64_t)0xffffffffffffffff);

  for (int i = 1; i < 64; i++) {
    EXPECT_TRUE(tcs.compat.contains(i));
    stringstream cname;
    cname << string("c") << i;
    EXPECT_TRUE(tcs.compat.contains(CompatSet::Feature(i,cname.str().c_str())));
    tcs.compat.remove(i);

    EXPECT_TRUE(tcs.ro_compat.contains(i));
    stringstream roname;
    roname << string("r") << i;
    EXPECT_TRUE(tcs.ro_compat.contains(CompatSet::Feature(i,roname.str().c_str())));
    tcs.ro_compat.remove(i);

    EXPECT_TRUE(tcs.incompat.contains(i));
    stringstream iname;
    iname << string("i") << i;
    EXPECT_TRUE(tcs.incompat.contains(CompatSet::Feature(i,iname.str().c_str())));
    tcs.incompat.remove(i);
  }
  //Due to a workaround for a bug bit 0 is always set even though it is
  //not a legal feature.
  EXPECT_EQ(tcs.compat.mask, (uint64_t)1);
  EXPECT_TRUE(tcs.compat.names.empty());
  EXPECT_EQ(tcs.ro_compat.mask, (uint64_t)1);
  EXPECT_TRUE(tcs.ro_compat.names.empty());
  EXPECT_EQ(tcs.incompat.mask, (uint64_t)1);
  EXPECT_TRUE(tcs.incompat.names.empty());
}

TEST(CephCompatSet, other) {
  CompatSet s1, s2, s1dup;

  s1.compat.insert(CompatSet::Feature(1, "c1"));
  s1.compat.insert(CompatSet::Feature(2, "c2"));
  s1.compat.insert(CompatSet::Feature(32, "c32"));
  s1.ro_compat.insert(CompatSet::Feature(63, "r63"));
  s1.incompat.insert(CompatSet::Feature(1, "i1"));

  s2.compat.insert(CompatSet::Feature(1, "c1"));
  s2.compat.insert(CompatSet::Feature(32, "c32"));
  s2.ro_compat.insert(CompatSet::Feature(63, "r63"));
  s2.incompat.insert(CompatSet::Feature(1, "i1"));

  s1dup = s1;

  //Check exact match
  EXPECT_EQ(s1.compare(s1dup), 0);

  //Check superset
  EXPECT_EQ(s1.compare(s2), 1);

  //Check missing features
  EXPECT_EQ(s2.compare(s1), -1);

  CompatSet diff = s2.unsupported(s1);
  EXPECT_EQ(diff.compat.mask, (uint64_t)1<<2 | 1);
  EXPECT_EQ(diff.ro_compat.mask, (uint64_t)1);
  EXPECT_EQ(diff.incompat.mask, (uint64_t)1);

  CompatSet s3 = s1;
  s3.incompat.insert(CompatSet::Feature(4, "i4"));

  diff = s1.unsupported(s3);
  EXPECT_EQ(diff.compat.mask, (uint64_t)1);
  EXPECT_EQ(diff.ro_compat.mask, (uint64_t)1);
  EXPECT_EQ(diff.incompat.mask, (uint64_t)1<<4 | 1);
}

TEST(CephCompatSet, merge) {
  CompatSet s1, s2, s1dup, s2dup;

  s1.compat.insert(CompatSet::Feature(1, "c1"));
  s1.compat.insert(CompatSet::Feature(2, "c2"));
  s1.compat.insert(CompatSet::Feature(32, "c32"));
  s1.ro_compat.insert(CompatSet::Feature(63, "r63"));
  s1.incompat.insert(CompatSet::Feature(1, "i1"));

  s1dup = s1;

  s2.compat.insert(CompatSet::Feature(1, "c1"));
  s2.compat.insert(CompatSet::Feature(32, "c32"));
  s2.ro_compat.insert(CompatSet::Feature(1, "r1"));
  s2.ro_compat.insert(CompatSet::Feature(63, "r63"));
  s2.incompat.insert(CompatSet::Feature(1, "i1"));

  s2dup = s2;

  //Nothing to merge if they are the same
  EXPECT_FALSE(s1.merge(s1dup));
  EXPECT_FALSE(s2.merge(s2dup));

  EXPECT_TRUE(s1.merge(s2));
  EXPECT_EQ(s1.compat.mask, (uint64_t)1<<1 | (uint64_t)1<<2 | (uint64_t)1<<32 | 1);
  EXPECT_EQ(s1.ro_compat.mask, (uint64_t)1<<1 | (uint64_t)1<<63 | 1);
  EXPECT_EQ(s1.incompat.mask, (uint64_t)1<<1 | 1);

  EXPECT_TRUE(s2.merge(s1dup));
  EXPECT_EQ(s2.compat.mask, (uint64_t)1<<1 | (uint64_t)1<<2 | (uint64_t)1<<32 | 1);
  EXPECT_EQ(s2.ro_compat.mask, (uint64_t)1<<1 | (uint64_t)1<<63 | 1);
  EXPECT_EQ(s2.incompat.mask, (uint64_t)1<<1 | 1);
}
