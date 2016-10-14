// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * Author: Greg Farnum <greg@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "gtest/gtest.h"
#include "mds/mdstypes.h"
#include "mds/inode_backtrace.h"

TEST(inode_t, compare_equal)
{
  inode_t foo;
  inode_t bar;
  int compare_r;
  bool divergent;
  compare_r = foo.compare(bar, &divergent);
  EXPECT_EQ(0, compare_r);
  EXPECT_FALSE(divergent);
  compare_r = bar.compare(foo, &divergent);
  EXPECT_EQ(0, compare_r);
  EXPECT_FALSE(divergent);


  foo.ino = 1234;
  foo.ctime.set_from_double(10.0);
  foo.mode = 0777;
  foo.uid = 42;
  foo.gid = 43;
  foo.nlink = 3;
  foo.version = 3;

  bar = foo;
  compare_r = foo.compare(bar, &divergent);
  EXPECT_EQ(0, compare_r);
  EXPECT_FALSE(divergent);
  compare_r = bar.compare(foo, &divergent);
  EXPECT_EQ(0, compare_r);
  EXPECT_FALSE(divergent);
}

TEST(inode_t, compare_aged)
{
  inode_t foo;
  inode_t bar;

  foo.ino = 1234;
  foo.ctime.set_from_double(10.0);
  foo.mode = 0777;
  foo.uid = 42;
  foo.gid = 43;
  foo.nlink = 3;
  foo.version = 3;
  foo.rstat.version = 1;

  bar = foo;
  bar.version = 2;

  int compare_r;
  bool divergent;
  compare_r = foo.compare(bar, &divergent);
  EXPECT_EQ(1, compare_r);
  EXPECT_FALSE(divergent);
  compare_r = bar.compare(foo, &divergent);
  EXPECT_EQ(-1, compare_r);
  EXPECT_FALSE(divergent);
}

TEST(inode_t, compare_divergent)
{
  inode_t foo;
  inode_t bar;

  foo.ino = 1234;
  foo.ctime.set_from_double(10.0);
  foo.mode = 0777;
  foo.uid = 42;
  foo.gid = 43;
  foo.nlink = 3;
  foo.version = 3;
  foo.rstat.version = 1;

  bar = foo;
  bar.version = 2;
  bar.rstat.version = 2;

  int compare_r;
  bool divergent;
  compare_r = foo.compare(bar, &divergent);
  EXPECT_EQ(1, compare_r);
  EXPECT_TRUE(divergent);
  compare_r = bar.compare(foo, &divergent);
  EXPECT_EQ(-1, compare_r);
  EXPECT_TRUE(divergent);
}

TEST(inode_backtrace_t, compare_equal)
{
  inode_backtrace_t foo;
  inode_backtrace_t bar;
  
  foo.ino = 1234;
  foo.pool = 12;
  foo.old_pools.insert(10);
  foo.old_pools.insert(5);

  inode_backpointer_t foop;
  foop.dirino = 3;
  foop.dname = "l3";
  foop.version = 15;
  foo.ancestors.push_back(foop);
  foop.dirino = 2;
  foop.dname = "l2";
  foop.version = 10;
  foo.ancestors.push_back(foop);
  foop.dirino = 1;
  foop.dname = "l1";
  foop.version = 25;
  foo.ancestors.push_back(foop);
 
  bar = foo;
  
  int compare_r;
  bool equivalent;
  bool divergent;

  compare_r = foo.compare(bar, &equivalent, &divergent);
  EXPECT_EQ(0, compare_r);
  EXPECT_TRUE(equivalent);
  EXPECT_FALSE(divergent);
}

TEST(inode_backtrace_t, compare_newer)
{
  inode_backtrace_t foo;
  inode_backtrace_t bar;

  foo.ino = 1234;
  foo.pool = 12;
  foo.old_pools.insert(10);
  foo.old_pools.insert(5);

  bar.ino = 1234;
  bar.pool = 12;
  bar.old_pools.insert(10);

  inode_backpointer_t foop;
  foop.dirino = 3;
  foop.dname = "l3";
  foop.version = 15;
  foo.ancestors.push_back(foop);
  foop.version = 14;
  bar.ancestors.push_back(foop);

  foop.dirino = 2;
  foop.dname = "l2";
  foop.version = 10;
  foo.ancestors.push_back(foop);
  foop.version = 9;
  bar.ancestors.push_back(foop);

  foop.dirino = 1;
  foop.dname = "l1";
  foop.version = 25;
  foo.ancestors.push_back(foop);
  bar.ancestors.push_back(foop);

  int compare_r;
  bool equivalent;
  bool divergent;

  compare_r = foo.compare(bar, &equivalent, &divergent);
  EXPECT_EQ(1, compare_r);
  EXPECT_TRUE(equivalent);
  EXPECT_FALSE(divergent);

  compare_r = bar.compare(foo, &equivalent, &divergent);
  EXPECT_EQ(-1, compare_r);
  EXPECT_TRUE(equivalent);
  EXPECT_FALSE(divergent);

  bar.ancestors.back().dirino = 75;
  bar.ancestors.back().dname = "l1-old";
  bar.ancestors.back().version = 70;

  compare_r = foo.compare(bar, &equivalent, &divergent);
  EXPECT_EQ(1, compare_r);
  EXPECT_FALSE(equivalent);
  EXPECT_FALSE(divergent);

  compare_r = bar.compare(foo, &equivalent, &divergent);
  EXPECT_EQ(-1, compare_r);
  EXPECT_FALSE(equivalent);
  EXPECT_FALSE(divergent);
}

TEST(inode_backtrace_t, compare_divergent)
{
  inode_backtrace_t foo;
  inode_backtrace_t bar;

  foo.ino = 1234;
  foo.pool = 12;
  foo.old_pools.insert(10);
  foo.old_pools.insert(5);

  bar.ino = 1234;
  bar.pool = 12;
  bar.old_pools.insert(10);

  inode_backpointer_t foop;
  foop.dirino = 3;
  foop.dname = "l3";
  foop.version = 15;
  foo.ancestors.push_back(foop);
  foop.version = 17;
  bar.ancestors.push_back(foop);

  foop.dirino = 2;
  foop.dname = "l2";
  foop.version = 10;
  foo.ancestors.push_back(foop);
  foop.version = 9;
  bar.ancestors.push_back(foop);

  foop.dirino = 1;
  foop.dname = "l1";
  foop.version = 25;
  foo.ancestors.push_back(foop);
  bar.ancestors.push_back(foop);

  int compare_r;
  bool equivalent;
  bool divergent;

  compare_r = foo.compare(bar, &equivalent, &divergent);
  EXPECT_EQ(1, compare_r);
  EXPECT_TRUE(divergent);
  compare_r = bar.compare(foo, &equivalent, &divergent);
  EXPECT_EQ(-1, compare_r);
  EXPECT_TRUE(divergent);
}
