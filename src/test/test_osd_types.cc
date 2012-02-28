// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/types.h"
#include "osd/osd_types.h"
#include "gtest/gtest.h"

#include <sstream>


TEST(pg_t, split)
{
  pg_t pgid(0, 0, -1);
  set<pg_t> s;
  bool b;

  s.clear();
  b = pgid.is_split(1, 1, &s);
  ASSERT_TRUE(!b);

  s.clear();
  b = pgid.is_split(2, 4, NULL);
  ASSERT_TRUE(b);
  b = pgid.is_split(2, 4, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(2, 0, -1)));

  s.clear();
  b = pgid.is_split(2, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(3u, s.size());
  ASSERT_TRUE(s.count(pg_t(2, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(4, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(6, 0, -1)));

  s.clear();
  b = pgid.is_split(3, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(4, 0, -1)));

  s.clear();
  b = pgid.is_split(6, 8, NULL);
  ASSERT_TRUE(!b);
  b = pgid.is_split(6, 8, &s);
  ASSERT_TRUE(!b);
  ASSERT_EQ(0u, s.size());

  pgid = pg_t(1, 0, -1);
  
  s.clear();
  b = pgid.is_split(2, 4, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(3, 0, -1)));

  s.clear();
  b = pgid.is_split(2, 6, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(2u, s.size());
  ASSERT_TRUE(s.count(pg_t(3, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(5, 0, -1)));

  s.clear();
  b = pgid.is_split(2, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(3u, s.size());
  ASSERT_TRUE(s.count(pg_t(3, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(5, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(7, 0, -1)));

  s.clear();
  b = pgid.is_split(4, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(5, 0, -1)));

  s.clear();
  b = pgid.is_split(3, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(3u, s.size());
  ASSERT_TRUE(s.count(pg_t(3, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(5, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(7, 0, -1)));

  s.clear();
  b = pgid.is_split(6, 8, &s);
  ASSERT_TRUE(!b);
  ASSERT_EQ(0u, s.size());

  pgid = pg_t(3, 0, -1);

  s.clear();
  b = pgid.is_split(7, 8, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(7, 0, -1)));

  s.clear();
  b = pgid.is_split(7, 12, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(2u, s.size());
  ASSERT_TRUE(s.count(pg_t(7, 0, -1)));
  ASSERT_TRUE(s.count(pg_t(11, 0, -1)));

  s.clear();
  b = pgid.is_split(7, 11, &s);
  ASSERT_TRUE(b);
  ASSERT_EQ(1u, s.size());
  ASSERT_TRUE(s.count(pg_t(7, 0, -1)));

}
