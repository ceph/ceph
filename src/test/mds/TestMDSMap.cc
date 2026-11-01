// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Ceph contributors
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "gtest/gtest.h"
#include "mds/MDSMap.h"

namespace {

class TestableMDSMap : public MDSMap {
public:
  using MDSMap::in;
  using MDSMap::mds_info;
  using MDSMap::up;
};

} // anonymous namespace

TEST(MDSMap, Issue72895RankSetMismatchIsDegraded)
{
  TestableMDSMap map;
  for (mds_rank_t rank = 0; rank < 5; ++rank) {
    map.in.insert(rank);
  }

  EXPECT_TRUE(map.is_degraded());
  EXPECT_FALSE(map.is_resizeable());
}

TEST(MDSMap, UpRankOutsideInIsDegraded)
{
  TestableMDSMap map;
  map.in.insert(1);
  map.up.emplace(0, 1);

  EXPECT_TRUE(map.is_degraded());
  EXPECT_FALSE(map.is_resizeable());
}

TEST(MDSMap, MatchingInAndUpIsNotDegraded)
{
  TestableMDSMap map;
  map.in.insert(0);
  map.up.emplace(0, 1);
  map.mds_info[mds_gid_t(1)].global_id = mds_gid_t(1);
  map.mds_info[mds_gid_t(1)].rank = 0;
  map.mds_info[mds_gid_t(1)].state = MDSMap::STATE_ACTIVE;

  EXPECT_FALSE(map.is_degraded());
  EXPECT_TRUE(map.is_resizeable());
}
