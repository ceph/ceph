// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "os/bluestore/bluestore_types.h"
#include "gtest/gtest.h"
#include "include/stringify.h"

#include <sstream>

TEST(bluestore_extent_ref_map_t, add)
{
  bluestore_extent_ref_map_t m;
  m.get(10, 10);
  ASSERT_EQ(1u, m.ref_map.size());
  cout << m << std::endl;
  m.get(20, 10);
  cout << m << std::endl;
  ASSERT_EQ(1u, m.ref_map.size());
  ASSERT_EQ(20u, m.ref_map[10].length);
  ASSERT_EQ(1u, m.ref_map[10].refs);
  m.get(40, 10);
  cout << m << std::endl;
  ASSERT_EQ(2u, m.ref_map.size());
  m.get(30, 10);
  cout << m << std::endl;
  ASSERT_EQ(1u, m.ref_map.size());
  m.get(50, 10);
  cout << m << std::endl;
  ASSERT_EQ(1u, m.ref_map.size());
  m.get(5, 5);
  cout << m << std::endl;
  ASSERT_EQ(1u, m.ref_map.size());
}

TEST(bluestore_extent_ref_map_t, get)
{
  bluestore_extent_ref_map_t m;
  m.get(00, 30);
  cout << m << std::endl;
  m.get(10, 10);
  cout << m << std::endl;
  ASSERT_EQ(3u, m.ref_map.size());
  ASSERT_EQ(10u, m.ref_map[0].length);
  ASSERT_EQ(1u, m.ref_map[0].refs);
  ASSERT_EQ(10u, m.ref_map[10].length);
  ASSERT_EQ(2u, m.ref_map[10].refs);
  ASSERT_EQ(10u, m.ref_map[20].length);
  ASSERT_EQ(1u, m.ref_map[20].refs);
  m.get(20, 5);
  cout << m << std::endl;
  ASSERT_EQ(3u, m.ref_map.size());
  ASSERT_EQ(15u, m.ref_map[10].length);
  ASSERT_EQ(2u, m.ref_map[10].refs);
  ASSERT_EQ(5u, m.ref_map[25].length);
  ASSERT_EQ(1u, m.ref_map[25].refs);
  m.get(5, 20);
  cout << m << std::endl;
  ASSERT_EQ(4u, m.ref_map.size());
  ASSERT_EQ(5u, m.ref_map[0].length);
  ASSERT_EQ(1u, m.ref_map[0].refs);
  ASSERT_EQ(5u, m.ref_map[5].length);
  ASSERT_EQ(2u, m.ref_map[5].refs);
  ASSERT_EQ(15u, m.ref_map[10].length);
  ASSERT_EQ(3u, m.ref_map[10].refs);
  ASSERT_EQ(5u, m.ref_map[25].length);
  ASSERT_EQ(1u, m.ref_map[25].refs);
  m.get(25, 3);
  cout << m << std::endl;
  ASSERT_EQ(5u, m.ref_map.size());
  ASSERT_EQ(5u, m.ref_map[0].length);
  ASSERT_EQ(1u, m.ref_map[0].refs);
  ASSERT_EQ(5u, m.ref_map[5].length);
  ASSERT_EQ(2u, m.ref_map[5].refs);
  ASSERT_EQ(15u, m.ref_map[10].length);
  ASSERT_EQ(3u, m.ref_map[10].refs);
  ASSERT_EQ(3u, m.ref_map[25].length);
  ASSERT_EQ(2u, m.ref_map[25].refs);
  ASSERT_EQ(2u, m.ref_map[28].length);
  ASSERT_EQ(1u, m.ref_map[28].refs);
}

TEST(bluestore_extent_ref_map_t, put)
{
  bluestore_extent_ref_map_t m;
  vector<bluestore_pextent_t> r;
  m.get(10, 30);
  m.put(10, 30, &r);
  cout << m << " " << r << std::endl;
  ASSERT_EQ(0u, m.ref_map.size());
  ASSERT_EQ(1u, r.size());
  ASSERT_EQ(10u, r[0].offset);
  ASSERT_EQ(30u, r[0].length);
  r.clear();
  m.get(10, 30);
  m.get(20, 10);
  m.put(10, 30, &r);
  cout << m << " " << r << std::endl;
  ASSERT_EQ(1u, m.ref_map.size());
  ASSERT_EQ(10u, m.ref_map[20].length);
  ASSERT_EQ(1u, m.ref_map[20].refs);
  ASSERT_EQ(2u, r.size());
  ASSERT_EQ(10u, r[0].offset);
  ASSERT_EQ(10u, r[0].length);
  ASSERT_EQ(30u, r[1].offset);
  ASSERT_EQ(10u, r[1].length);
  r.clear();
  m.get(30, 10);
  m.get(30, 10);
  m.put(20, 15, &r);
  cout << m << " " << r << std::endl;
  ASSERT_EQ(2u, m.ref_map.size());
  ASSERT_EQ(5u, m.ref_map[30].length);
  ASSERT_EQ(1u, m.ref_map[30].refs);
  ASSERT_EQ(5u, m.ref_map[35].length);
  ASSERT_EQ(2u, m.ref_map[35].refs);
  ASSERT_EQ(1u, r.size());
  ASSERT_EQ(20u, r[0].offset);
  ASSERT_EQ(10u, r[0].length);
  r.clear();
  m.put(33, 5, &r);
  cout << m << " " << r << std::endl;
  ASSERT_EQ(3u, m.ref_map.size());
  ASSERT_EQ(3u, m.ref_map[30].length);
  ASSERT_EQ(1u, m.ref_map[30].refs);
  ASSERT_EQ(3u, m.ref_map[35].length);
  ASSERT_EQ(1u, m.ref_map[35].refs);
  ASSERT_EQ(2u, m.ref_map[38].length);
  ASSERT_EQ(2u, m.ref_map[38].refs);
  ASSERT_EQ(1u, r.size());
  ASSERT_EQ(33u, r[0].offset);
  ASSERT_EQ(2u, r[0].length);
}

TEST(bluestore_extent_ref_map_t, contains)
{
  bluestore_extent_ref_map_t m;
  m.get(10, 30);
  ASSERT_TRUE(m.contains(10, 30));
  ASSERT_TRUE(m.contains(10, 10));
  ASSERT_TRUE(m.contains(30, 10));
  ASSERT_FALSE(m.contains(0, 10));
  ASSERT_FALSE(m.contains(0, 20));
  ASSERT_FALSE(m.contains(0, 100));
  ASSERT_FALSE(m.contains(40, 10));
  ASSERT_FALSE(m.contains(30, 11));
  m.get(40, 10);
  m.get(40, 10);
  ASSERT_TRUE(m.contains(30, 11));
  ASSERT_TRUE(m.contains(30, 20));
  ASSERT_TRUE(m.contains(10, 40));
  ASSERT_FALSE(m.contains(0, 50));
  ASSERT_FALSE(m.contains(40, 20));
  m.get(60, 100);
  ASSERT_TRUE(m.contains(60, 10));
  ASSERT_TRUE(m.contains(40, 10));
  ASSERT_FALSE(m.contains(40, 11));
  ASSERT_FALSE(m.contains(40, 20));
  ASSERT_FALSE(m.contains(40, 30));
  ASSERT_FALSE(m.contains(40, 3000));
  ASSERT_FALSE(m.contains(4000, 30));
}

TEST(bluestore_extent_ref_map_t, intersects)
{
  bluestore_extent_ref_map_t m;
  m.get(10, 30);
  ASSERT_TRUE(m.intersects(10, 30));
  ASSERT_TRUE(m.intersects(0, 11));
  ASSERT_TRUE(m.intersects(10, 40));
  ASSERT_TRUE(m.intersects(15, 40));
  ASSERT_FALSE(m.intersects(0, 10));
  ASSERT_FALSE(m.intersects(0, 5));
  ASSERT_FALSE(m.intersects(40, 20));
  ASSERT_FALSE(m.intersects(41, 20));
  m.get(40, 10);
  m.get(40, 10);
  ASSERT_TRUE(m.intersects(0, 100));
  ASSERT_TRUE(m.intersects(10, 35));
  ASSERT_TRUE(m.intersects(45, 10));
  ASSERT_FALSE(m.intersects(50, 5));
  m.get(60, 100);
  ASSERT_TRUE(m.intersects(45, 10));
  ASSERT_TRUE(m.intersects(55, 10));
  ASSERT_TRUE(m.intersects(50, 11));
  ASSERT_FALSE(m.intersects(50, 10));
  ASSERT_FALSE(m.intersects(51, 9));
  ASSERT_FALSE(m.intersects(55, 1));
}

TEST(bluestore_onode_t, punch_hole)
{
  bluestore_onode_t on;
  vector<bluestore_lextent_t> r;
  on.extent_map[0] = bluestore_lextent_t(1, 0, 100, 0);
  on.extent_map[100] = bluestore_lextent_t(2, 0, 100, 0);

  on.punch_hole(0, 100, &r);
  ASSERT_EQ(1u, on.extent_map.size());
  ASSERT_EQ(1u, r.size());
  ASSERT_EQ(1, r[0].blob);
  ASSERT_EQ(0u, r[0].offset);
  ASSERT_EQ(100u, r[0].length);
  r.clear();

  on.punch_hole(150, 10, &r);
  ASSERT_EQ(2u, on.extent_map.size());
  ASSERT_EQ(100u, on.extent_map.begin()->first);
  ASSERT_EQ(0u, on.extent_map.begin()->second.offset);
  ASSERT_EQ(50u, on.extent_map.begin()->second.length);
  ASSERT_EQ(160u, on.extent_map.rbegin()->first);
  ASSERT_EQ(60u, on.extent_map.rbegin()->second.offset);
  ASSERT_EQ(40u, on.extent_map.rbegin()->second.length);
  ASSERT_EQ(1u, r.size());
  ASSERT_EQ(2, r[0].blob);
  ASSERT_EQ(50u, r[0].offset);
  ASSERT_EQ(10u, r[0].length);
  r.clear();

  on.punch_hole(140, 20, &r);
  ASSERT_EQ(2u, on.extent_map.size());
  ASSERT_EQ(100u, on.extent_map.begin()->first);
  ASSERT_EQ(0u, on.extent_map.begin()->second.offset);
  ASSERT_EQ(40u, on.extent_map.begin()->second.length);
  ASSERT_EQ(160u, on.extent_map.rbegin()->first);
  ASSERT_EQ(60u, on.extent_map.rbegin()->second.offset);
  ASSERT_EQ(40u, on.extent_map.rbegin()->second.length);
  ASSERT_EQ(1u, r.size());
  ASSERT_EQ(2, r[0].blob);
  ASSERT_EQ(40u, r[0].offset);
  ASSERT_EQ(10u, r[0].length);
  r.clear();

  on.punch_hole(130, 40, &r);
  ASSERT_EQ(2u, on.extent_map.size());
  ASSERT_EQ(100u, on.extent_map.begin()->first);
  ASSERT_EQ(0u, on.extent_map.begin()->second.offset);
  ASSERT_EQ(30u, on.extent_map.begin()->second.length);
  ASSERT_EQ(170u, on.extent_map.rbegin()->first);
  ASSERT_EQ(70u, on.extent_map.rbegin()->second.offset);
  ASSERT_EQ(30u, on.extent_map.rbegin()->second.length);
  ASSERT_EQ(2u, r.size());
  ASSERT_EQ(2, r[0].blob);
  ASSERT_EQ(30u, r[0].offset);
  ASSERT_EQ(10u, r[0].length);
  ASSERT_EQ(2, r[1].blob);
  ASSERT_EQ(60u, r[1].offset);
  ASSERT_EQ(10u, r[1].length);
  r.clear();

  on.punch_hole(110, 10, &r);
  ASSERT_EQ(3u, on.extent_map.size());
  ASSERT_EQ(100u, on.extent_map.begin()->first);
  ASSERT_EQ(0u, on.extent_map.begin()->second.offset);
  ASSERT_EQ(10u, on.extent_map.begin()->second.length);
  ASSERT_EQ(20u, on.extent_map[120].offset);
  ASSERT_EQ(10u, on.extent_map[120].length);
  ASSERT_EQ(170u, on.extent_map.rbegin()->first);
  ASSERT_EQ(70u, on.extent_map.rbegin()->second.offset);
  ASSERT_EQ(30u, on.extent_map.rbegin()->second.length);
  ASSERT_EQ(1u, r.size());
  ASSERT_EQ(2, r[0].blob);
  ASSERT_EQ(10u, r[0].offset);
  ASSERT_EQ(10u, r[0].length);
  r.clear();

  on.punch_hole(0, 1000, &r);
  ASSERT_EQ(0u, on.extent_map.size());
  ASSERT_EQ(3u, r.size());
  ASSERT_EQ(2, r[0].blob);
  ASSERT_EQ(0u, r[0].offset);
  ASSERT_EQ(10u, r[0].length);
  ASSERT_EQ(2, r[1].blob);
  ASSERT_EQ(20u, r[1].offset);
  ASSERT_EQ(10u, r[1].length);
  ASSERT_EQ(2, r[2].blob);
  ASSERT_EQ(70u, r[2].offset);
  ASSERT_EQ(30u, r[2].length);
  r.clear();
}
