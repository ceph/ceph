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
  m.add(10, 10);
  ASSERT_EQ(1, m.ref_map.size());
  cout << m << std::endl;
  m.add(20, 10);
  cout << m << std::endl;
  ASSERT_EQ(1, m.ref_map.size());
  ASSERT_EQ(20, m.ref_map[10].length);
  ASSERT_EQ(2, m.ref_map[10].refs);
  m.add(40, 10);
  cout << m << std::endl;
  ASSERT_EQ(2, m.ref_map.size());
  m.add(30, 10);
  cout << m << std::endl;
  ASSERT_EQ(1, m.ref_map.size());
  m.add(50, 10, 3);
  cout << m << std::endl;
  ASSERT_EQ(2, m.ref_map.size());
}

TEST(bluestore_extent_ref_map_t, get)
{
  bluestore_extent_ref_map_t m;
  m.add(00, 30);
  cout << m << std::endl;
  m.get(10, 10);
  cout << m << std::endl;
  ASSERT_EQ(3, m.ref_map.size());
  ASSERT_EQ(10, m.ref_map[0].length);
  ASSERT_EQ(2, m.ref_map[0].refs);
  ASSERT_EQ(10, m.ref_map[10].length);
  ASSERT_EQ(3, m.ref_map[10].refs);
  ASSERT_EQ(10, m.ref_map[20].length);
  ASSERT_EQ(2, m.ref_map[20].refs);
  m.get(20, 5);
  cout << m << std::endl;
  ASSERT_EQ(3, m.ref_map.size());
  ASSERT_EQ(15, m.ref_map[10].length);
  ASSERT_EQ(3, m.ref_map[10].refs);
  ASSERT_EQ(5, m.ref_map[25].length);
  ASSERT_EQ(2, m.ref_map[25].refs);
  m.get(5, 20);
  cout << m << std::endl;
  ASSERT_EQ(4, m.ref_map.size());
  ASSERT_EQ(5, m.ref_map[0].length);
  ASSERT_EQ(2, m.ref_map[0].refs);
  ASSERT_EQ(5, m.ref_map[5].length);
  ASSERT_EQ(3, m.ref_map[5].refs);
  ASSERT_EQ(15, m.ref_map[10].length);
  ASSERT_EQ(4, m.ref_map[10].refs);
  ASSERT_EQ(5, m.ref_map[25].length);
  ASSERT_EQ(2, m.ref_map[25].refs);
  m.get(25, 3);
  cout << m << std::endl;
  ASSERT_EQ(5, m.ref_map.size());
  ASSERT_EQ(5, m.ref_map[0].length);
  ASSERT_EQ(2, m.ref_map[0].refs);
  ASSERT_EQ(5, m.ref_map[5].length);
  ASSERT_EQ(3, m.ref_map[5].refs);
  ASSERT_EQ(15, m.ref_map[10].length);
  ASSERT_EQ(4, m.ref_map[10].refs);
  ASSERT_EQ(3, m.ref_map[25].length);
  ASSERT_EQ(3, m.ref_map[25].refs);
  ASSERT_EQ(2, m.ref_map[28].length);
  ASSERT_EQ(2, m.ref_map[28].refs);
}

TEST(bluestore_extent_ref_map_t, put)
{
  bluestore_extent_ref_map_t m;
  vector<bluestore_extent_t> r;
  m.add(10, 30, 1);
  m.put(10, 30, &r);
  cout << m << " " << r << std::endl;
  ASSERT_EQ(0, m.ref_map.size());
  ASSERT_EQ(1, r.size());
  ASSERT_EQ(10, r[0].offset);
  ASSERT_EQ(30, r[0].length);
  r.clear();
  m.add(10, 30, 1);
  m.get(20, 10);
  m.put(10, 30, &r);
  cout << m << " " << r << std::endl;
  ASSERT_EQ(1, m.ref_map.size());
  ASSERT_EQ(10, m.ref_map[20].length);
  ASSERT_EQ(1, m.ref_map[20].refs);
  ASSERT_EQ(2, r.size());
  ASSERT_EQ(10, r[0].offset);
  ASSERT_EQ(10, r[0].length);
  ASSERT_EQ(30, r[1].offset);
  ASSERT_EQ(10, r[1].length);
  r.clear();
  m.add(30, 10);
  m.put(20, 15, &r);
  cout << m << " " << r << std::endl;
  ASSERT_EQ(2, m.ref_map.size());
  ASSERT_EQ(5, m.ref_map[30].length);
  ASSERT_EQ(1, m.ref_map[30].refs);
  ASSERT_EQ(5, m.ref_map[35].length);
  ASSERT_EQ(2, m.ref_map[35].refs);
  ASSERT_EQ(1, r.size());
  ASSERT_EQ(20, r[0].offset);
  ASSERT_EQ(10, r[0].length);
  r.clear();
  m.put(33, 5, &r);
  cout << m << " " << r << std::endl;
  ASSERT_EQ(3, m.ref_map.size());
  ASSERT_EQ(3, m.ref_map[30].length);
  ASSERT_EQ(1, m.ref_map[30].refs);
  ASSERT_EQ(3, m.ref_map[35].length);
  ASSERT_EQ(1, m.ref_map[35].refs);
  ASSERT_EQ(2, m.ref_map[38].length);
  ASSERT_EQ(2, m.ref_map[38].refs);
  ASSERT_EQ(1, r.size());
  ASSERT_EQ(33, r[0].offset);
  ASSERT_EQ(2, r[0].length);
}
