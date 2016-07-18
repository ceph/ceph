// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "os/bluestore/bluestore_types.h"
#include "gtest/gtest.h"
#include "include/stringify.h"
#include "common/ceph_time.h"

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

TEST(bluestore_blob_t, put_ref)
{
  unsigned mas = 4096;
  unsigned mrs = 8192;

  {
    bluestore_blob_t b;
    vector<bluestore_pextent_t> r;
    b.extents.push_back(bluestore_pextent_t(0, mas*2));
    b.ref_map.get(0, mas*2);
    ASSERT_TRUE(b.is_allocated(0, mas*2));
    b.put_ref(0, mas*2, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_FALSE(b.is_allocated(0, mas*2));
    ASSERT_FALSE(b.is_allocated(0, mas));
    ASSERT_FALSE(b.is_allocated(mas, 0));
    ASSERT_FALSE(b.extents[0].is_valid());
    ASSERT_EQ(mas*2, b.extents[0].length);
  }
  {
    bluestore_blob_t b;
    vector<bluestore_pextent_t> r;
    b.extents.push_back(bluestore_pextent_t(123, mas*2));
    b.ref_map.get(0, mas*2);
    b.put_ref(0, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*2));
    b.put_ref(mas, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(123u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_FALSE(b.is_allocated(0, mas*2));
    ASSERT_FALSE(b.extents[0].is_valid());
    ASSERT_EQ(mas*2, b.extents[0].length);
  }
  {
    bluestore_blob_t b;
    vector<bluestore_pextent_t> r;
    b.extents.push_back(bluestore_pextent_t(1, mas));
    b.extents.push_back(bluestore_pextent_t(2, mas));
    b.extents.push_back(bluestore_pextent_t(3, mas));
    b.extents.push_back(bluestore_pextent_t(4, mas));
    b.ref_map.get(0, mas*4);
    b.put_ref(mas, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*4));
    ASSERT_TRUE(b.is_allocated(mas, mas));
    b.put_ref(mas*2, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(mas*2, mas));
    ASSERT_TRUE(b.is_allocated(0, mas*4));
    b.put_ref(mas*3, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(2u, r.size());
    ASSERT_EQ(3u, r[0].offset);
    ASSERT_EQ(mas, r[0].length);
    ASSERT_EQ(4u, r[1].offset);
    ASSERT_EQ(mas, r[1].length);
    ASSERT_TRUE(b.is_allocated(0, mas*2));
    ASSERT_FALSE(b.is_allocated(mas*2, mas*2));
    ASSERT_TRUE(b.extents[0].is_valid());
    ASSERT_TRUE(b.extents[1].is_valid());
    ASSERT_FALSE(b.extents[2].is_valid());
    ASSERT_EQ(3u, b.extents.size());
  }
  {
    bluestore_blob_t b;
    vector<bluestore_pextent_t> r;
    b.extents.push_back(bluestore_pextent_t(1, mas));
    b.extents.push_back(bluestore_pextent_t(2, mas));
    b.extents.push_back(bluestore_pextent_t(3, mas));
    b.extents.push_back(bluestore_pextent_t(4, mas));
    b.extents.push_back(bluestore_pextent_t(5, mas));
    b.extents.push_back(bluestore_pextent_t(6, mas));
    b.ref_map.get(0, mas*6);
    b.put_ref(mas, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*6));
    b.put_ref(mas*2, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*6));
    b.put_ref(mas*3, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(2u, r.size());
    ASSERT_EQ(3u, r[0].offset);
    ASSERT_EQ(mas, r[0].length);
    ASSERT_EQ(4u, r[1].offset);
    ASSERT_EQ(mas, r[1].length);
    ASSERT_TRUE(b.is_allocated(0, mas*2));
    ASSERT_FALSE(b.is_allocated(mas*2, mas*2));
    ASSERT_TRUE(b.is_allocated(mas*4, mas*2));
    ASSERT_EQ(5u, b.extents.size());
    ASSERT_TRUE(b.extents[0].is_valid());
    ASSERT_TRUE(b.extents[1].is_valid());
    ASSERT_FALSE(b.extents[2].is_valid());
    ASSERT_TRUE(b.extents[3].is_valid());
    ASSERT_TRUE(b.extents[4].is_valid());
  }
  {
    bluestore_blob_t b;
    vector<bluestore_pextent_t> r;
    b.extents.push_back(bluestore_pextent_t(1, mas * 6));
    b.ref_map.get(0, mas*6);
    b.put_ref(mas, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*6));
    b.put_ref(mas*2, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*6));
    b.put_ref(mas*3, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x2001u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_TRUE(b.is_allocated(0, mas*2));
    ASSERT_FALSE(b.is_allocated(mas*2, mas*2));
    ASSERT_TRUE(b.is_allocated(mas*4, mas*2));
    ASSERT_EQ(3u, b.extents.size());
    ASSERT_TRUE(b.extents[0].is_valid());
    ASSERT_FALSE(b.extents[1].is_valid());
    ASSERT_TRUE(b.extents[2].is_valid());
  }
  {
    bluestore_blob_t b;
    vector<bluestore_pextent_t> r;
    b.extents.push_back(bluestore_pextent_t(1, mas * 4));
    b.extents.push_back(bluestore_pextent_t(2, mas * 4));
    b.extents.push_back(bluestore_pextent_t(3, mas * 4));
    b.ref_map.get(0, mas*12);
    b.put_ref(mas, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*12));
    b.put_ref(mas*9, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*12));
    b.put_ref(mas*2, mas*7, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(3u, r.size());
    ASSERT_EQ(0x2001u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_EQ(0x2u, r[1].offset);
    ASSERT_EQ(mas*4, r[1].length);
    ASSERT_EQ(0x3u, r[2].offset);
    ASSERT_EQ(mas*2, r[2].length);
    ASSERT_TRUE(b.is_allocated(0, mas*2));
    ASSERT_FALSE(b.is_allocated(mas*2, mas*8));
    ASSERT_TRUE(b.is_allocated(mas*10, mas*2));
    ASSERT_EQ(3u, b.extents.size());
    ASSERT_TRUE(b.extents[0].is_valid());
    ASSERT_FALSE(b.extents[1].is_valid());
    ASSERT_TRUE(b.extents[2].is_valid());
  }
  {
    bluestore_blob_t b;
    vector<bluestore_pextent_t> r;
    b.extents.push_back(bluestore_pextent_t(1, mas * 4));
    b.extents.push_back(bluestore_pextent_t(2, mas * 4));
    b.extents.push_back(bluestore_pextent_t(3, mas * 4));
    b.ref_map.get(0, mas*12);
    b.put_ref(mas, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*12));
    b.put_ref(mas*9, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*12));
    b.put_ref(mas*2, mas*7, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(3u, r.size());
    ASSERT_EQ(0x2001u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_EQ(0x2u, r[1].offset);
    ASSERT_EQ(mas*4, r[1].length);
    ASSERT_EQ(0x3u, r[2].offset);
    ASSERT_EQ(mas*2, r[2].length);
    ASSERT_TRUE(b.is_allocated(0, mas*2));
    ASSERT_FALSE(b.is_allocated(mas*2, mas*8));
    ASSERT_TRUE(b.is_allocated(mas*10, mas*2));
    ASSERT_EQ(3u, b.extents.size());
    ASSERT_TRUE(b.extents[0].is_valid());
    ASSERT_FALSE(b.extents[1].is_valid());
    ASSERT_TRUE(b.extents[2].is_valid());
    b.put_ref(0, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x1u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_EQ(2u, b.extents.size());
    ASSERT_FALSE(b.extents[0].is_valid());
    ASSERT_TRUE(b.extents[1].is_valid());
    b.put_ref(mas*10, mas*2, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x2003u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_EQ(1u, b.extents.size());
    ASSERT_FALSE(b.extents[0].is_valid());
  }
  {
    bluestore_blob_t b;
    vector<bluestore_pextent_t> r;
    b.extents.push_back(bluestore_pextent_t(1, mas * 4));
    b.extents.push_back(bluestore_pextent_t(2, mas * 4));
    b.extents.push_back(bluestore_pextent_t(3, mas * 4));
    b.ref_map.get(0, mas*12);
    b.put_ref(mas, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*12));
    b.put_ref(mas*9, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*12));
    b.put_ref(mas*2, mas*7, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(3u, r.size());
    ASSERT_EQ(0x2001u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_EQ(0x2u, r[1].offset);
    ASSERT_EQ(mas*4, r[1].length);
    ASSERT_EQ(0x3u, r[2].offset);
    ASSERT_EQ(mas*2, r[2].length);
    ASSERT_TRUE(b.is_allocated(0, mas*2));
    ASSERT_FALSE(b.is_allocated(mas*2, mas*8));
    ASSERT_TRUE(b.is_allocated(mas*10, mas*2));
    ASSERT_EQ(3u, b.extents.size());
    ASSERT_TRUE(b.extents[0].is_valid());
    ASSERT_FALSE(b.extents[1].is_valid());
    ASSERT_TRUE(b.extents[2].is_valid());
    b.put_ref(mas*10, mas*2, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x2003u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_EQ(2u, b.extents.size());
    ASSERT_TRUE(b.extents[0].is_valid());
    ASSERT_FALSE(b.extents[1].is_valid());
    b.put_ref(0, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x1u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_EQ(1u, b.extents.size());
    ASSERT_FALSE(b.extents[0].is_valid());
  }
  {
    bluestore_blob_t b;
    vector<bluestore_pextent_t> r;
    b.extents.push_back(bluestore_pextent_t(1, mas * 8));
    b.ref_map.get(0, mas*8);
    b.put_ref(0, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*8));
    b.put_ref(mas*7, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*8));
    b.put_ref(mas*2, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, 8));
    b.put_ref(mas*3, mas*4, mrs, &r);
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x2001u, r[0].offset);
    ASSERT_EQ(mas*6, r[0].length);
    ASSERT_TRUE(b.is_allocated(0, mas*2));
    ASSERT_FALSE(b.is_allocated(mas*2, mas*6));
    ASSERT_EQ(2u, b.extents.size());
    ASSERT_TRUE(b.extents[0].is_valid());
    ASSERT_FALSE(b.extents[1].is_valid());
    b.put_ref(mas, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x1u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_EQ(1u, b.extents.size());
    ASSERT_FALSE(b.extents[0].is_valid());
  }
  // verify csum chunk size if factored in properly
  {
    bluestore_blob_t b;
    vector<bluestore_pextent_t> r;
    b.extents.push_back(bluestore_pextent_t(0, mas*4));
    b.init_csum(bluestore_blob_t::CSUM_CRC32C, 14, mas * 4);
    b.ref_map.get(0, mas*4);
    ASSERT_TRUE(b.is_allocated(0, mas*4));
    b.put_ref(0, mas*3, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*4));
    ASSERT_TRUE(b.extents[0].is_valid());
    ASSERT_EQ(mas*4, b.extents[0].length);
  }
}

TEST(bluestore_blob_t, calc_csum)
{
  bufferlist bl;
  bl.append("asdfghjkqwertyuizxcvbnm,");
  bufferlist bl2;
  bl2.append("xxxxXXXXyyyyYYYYzzzzZZZZ");
  bufferlist f;
  f.substr_of(bl, 0, 8);
  bufferlist m;
  m.substr_of(bl, 8, 8);
  bufferlist e;
  e.substr_of(bl, 16, 8);
  bufferlist n;
  n.append("12345678");

  for (unsigned csum_type = 1;
       csum_type < bluestore_blob_t::CSUM_MAX;
       ++csum_type) {
    cout << "csum_type " << bluestore_blob_t::get_csum_type_string(csum_type)
	 << std::endl;

    bluestore_blob_t b;
    int bad_off;
    ASSERT_EQ(0, b.verify_csum(0, bl, &bad_off));
    ASSERT_EQ(-1, bad_off);

    b.init_csum(csum_type, 3, 24);
    cout << "  value size " << b.get_csum_value_size() << std::endl;
    b.calc_csum(0, bl);
    ASSERT_EQ(0, b.verify_csum(0, bl, &bad_off));
    ASSERT_EQ(-1, bad_off);
    ASSERT_EQ(-1, b.verify_csum(0, bl2, &bad_off));
    ASSERT_EQ(0, bad_off);

    ASSERT_EQ(0, b.verify_csum(0, f, &bad_off));
    ASSERT_EQ(-1, bad_off);
    ASSERT_EQ(-1, b.verify_csum(8, f, &bad_off));
    ASSERT_EQ(8, bad_off);
    ASSERT_EQ(-1, b.verify_csum(16, f, &bad_off));
    ASSERT_EQ(16, bad_off);

    ASSERT_EQ(-1, b.verify_csum(0, m, &bad_off));
    ASSERT_EQ(0, bad_off);
    ASSERT_EQ(0, b.verify_csum(8, m, &bad_off));
    ASSERT_EQ(-1, bad_off);
    ASSERT_EQ(-1, b.verify_csum(16, m, &bad_off));
    ASSERT_EQ(16, bad_off);

    ASSERT_EQ(-1, b.verify_csum(0, e, &bad_off));
    ASSERT_EQ(0, bad_off);
    ASSERT_EQ(-1, b.verify_csum(8, e, &bad_off));
    ASSERT_EQ(8, bad_off);
    ASSERT_EQ(0, b.verify_csum(16, e, &bad_off));
    ASSERT_EQ(-1, bad_off);

    b.calc_csum(8, n);
    ASSERT_EQ(0, b.verify_csum(0, f, &bad_off));
    ASSERT_EQ(-1, bad_off);
    ASSERT_EQ(0, b.verify_csum(8, n, &bad_off));
    ASSERT_EQ(-1, bad_off);
    ASSERT_EQ(0, b.verify_csum(16, e, &bad_off));
    ASSERT_EQ(-1, bad_off);
    ASSERT_EQ(-1, b.verify_csum(0, bl, &bad_off));
    ASSERT_EQ(8, bad_off);
  }
}

TEST(bluestore_blob_t, csum_bench)
{
  bufferlist bl;
  bufferptr bp(10485760);
  for (char *a = bp.c_str(); a < bp.c_str() + bp.length(); ++a)
    *a = (unsigned long)a & 0xff;
  bl.append(bp);
  int count = 256;
  for (unsigned csum_type = 1;
       csum_type < bluestore_blob_t::CSUM_MAX;
       ++csum_type) {
    bluestore_blob_t b;
    b.init_csum(csum_type, 12, bl.length());
    ceph::mono_clock::time_point start = ceph::mono_clock::now();
    for (int i = 0; i<count; ++i) {
      b.calc_csum(0, bl);
    }
    ceph::mono_clock::time_point end = ceph::mono_clock::now();
    auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    double mbsec = (double)count * (double)bl.length() / 1000000.0 / (double)dur.count() * 1000000000.0;
    cout << "csum_type " << bluestore_blob_t::get_csum_type_string(csum_type)
	 << ", " << dur << " seconds, "
	 << mbsec << " MB/sec" << std::endl;
  }
}

TEST(bluestore_onode_t, get_preferred_csum_order)
{
  bluestore_onode_t on;
  ASSERT_EQ(0u, on.get_preferred_csum_order());
  on.expected_write_size = 4096;
  ASSERT_EQ(12u, on.get_preferred_csum_order());
  on.expected_write_size = 4096;
  ASSERT_EQ(12u, on.get_preferred_csum_order());
  on.expected_write_size = 8192;
  ASSERT_EQ(13u, on.get_preferred_csum_order());
  on.expected_write_size = 8192 + 4096;
  ASSERT_EQ(12u, on.get_preferred_csum_order());
  on.expected_write_size = 1048576;
  ASSERT_EQ(20u, on.get_preferred_csum_order());
}

TEST(bluestore_onode_t, find_lextent)
{
  bluestore_onode_t on;
  ASSERT_EQ(on.extent_map.end(), on.find_lextent(0));
  ASSERT_EQ(on.extent_map.end(), on.find_lextent(100));

  on.extent_map[100] = bluestore_lextent_t(1, 0, 100);
  map<uint64_t,bluestore_lextent_t>::iterator a = on.extent_map.find(100);
  ASSERT_EQ(on.extent_map.end(), on.find_lextent(0));
  ASSERT_EQ(on.extent_map.end(), on.find_lextent(99));
  ASSERT_EQ(a, on.find_lextent(100));
  ASSERT_EQ(a, on.find_lextent(101));
  ASSERT_EQ(a, on.find_lextent(199));
  ASSERT_EQ(on.extent_map.end(), on.find_lextent(200));

  on.extent_map[200] = bluestore_lextent_t(2, 0, 100);
  map<uint64_t,bluestore_lextent_t>::iterator b = on.extent_map.find(200);
  ASSERT_EQ(on.extent_map.end(), on.find_lextent(0));
  ASSERT_EQ(on.extent_map.end(), on.find_lextent(99));
  ASSERT_EQ(a, on.find_lextent(100));
  ASSERT_EQ(a, on.find_lextent(101));
  ASSERT_EQ(a, on.find_lextent(199));
  ASSERT_EQ(b, on.find_lextent(200));
  ASSERT_EQ(b, on.find_lextent(299));
  ASSERT_EQ(on.extent_map.end(), on.find_lextent(300));

  on.extent_map[400] = bluestore_lextent_t(4, 0, 100);
  map<uint64_t,bluestore_lextent_t>::iterator d = on.extent_map.find(400);
  ASSERT_EQ(on.extent_map.end(), on.find_lextent(0));
  ASSERT_EQ(on.extent_map.end(), on.find_lextent(99));
  ASSERT_EQ(a, on.find_lextent(100));
  ASSERT_EQ(a, on.find_lextent(101));
  ASSERT_EQ(a, on.find_lextent(199));
  ASSERT_EQ(b, on.find_lextent(200));
  ASSERT_EQ(b, on.find_lextent(299));
  ASSERT_EQ(on.extent_map.end(), on.find_lextent(300));
  ASSERT_EQ(on.extent_map.end(), on.find_lextent(399));
  ASSERT_EQ(d, on.find_lextent(400));
  ASSERT_EQ(d, on.find_lextent(499));
  ASSERT_EQ(on.extent_map.end(), on.find_lextent(500));
}

TEST(bluestore_onode_t, seek_lextent)
{
  bluestore_onode_t on;
  ASSERT_EQ(on.extent_map.end(), on.seek_lextent(0));
  ASSERT_EQ(on.extent_map.end(), on.seek_lextent(100));

  on.extent_map[100] = bluestore_lextent_t(1, 0, 100);
  map<uint64_t,bluestore_lextent_t>::iterator a = on.extent_map.find(100);
  ASSERT_EQ(a, on.seek_lextent(0));
  ASSERT_EQ(a, on.seek_lextent(99));
  ASSERT_EQ(a, on.seek_lextent(100));
  ASSERT_EQ(a, on.seek_lextent(101));
  ASSERT_EQ(a, on.seek_lextent(199));
  ASSERT_EQ(on.extent_map.end(), on.seek_lextent(200));

  on.extent_map[200] = bluestore_lextent_t(2, 0, 100);
  map<uint64_t,bluestore_lextent_t>::iterator b = on.extent_map.find(200);
  ASSERT_EQ(a, on.seek_lextent(0));
  ASSERT_EQ(a, on.seek_lextent(99));
  ASSERT_EQ(a, on.seek_lextent(100));
  ASSERT_EQ(a, on.seek_lextent(101));
  ASSERT_EQ(a, on.seek_lextent(199));
  ASSERT_EQ(b, on.seek_lextent(200));
  ASSERT_EQ(b, on.seek_lextent(299));
  ASSERT_EQ(on.extent_map.end(), on.seek_lextent(300));

  on.extent_map[400] = bluestore_lextent_t(4, 0, 100);
  map<uint64_t,bluestore_lextent_t>::iterator d = on.extent_map.find(400);
  ASSERT_EQ(a, on.seek_lextent(0));
  ASSERT_EQ(a, on.seek_lextent(99));
  ASSERT_EQ(a, on.seek_lextent(100));
  ASSERT_EQ(a, on.seek_lextent(101));
  ASSERT_EQ(a, on.seek_lextent(199));
  ASSERT_EQ(b, on.seek_lextent(200));
  ASSERT_EQ(b, on.seek_lextent(299));
  ASSERT_EQ(d, on.seek_lextent(300));
  ASSERT_EQ(d, on.seek_lextent(399));
  ASSERT_EQ(d, on.seek_lextent(400));
  ASSERT_EQ(d, on.seek_lextent(499));
  ASSERT_EQ(on.extent_map.end(), on.seek_lextent(500));
}

TEST(bluestore_onode_t, has_any_lextents)
{
  bluestore_onode_t on;
  ASSERT_FALSE(on.has_any_lextents(0, 0));
  ASSERT_FALSE(on.has_any_lextents(0, 1000));
  ASSERT_FALSE(on.has_any_lextents(1000, 1000));

  on.extent_map[100] = bluestore_lextent_t(1, 0, 100);
  ASSERT_FALSE(on.has_any_lextents(0, 50));
  ASSERT_FALSE(on.has_any_lextents(0, 100));
  ASSERT_FALSE(on.has_any_lextents(50, 50));
  ASSERT_TRUE(on.has_any_lextents(50, 51));
  ASSERT_TRUE(on.has_any_lextents(50, 100051));
  ASSERT_TRUE(on.has_any_lextents(100, 100));
  ASSERT_TRUE(on.has_any_lextents(100, 1));
  ASSERT_TRUE(on.has_any_lextents(199, 1));
  ASSERT_TRUE(on.has_any_lextents(199, 2));
  ASSERT_FALSE(on.has_any_lextents(200, 2));

  on.extent_map[200] = bluestore_lextent_t(2, 0, 100);
  ASSERT_TRUE(on.has_any_lextents(199, 1));
  ASSERT_TRUE(on.has_any_lextents(199, 2));
  ASSERT_TRUE(on.has_any_lextents(200, 2));
  ASSERT_TRUE(on.has_any_lextents(200, 200));
  ASSERT_TRUE(on.has_any_lextents(299, 1));
  ASSERT_FALSE(on.has_any_lextents(300, 1));

  on.extent_map[400] = bluestore_lextent_t(4, 0, 100);
  ASSERT_TRUE(on.has_any_lextents(0, 10000));
  ASSERT_TRUE(on.has_any_lextents(199, 1));
  ASSERT_FALSE(on.has_any_lextents(300, 1));
  ASSERT_FALSE(on.has_any_lextents(300, 100));
  ASSERT_FALSE(on.has_any_lextents(399, 1));
  ASSERT_TRUE(on.has_any_lextents(400, 1));
  ASSERT_TRUE(on.has_any_lextents(400, 100));
  ASSERT_TRUE(on.has_any_lextents(400, 1000));
  ASSERT_TRUE(on.has_any_lextents(499, 1000));
  ASSERT_FALSE(on.has_any_lextents(500, 1000));
}

TEST(bluestore_onode_t, compress_extent_map)
{
  bluestore_onode_t on;
  vector<bluestore_lextent_t> r;
  on.extent_map[0] = bluestore_lextent_t(1, 0, 100);
  on.extent_map[100] = bluestore_lextent_t(2, 0, 100);
  ASSERT_EQ(0, on.compress_extent_map());
  ASSERT_EQ(2u, on.extent_map.size());

  on.extent_map[200] = bluestore_lextent_t(2, 100, 100);
  on.extent_map[300] = bluestore_lextent_t(2, 200, 100);
  ASSERT_EQ(2, on.compress_extent_map());
  ASSERT_EQ(2u, on.extent_map.size());

  on.extent_map[200] = bluestore_lextent_t(3, 100, 100);
  on.extent_map[300] = bluestore_lextent_t(2, 200, 100);
  ASSERT_EQ(0, on.compress_extent_map());
  ASSERT_EQ(4u, on.extent_map.size());

  on.extent_map[400] = bluestore_lextent_t(2, 300, 100);
  on.extent_map[500] = bluestore_lextent_t(2, 500, 100);
  on.extent_map[600] = bluestore_lextent_t(2, 600, 100);
  ASSERT_EQ(2, on.compress_extent_map());
  ASSERT_EQ(5u, on.extent_map.size());

  on.extent_map[400] = bluestore_lextent_t(2, 300, 100);
  on.extent_map[500] = bluestore_lextent_t(2, 400, 100);
  on.extent_map[700] = bluestore_lextent_t(2, 500, 100);
  ASSERT_EQ(1, on.compress_extent_map());
  ASSERT_EQ(6u, on.extent_map.size());
}

TEST(bluestore_onode_t, punch_hole)
{
  bluestore_onode_t on;
  vector<bluestore_lextent_t> r;
  on.extent_map[0] = bluestore_lextent_t(1, 0, 100);
  on.extent_map[100] = bluestore_lextent_t(2, 0, 100);

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
