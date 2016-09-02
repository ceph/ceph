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
    uint64_t bad_csum;
    ASSERT_EQ(0, b.verify_csum(0, bl, &bad_off, &bad_csum));
    ASSERT_EQ(-1, bad_off);

    b.init_csum(csum_type, 3, 24);
    cout << "  value size " << b.get_csum_value_size() << std::endl;
    b.calc_csum(0, bl);
    ASSERT_EQ(0, b.verify_csum(0, bl, &bad_off, &bad_csum));
    ASSERT_EQ(-1, bad_off);
    ASSERT_EQ(-1, b.verify_csum(0, bl2, &bad_off, &bad_csum));
    ASSERT_EQ(0, bad_off);

    ASSERT_EQ(0, b.verify_csum(0, f, &bad_off, &bad_csum));
    ASSERT_EQ(-1, bad_off);
    ASSERT_EQ(-1, b.verify_csum(8, f, &bad_off, &bad_csum));
    ASSERT_EQ(8, bad_off);
    ASSERT_EQ(-1, b.verify_csum(16, f, &bad_off, &bad_csum));
    ASSERT_EQ(16, bad_off);

    ASSERT_EQ(-1, b.verify_csum(0, m, &bad_off, &bad_csum));
    ASSERT_EQ(0, bad_off);
    ASSERT_EQ(0, b.verify_csum(8, m, &bad_off, &bad_csum));
    ASSERT_EQ(-1, bad_off);
    ASSERT_EQ(-1, b.verify_csum(16, m, &bad_off, &bad_csum));
    ASSERT_EQ(16, bad_off);

    ASSERT_EQ(-1, b.verify_csum(0, e, &bad_off, &bad_csum));
    ASSERT_EQ(0, bad_off);
    ASSERT_EQ(-1, b.verify_csum(8, e, &bad_off, &bad_csum));
    ASSERT_EQ(8, bad_off);
    ASSERT_EQ(0, b.verify_csum(16, e, &bad_off, &bad_csum));
    ASSERT_EQ(-1, bad_off);

    b.calc_csum(8, n);
    ASSERT_EQ(0, b.verify_csum(0, f, &bad_off, &bad_csum));
    ASSERT_EQ(-1, bad_off);
    ASSERT_EQ(0, b.verify_csum(8, n, &bad_off, &bad_csum));
    ASSERT_EQ(-1, bad_off);
    ASSERT_EQ(0, b.verify_csum(16, e, &bad_off, &bad_csum));
    ASSERT_EQ(-1, bad_off);
    ASSERT_EQ(-1, b.verify_csum(0, bl, &bad_off, &bad_csum));
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

