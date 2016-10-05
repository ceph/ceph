// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "os/bluestore/bluestore_types.h"
#include "gtest/gtest.h"
#include "include/stringify.h"
#include "common/ceph_time.h"
#include "os/bluestore/BlueStore.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/global_context.h"

#include <sstream>

#define _STR(x) #x
#define STRINGIFY(x) _STR(x)

TEST(bluestore, sizeof) {
#define P(t) cout << STRINGIFY(t) << "\t" << sizeof(t) << std::endl
  P(BlueStore::Onode);
  P(BlueStore::Extent);
  P(BlueStore::Blob);
  P(BlueStore::SharedBlob);
  P(BlueStore::ExtentMap);
  P(BlueStore::extent_map_t);
  P(BlueStore::blob_map_t);
  P(BlueStore::BufferSpace);
  P(bluestore_extent_ref_map_t);
  P(bluestore_extent_ref_map_t::record_t);
  P(std::atomic_int);
  P(BlueStore::SharedBlobRef);
  P(boost::intrusive::set_base_hook<>);
  P(bufferlist);
  cout << "map<uint64_t,uint64_t>\t" << sizeof(map<uint64_t,uint64_t>) << std::endl;
  cout << "map<char,char>\t" << sizeof(map<char,char>) << std::endl;
}

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


TEST(Blob, put_ref)
{
  {
    BlueStore::Blob b;
    b.shared_blob = new BlueStore::SharedBlob(-1, string(), nullptr);
    b.shared_blob->get();  // hack to avoid dtor from running
    b.dirty_blob().extents.push_back(bluestore_pextent_t(0x40715000, 0x2000));
    b.dirty_blob().extents.push_back(
      bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, 0x8000));
    b.dirty_blob().extents.push_back(bluestore_pextent_t(0x4071f000, 0x5000));
    b.ref_map.get(0, 0x1200);
    b.ref_map.get(0xae00, 0x4200);
    cout << b << std::endl;
    vector<bluestore_pextent_t> r;

    b.put_ref(0, 0x1200, 0x1000, &r);
    cout << " r " << r << std::endl;
    cout << b << std::endl;

    r.clear();
    b.put_ref(0xae00, 0x4200, 0x1000, &r);
    cout << " r " << r << std::endl;
    cout << b << std::endl;  
  }

  unsigned mas = 4096;
  unsigned mrs = 8192;

  {
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(-1, string(), nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    vector<bluestore_pextent_t> r;
    b.extents.push_back(bluestore_pextent_t(0, mas*2));
    B.get_ref(0, mas*2);
    ASSERT_TRUE(b.is_allocated(0, mas*2));
    B.put_ref(0, mas*2, mrs, &r);
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
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(-1, string(), nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    vector<bluestore_pextent_t> r;
    b.extents.push_back(bluestore_pextent_t(123, mas*2));
    B.get_ref(0, mas*2);
    B.put_ref(0, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*2));
    B.put_ref(mas, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(123u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_FALSE(b.is_allocated(0, mas*2));
    ASSERT_FALSE(b.extents[0].is_valid());
    ASSERT_EQ(mas*2, b.extents[0].length);
  }
  {
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(-1, string(), nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    vector<bluestore_pextent_t> r;
    b.extents.push_back(bluestore_pextent_t(1, mas));
    b.extents.push_back(bluestore_pextent_t(2, mas));
    b.extents.push_back(bluestore_pextent_t(3, mas));
    b.extents.push_back(bluestore_pextent_t(4, mas));
    B.get_ref(0, mas*4);
    B.put_ref(mas, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*4));
    ASSERT_TRUE(b.is_allocated(mas, mas));
    B.put_ref(mas*2, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(mas*2, mas));
    ASSERT_TRUE(b.is_allocated(0, mas*4));
    B.put_ref(mas*3, mas, mrs, &r);
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
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(-1, string(), nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    vector<bluestore_pextent_t> r;
    b.extents.push_back(bluestore_pextent_t(1, mas));
    b.extents.push_back(bluestore_pextent_t(2, mas));
    b.extents.push_back(bluestore_pextent_t(3, mas));
    b.extents.push_back(bluestore_pextent_t(4, mas));
    b.extents.push_back(bluestore_pextent_t(5, mas));
    b.extents.push_back(bluestore_pextent_t(6, mas));
    B.get_ref(0, mas*6);
    B.put_ref(mas, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*6));
    B.put_ref(mas*2, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*6));
    B.put_ref(mas*3, mas, mrs, &r);
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
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(-1, string(), nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    vector<bluestore_pextent_t> r;
    b.extents.push_back(bluestore_pextent_t(1, mas * 6));
    B.get_ref(0, mas*6);
    B.put_ref(mas, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*6));
    B.put_ref(mas*2, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*6));
    B.put_ref(mas*3, mas, mrs, &r);
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
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(-1, string(), nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    vector<bluestore_pextent_t> r;
    b.extents.push_back(bluestore_pextent_t(1, mas * 4));
    b.extents.push_back(bluestore_pextent_t(2, mas * 4));
    b.extents.push_back(bluestore_pextent_t(3, mas * 4));
    B.get_ref(0, mas*12);
    B.put_ref(mas, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*12));
    B.put_ref(mas*9, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*12));
    B.put_ref(mas*2, mas*7, mrs, &r);
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
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(-1, string(), nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    vector<bluestore_pextent_t> r;
    b.extents.push_back(bluestore_pextent_t(1, mas * 4));
    b.extents.push_back(bluestore_pextent_t(2, mas * 4));
    b.extents.push_back(bluestore_pextent_t(3, mas * 4));
    B.get_ref(0, mas*12);
    B.put_ref(mas, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*12));
    B.put_ref(mas*9, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*12));
    B.put_ref(mas*2, mas*7, mrs, &r);
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
    B.put_ref(0, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x1u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_EQ(2u, b.extents.size());
    ASSERT_FALSE(b.extents[0].is_valid());
    ASSERT_TRUE(b.extents[1].is_valid());
    B.put_ref(mas*10, mas*2, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x2003u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_EQ(1u, b.extents.size());
    ASSERT_FALSE(b.extents[0].is_valid());
  }
  {
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(-1, string(), nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    vector<bluestore_pextent_t> r;
    b.extents.push_back(bluestore_pextent_t(1, mas * 4));
    b.extents.push_back(bluestore_pextent_t(2, mas * 4));
    b.extents.push_back(bluestore_pextent_t(3, mas * 4));
    B.get_ref(0, mas*12);
    B.put_ref(mas, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*12));
    B.put_ref(mas*9, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*12));
    B.put_ref(mas*2, mas*7, mrs, &r);
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
    B.put_ref(mas*10, mas*2, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x2003u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_EQ(2u, b.extents.size());
    ASSERT_TRUE(b.extents[0].is_valid());
    ASSERT_FALSE(b.extents[1].is_valid());
    B.put_ref(0, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x1u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_EQ(1u, b.extents.size());
    ASSERT_FALSE(b.extents[0].is_valid());
  }
  {
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(-1, string(), nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    vector<bluestore_pextent_t> r;
    b.extents.push_back(bluestore_pextent_t(1, mas * 8));
    B.get_ref(0, mas*8);
    B.put_ref(0, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*8));
    B.put_ref(mas*7, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*8));
    B.put_ref(mas*2, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, 8));
    B.put_ref(mas*3, mas*4, mrs, &r);
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x2001u, r[0].offset);
    ASSERT_EQ(mas*6, r[0].length);
    ASSERT_TRUE(b.is_allocated(0, mas*2));
    ASSERT_FALSE(b.is_allocated(mas*2, mas*6));
    ASSERT_EQ(2u, b.extents.size());
    ASSERT_TRUE(b.extents[0].is_valid());
    ASSERT_FALSE(b.extents[1].is_valid());
    B.put_ref(mas, mas, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x1u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_EQ(1u, b.extents.size());
    ASSERT_FALSE(b.extents[0].is_valid());
  }
  // verify csum chunk size if factored in properly
  {
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(-1, string(), nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    vector<bluestore_pextent_t> r;
    b.extents.push_back(bluestore_pextent_t(0, mas*4));
    b.init_csum(bluestore_blob_t::CSUM_CRC32C, 14, mas * 4);
    B.get_ref(0, mas*4);
    ASSERT_TRUE(b.is_allocated(0, mas*4));
    B.put_ref(0, mas*3, mrs, &r);
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*4));
    ASSERT_TRUE(b.extents[0].is_valid());
    ASSERT_EQ(mas*4, b.extents[0].length);
  }
}

TEST(bluestore_blob_t, can_split)
{
  bluestore_blob_t a;
  a.flags = bluestore_blob_t::FLAG_MUTABLE;
  ASSERT_TRUE(a.can_split());
  a.flags = bluestore_blob_t::FLAG_SHARED;
  ASSERT_FALSE(a.can_split());
  a.flags = bluestore_blob_t::FLAG_COMPRESSED;
  ASSERT_FALSE(a.can_split());
  a.flags = bluestore_blob_t::FLAG_HAS_UNUSED;
  ASSERT_FALSE(a.can_split());
}

TEST(bluestore_blob_t, can_split_at)
{
  bluestore_blob_t a;
  a.flags = bluestore_blob_t::FLAG_MUTABLE;
  a.extents.emplace_back(bluestore_pextent_t(0x10000, 0x2000));
  a.extents.emplace_back(bluestore_pextent_t(0x20000, 0x2000));
  ASSERT_TRUE(a.can_split_at(0x1000));
  ASSERT_TRUE(a.can_split_at(0x1800));
  a.init_csum(bluestore_blob_t::CSUM_CRC32C, 12, 0x4000);
  ASSERT_TRUE(a.can_split_at(0x1000));
  ASSERT_TRUE(a.can_split_at(0x2000));
  ASSERT_TRUE(a.can_split_at(0x3000));
  ASSERT_FALSE(a.can_split_at(0x2800));
}

TEST(bluestore_blob_t, prune_tail)
{
  bluestore_blob_t a;
  a.flags = bluestore_blob_t::FLAG_MUTABLE;
  a.extents.emplace_back(bluestore_pextent_t(0x10000, 0x2000));
  a.extents.emplace_back(bluestore_pextent_t(0x20000, 0x2000));
  ASSERT_FALSE(a.can_prune_tail());
  a.extents.emplace_back(
    bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, 0x2000));
  ASSERT_TRUE(a.can_prune_tail());
  a.prune_tail();
  ASSERT_FALSE(a.can_prune_tail());
  ASSERT_EQ(2u, a.extents.size());
  ASSERT_EQ(0x4000u, a.get_logical_length());

  a.extents.emplace_back(
    bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, 0x2000));
  a.init_csum(bluestore_blob_t::CSUM_CRC32C_8, 12, 0x6000);
  ASSERT_EQ(6u, a.csum_data.length());
  ASSERT_TRUE(a.can_prune_tail());
  a.prune_tail();
  ASSERT_FALSE(a.can_prune_tail());
  ASSERT_EQ(2u, a.extents.size());
  ASSERT_EQ(0x4000u, a.get_logical_length());
  ASSERT_EQ(4u, a.csum_data.length());

  bluestore_blob_t b;
  b.extents.emplace_back(
    bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, 0x2000));
  ASSERT_FALSE(a.can_prune_tail());
}

TEST(Blob, split)
{
  BlueStore::Cache *cache = BlueStore::Cache::create("lru", NULL);
  {
    BlueStore::Blob L, R;
    L.shared_blob = new BlueStore::SharedBlob(-1, string(), cache);
    L.shared_blob->get();  // hack to avoid dtor from running
    R.shared_blob = new BlueStore::SharedBlob(-1, string(), cache);
    R.shared_blob->get();  // hack to avoid dtor from running
    L.dirty_blob().extents.emplace_back(bluestore_pextent_t(0x2000, 0x2000));
    L.dirty_blob().init_csum(bluestore_blob_t::CSUM_CRC32C, 12, 0x2000);
    L.split(0x1000, &R);
    ASSERT_EQ(0x1000u, L.get_blob().get_logical_length());
    ASSERT_EQ(4u, L.get_blob().csum_data.length());
    ASSERT_EQ(1u, L.get_blob().extents.size());
    ASSERT_EQ(0x2000u, L.get_blob().extents.front().offset);
    ASSERT_EQ(0x1000u, L.get_blob().extents.front().length);
    ASSERT_EQ(0x1000u, R.get_blob().get_logical_length());
    ASSERT_EQ(4u, R.get_blob().csum_data.length());
    ASSERT_EQ(1u, R.get_blob().extents.size());
    ASSERT_EQ(0x3000u, R.get_blob().extents.front().offset);
    ASSERT_EQ(0x1000u, R.get_blob().extents.front().length);
  }
  {
    BlueStore::Blob L, R;
    L.shared_blob = new BlueStore::SharedBlob(-1, string(), cache);
    L.shared_blob->get();  // hack to avoid dtor from running
    R.shared_blob = new BlueStore::SharedBlob(-1, string(), cache);
    R.shared_blob->get();  // hack to avoid dtor from running
    L.dirty_blob().extents.emplace_back(bluestore_pextent_t(0x2000, 0x1000));
    L.dirty_blob().extents.emplace_back(bluestore_pextent_t(0x12000, 0x1000));
    L.dirty_blob().init_csum(bluestore_blob_t::CSUM_CRC32C, 12, 0x2000);
    L.split(0x1000, &R);
    ASSERT_EQ(0x1000u, L.get_blob().get_logical_length());
    ASSERT_EQ(4u, L.get_blob().csum_data.length());
    ASSERT_EQ(1u, L.get_blob().extents.size());
    ASSERT_EQ(0x2000u, L.get_blob().extents.front().offset);
    ASSERT_EQ(0x1000u, L.get_blob().extents.front().length);
    ASSERT_EQ(0x1000u, R.get_blob().get_logical_length());
    ASSERT_EQ(4u, R.get_blob().csum_data.length());
    ASSERT_EQ(1u, R.get_blob().extents.size());
    ASSERT_EQ(0x12000u, R.get_blob().extents.front().offset);
    ASSERT_EQ(0x1000u, R.get_blob().extents.front().length);
  }
}

TEST(ExtentMap, find_lextent)
{
  BlueStore::LRUCache cache;
  BlueStore::ExtentMap em(nullptr);
  BlueStore::BlobRef br(new BlueStore::Blob);
  br->shared_blob = new BlueStore::SharedBlob(-1, string(), &cache);

  ASSERT_EQ(em.extent_map.end(), em.find_lextent(0));
  ASSERT_EQ(em.extent_map.end(), em.find_lextent(100));

  em.extent_map.insert(*new BlueStore::Extent(100, 0, 100, 1, br));
  auto a = em.find(100);
  ASSERT_EQ(em.extent_map.end(), em.find_lextent(0));
  ASSERT_EQ(em.extent_map.end(), em.find_lextent(99));
  ASSERT_EQ(a, em.find_lextent(100));
  ASSERT_EQ(a, em.find_lextent(101));
  ASSERT_EQ(a, em.find_lextent(199));
  ASSERT_EQ(em.extent_map.end(), em.find_lextent(200));

  em.extent_map.insert(*new BlueStore::Extent(200, 0, 100, 1, br));
  auto b = em.find(200);
  ASSERT_EQ(em.extent_map.end(), em.find_lextent(0));
  ASSERT_EQ(em.extent_map.end(), em.find_lextent(99));
  ASSERT_EQ(a, em.find_lextent(100));
  ASSERT_EQ(a, em.find_lextent(101));
  ASSERT_EQ(a, em.find_lextent(199));
  ASSERT_EQ(b, em.find_lextent(200));
  ASSERT_EQ(b, em.find_lextent(299));
  ASSERT_EQ(em.extent_map.end(), em.find_lextent(300));

  em.extent_map.insert(*new BlueStore::Extent(400, 0, 100, 1, br));
  auto d = em.find(400);
  ASSERT_EQ(em.extent_map.end(), em.find_lextent(0));
  ASSERT_EQ(em.extent_map.end(), em.find_lextent(99));
  ASSERT_EQ(a, em.find_lextent(100));
  ASSERT_EQ(a, em.find_lextent(101));
  ASSERT_EQ(a, em.find_lextent(199));
  ASSERT_EQ(b, em.find_lextent(200));
  ASSERT_EQ(b, em.find_lextent(299));
  ASSERT_EQ(em.extent_map.end(), em.find_lextent(300));
  ASSERT_EQ(em.extent_map.end(), em.find_lextent(399));
  ASSERT_EQ(d, em.find_lextent(400));
  ASSERT_EQ(d, em.find_lextent(499));
  ASSERT_EQ(em.extent_map.end(), em.find_lextent(500));
}

TEST(ExtentMap, seek_lextent)
{
  BlueStore::LRUCache cache;
  BlueStore::ExtentMap em(nullptr);
  BlueStore::BlobRef br(new BlueStore::Blob);
  br->shared_blob = new BlueStore::SharedBlob(-1, string(), &cache);

  ASSERT_EQ(em.extent_map.end(), em.seek_lextent(0));
  ASSERT_EQ(em.extent_map.end(), em.seek_lextent(100));

  em.extent_map.insert(*new BlueStore::Extent(100, 0, 100, 1, br));
  auto a = em.find(100);
  ASSERT_EQ(a, em.seek_lextent(0));
  ASSERT_EQ(a, em.seek_lextent(99));
  ASSERT_EQ(a, em.seek_lextent(100));
  ASSERT_EQ(a, em.seek_lextent(101));
  ASSERT_EQ(a, em.seek_lextent(199));
  ASSERT_EQ(em.extent_map.end(), em.seek_lextent(200));

  em.extent_map.insert(*new BlueStore::Extent(200, 0, 100, 1, br));
  auto b = em.find(200);
  ASSERT_EQ(a, em.seek_lextent(0));
  ASSERT_EQ(a, em.seek_lextent(99));
  ASSERT_EQ(a, em.seek_lextent(100));
  ASSERT_EQ(a, em.seek_lextent(101));
  ASSERT_EQ(a, em.seek_lextent(199));
  ASSERT_EQ(b, em.seek_lextent(200));
  ASSERT_EQ(b, em.seek_lextent(299));
  ASSERT_EQ(em.extent_map.end(), em.seek_lextent(300));

  em.extent_map.insert(*new BlueStore::Extent(400, 0, 100, 1, br));
  auto d = em.find(400);
  ASSERT_EQ(a, em.seek_lextent(0));
  ASSERT_EQ(a, em.seek_lextent(99));
  ASSERT_EQ(a, em.seek_lextent(100));
  ASSERT_EQ(a, em.seek_lextent(101));
  ASSERT_EQ(a, em.seek_lextent(199));
  ASSERT_EQ(b, em.seek_lextent(200));
  ASSERT_EQ(b, em.seek_lextent(299));
  ASSERT_EQ(d, em.seek_lextent(300));
  ASSERT_EQ(d, em.seek_lextent(399));
  ASSERT_EQ(d, em.seek_lextent(400));
  ASSERT_EQ(d, em.seek_lextent(499));
  ASSERT_EQ(em.extent_map.end(), em.seek_lextent(500));
}

TEST(ExtentMap, has_any_lextents)
{
  BlueStore::LRUCache cache;
  BlueStore::ExtentMap em(nullptr);
  BlueStore::BlobRef b(new BlueStore::Blob);
  b->shared_blob = new BlueStore::SharedBlob(-1, string(), &cache);

  ASSERT_FALSE(em.has_any_lextents(0, 0));
  ASSERT_FALSE(em.has_any_lextents(0, 1000));
  ASSERT_FALSE(em.has_any_lextents(1000, 1000));

  em.extent_map.insert(*new BlueStore::Extent(100, 0, 100, 1, b));
  ASSERT_FALSE(em.has_any_lextents(0, 50));
  ASSERT_FALSE(em.has_any_lextents(0, 100));
  ASSERT_FALSE(em.has_any_lextents(50, 50));
  ASSERT_TRUE(em.has_any_lextents(50, 51));
  ASSERT_TRUE(em.has_any_lextents(50, 100051));
  ASSERT_TRUE(em.has_any_lextents(100, 100));
  ASSERT_TRUE(em.has_any_lextents(100, 1));
  ASSERT_TRUE(em.has_any_lextents(199, 1));
  ASSERT_TRUE(em.has_any_lextents(199, 2));
  ASSERT_FALSE(em.has_any_lextents(200, 2));

  em.extent_map.insert(*new BlueStore::Extent(200, 0, 100, 1, b));
  ASSERT_TRUE(em.has_any_lextents(199, 1));
  ASSERT_TRUE(em.has_any_lextents(199, 2));
  ASSERT_TRUE(em.has_any_lextents(200, 2));
  ASSERT_TRUE(em.has_any_lextents(200, 200));
  ASSERT_TRUE(em.has_any_lextents(299, 1));
  ASSERT_FALSE(em.has_any_lextents(300, 1));

  em.extent_map.insert(*new BlueStore::Extent(400, 0, 100, 1, b));
  ASSERT_TRUE(em.has_any_lextents(0, 10000));
  ASSERT_TRUE(em.has_any_lextents(199, 1));
  ASSERT_FALSE(em.has_any_lextents(300, 1));
  ASSERT_FALSE(em.has_any_lextents(300, 100));
  ASSERT_FALSE(em.has_any_lextents(399, 1));
  ASSERT_TRUE(em.has_any_lextents(400, 1));
  ASSERT_TRUE(em.has_any_lextents(400, 100));
  ASSERT_TRUE(em.has_any_lextents(400, 1000));
  ASSERT_TRUE(em.has_any_lextents(499, 1000));
  ASSERT_FALSE(em.has_any_lextents(500, 1000));
}

TEST(ExtentMap, compress_extent_map)
{
  BlueStore::LRUCache cache;
  BlueStore::ExtentMap em(nullptr);
  BlueStore::BlobRef b1(new BlueStore::Blob);
  BlueStore::BlobRef b2(new BlueStore::Blob);
  BlueStore::BlobRef b3(new BlueStore::Blob);
  b1->shared_blob = new BlueStore::SharedBlob(-1, string(), &cache);
  b2->shared_blob = new BlueStore::SharedBlob(-1, string(), &cache);
  b3->shared_blob = new BlueStore::SharedBlob(-1, string(), &cache);

  em.extent_map.insert(*new BlueStore::Extent(0, 0, 100, 1, b1));
  em.extent_map.insert(*new BlueStore::Extent(100, 0, 100, 1, b2));
  ASSERT_EQ(0, em.compress_extent_map(0, 10000));
  ASSERT_EQ(2u, em.extent_map.size());

  em.extent_map.insert(*new BlueStore::Extent(200, 100, 100, 1, b2));
  em.extent_map.insert(*new BlueStore::Extent(300, 200, 100, 1, b2));
  ASSERT_EQ(0, em.compress_extent_map(0, 0));
  ASSERT_EQ(0, em.compress_extent_map(100000, 1000));
  ASSERT_EQ(2, em.compress_extent_map(0, 100000));
  ASSERT_EQ(2u, em.extent_map.size());

  em.extent_map.erase(em.find(100));
  em.extent_map.insert(*new BlueStore::Extent(100, 0, 100, 1, b2));
  em.extent_map.insert(*new BlueStore::Extent(200, 100, 100, 1, b3));
  em.extent_map.insert(*new BlueStore::Extent(300, 200, 100, 1, b2));
  ASSERT_EQ(0, em.compress_extent_map(0, 1));
  ASSERT_EQ(0, em.compress_extent_map(0, 100000));
  ASSERT_EQ(4u, em.extent_map.size());

  em.extent_map.insert(*new BlueStore::Extent(400, 300, 100, 1, b2));
  em.extent_map.insert(*new BlueStore::Extent(500, 500, 100, 1, b2));
  em.extent_map.insert(*new BlueStore::Extent(600, 600, 100, 1, b2));
  em.extent_map.insert(*new BlueStore::Extent(700, 0, 100, 1, b1));
  em.extent_map.insert(*new BlueStore::Extent(800, 0, 100, 1, b3));
  ASSERT_EQ(0, em.compress_extent_map(0, 99));
  ASSERT_EQ(0, em.compress_extent_map(800, 1000));
  ASSERT_EQ(2, em.compress_extent_map(100, 500));
  ASSERT_EQ(7u, em.extent_map.size());
  em.extent_map.erase(em.find(300));
  em.extent_map.erase(em.find(500));  
  em.extent_map.erase(em.find(700));
  em.extent_map.insert(*new BlueStore::Extent(400, 300, 100, 1, b2));
  em.extent_map.insert(*new BlueStore::Extent(500, 400, 100, 1, b2));
  em.extent_map.insert(*new BlueStore::Extent(700, 500, 100, 1, b2));
  ASSERT_EQ(1, em.compress_extent_map(0, 1000));
  ASSERT_EQ(6u, em.extent_map.size());
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);
  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  int r = RUN_ALL_TESTS();
  g_ceph_context->put();
  return r;
}
