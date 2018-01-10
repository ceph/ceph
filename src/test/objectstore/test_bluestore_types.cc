// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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
  P(BlueStore::Buffer);
  P(bluestore_onode_t);
  P(bluestore_blob_t);
  P(PExtentVector);
  P(bluestore_shared_blob_t);
  P(bluestore_extent_ref_map_t);
  P(bluestore_extent_ref_map_t::record_t);
  P(bluestore_blob_use_tracker_t);
  P(std::atomic_int);
  P(BlueStore::SharedBlobRef);
  P(boost::intrusive::set_base_hook<>);
  P(boost::intrusive::unordered_set_base_hook<>);
  P(bufferlist);
  P(bufferptr);
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
  PExtentVector r;
  bool maybe_unshared = false;
  m.get(10, 30);
  maybe_unshared = true;
  m.put(10, 30, &r, &maybe_unshared);
  cout << m << " " << r << " " << (int)maybe_unshared << std::endl;
  ASSERT_EQ(0u, m.ref_map.size());
  ASSERT_EQ(1u, r.size());
  ASSERT_EQ(10u, r[0].offset);
  ASSERT_EQ(30u, r[0].length);
  ASSERT_TRUE(maybe_unshared);
  r.clear();
  m.get(10, 30);
  m.get(20, 10);
  maybe_unshared = true;
  m.put(10, 30, &r, &maybe_unshared);
  cout << m << " " << r << " " << (int)maybe_unshared << std::endl;
  ASSERT_EQ(1u, m.ref_map.size());
  ASSERT_EQ(10u, m.ref_map[20].length);
  ASSERT_EQ(1u, m.ref_map[20].refs);
  ASSERT_EQ(2u, r.size());
  ASSERT_EQ(10u, r[0].offset);
  ASSERT_EQ(10u, r[0].length);
  ASSERT_EQ(30u, r[1].offset);
  ASSERT_EQ(10u, r[1].length);
  ASSERT_TRUE(maybe_unshared);
  r.clear();
  m.get(30, 10);
  m.get(30, 10);
  maybe_unshared = true;
  m.put(20, 15, &r, &maybe_unshared);
  cout << m << " " << r << " " << (int)maybe_unshared << std::endl;
  ASSERT_EQ(2u, m.ref_map.size());
  ASSERT_EQ(5u, m.ref_map[30].length);
  ASSERT_EQ(1u, m.ref_map[30].refs);
  ASSERT_EQ(5u, m.ref_map[35].length);
  ASSERT_EQ(2u, m.ref_map[35].refs);
  ASSERT_EQ(1u, r.size());
  ASSERT_EQ(20u, r[0].offset);
  ASSERT_EQ(10u, r[0].length);
  ASSERT_FALSE(maybe_unshared);
  r.clear();
  maybe_unshared = true;
  m.put(33, 5, &r, &maybe_unshared);
  cout << m << " " << r << " " << (int)maybe_unshared << std::endl;
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
  ASSERT_FALSE(maybe_unshared);
  r.clear();
  maybe_unshared = true;
  m.put(38, 2, &r, &maybe_unshared);
  cout << m << " " << r << " " << (int)maybe_unshared << std::endl;
  ASSERT_TRUE(maybe_unshared);
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

  for (unsigned csum_type = Checksummer::CSUM_NONE + 1;
       csum_type < Checksummer::CSUM_MAX;
       ++csum_type) {
    cout << "csum_type " << Checksummer::get_csum_type_string(csum_type)
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
       csum_type < Checksummer::CSUM_MAX;
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
    cout << "csum_type " << Checksummer::get_csum_type_string(csum_type)
	 << ", " << dur << " seconds, "
	 << mbsec << " MB/sec" << std::endl;
  }
}

TEST(Blob, put_ref)
{
  {
    BlueStore store(g_ceph_context, "", 4096);
    BlueStore::Cache *cache = BlueStore::Cache::create(
      g_ceph_context, "lru", NULL);
    BlueStore::Collection coll(&store, cache, coll_t());
    BlueStore::Blob b;
    b.shared_blob = new BlueStore::SharedBlob(nullptr);
    b.shared_blob->get();  // hack to avoid dtor from running
    b.dirty_blob().allocated_test(bluestore_pextent_t(0x40715000, 0x2000));
    b.dirty_blob().allocated_test(
      bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, 0x8000));
    b.dirty_blob().allocated_test(bluestore_pextent_t(0x4071f000, 0x5000));
    b.get_ref(&coll, 0, 0x1200);
    b.get_ref(&coll, 0xae00, 0x4200);
    ASSERT_EQ(0x5400u, b.get_referenced_bytes());
    cout << b << std::endl;
    PExtentVector r;

    ASSERT_FALSE(b.put_ref(&coll, 0, 0x1200, &r));
    ASSERT_EQ(0x4200u, b.get_referenced_bytes());
    cout << " r " << r << std::endl;
    cout << b << std::endl;

    r.clear();
    ASSERT_TRUE(b.put_ref(&coll, 0xae00, 0x4200, &r));
    ASSERT_EQ(0u, b.get_referenced_bytes());
    cout << " r " << r << std::endl;
    cout << b << std::endl;
  }

  unsigned mas = 4096;
  BlueStore store(g_ceph_context, "", 8192);
  BlueStore::Cache *cache = BlueStore::Cache::create(
    g_ceph_context, "lru", NULL);
  BlueStore::CollectionRef coll(new BlueStore::Collection(&store, cache, coll_t()));

  {
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    PExtentVector r;
    b.allocated_test(bluestore_pextent_t(0, mas * 2));
    B.get_ref(coll.get(), 0, mas*2);
    ASSERT_EQ(mas * 2, B.get_referenced_bytes());
    ASSERT_TRUE(b.is_allocated(0, mas*2));
    ASSERT_TRUE(B.put_ref(coll.get(), 0, mas*2, &r));
    ASSERT_EQ(0u, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_FALSE(b.is_allocated(0, mas*2));
    ASSERT_FALSE(b.is_allocated(0, mas));
    ASSERT_FALSE(b.is_allocated(mas, 0));
    ASSERT_FALSE(b.get_extents()[0].is_valid());
    ASSERT_EQ(mas*2, b.get_extents()[0].length);
  }
  {
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    PExtentVector r;
    b.allocated_test(bluestore_pextent_t(123, mas * 2));
    B.get_ref(coll.get(), 0, mas*2);
    ASSERT_EQ(mas * 2, B.get_referenced_bytes());
    ASSERT_FALSE(B.put_ref(coll.get(), 0, mas, &r));
    ASSERT_EQ(mas, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*2));
    ASSERT_TRUE(B.put_ref(coll.get(), mas, mas, &r));
    ASSERT_EQ(0u, B.get_referenced_bytes());
    ASSERT_EQ(0u, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(123u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_FALSE(b.is_allocated(0, mas*2));
    ASSERT_FALSE(b.get_extents()[0].is_valid());
    ASSERT_EQ(mas*2, b.get_extents()[0].length);
  }
  {
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    PExtentVector r;
    b.allocated_test(bluestore_pextent_t(1, mas));
    b.allocated_test(bluestore_pextent_t(2, mas));
    b.allocated_test(bluestore_pextent_t(3, mas));
    b.allocated_test(bluestore_pextent_t(4, mas));
    B.get_ref(coll.get(), 0, mas*4);
    ASSERT_EQ(mas * 4, B.get_referenced_bytes());
    ASSERT_FALSE(B.put_ref(coll.get(), mas, mas, &r));
    ASSERT_EQ(mas * 3, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*4));
    ASSERT_TRUE(b.is_allocated(mas, mas));
    ASSERT_FALSE(B.put_ref(coll.get(), mas*2, mas, &r));
    ASSERT_EQ(mas * 2, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(mas*2, mas));
    ASSERT_TRUE(b.is_allocated(0, mas*4));
    ASSERT_FALSE(B.put_ref(coll.get(), mas*3, mas, &r));
    ASSERT_EQ(mas, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(2u, r.size());
    ASSERT_EQ(3u, r[0].offset);
    ASSERT_EQ(mas, r[0].length);
    ASSERT_EQ(4u, r[1].offset);
    ASSERT_EQ(mas, r[1].length);
    ASSERT_TRUE(b.is_allocated(0, mas*2));
    ASSERT_FALSE(b.is_allocated(mas*2, mas*2));
    ASSERT_TRUE(b.get_extents()[0].is_valid());
    ASSERT_TRUE(b.get_extents()[1].is_valid());
    ASSERT_FALSE(b.get_extents()[2].is_valid());
    ASSERT_EQ(3u, b.get_extents().size());
  }
  {
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    PExtentVector r;
    b.allocated_test(bluestore_pextent_t(1, mas));
    b.allocated_test(bluestore_pextent_t(2, mas));
    b.allocated_test(bluestore_pextent_t(3, mas));
    b.allocated_test(bluestore_pextent_t(4, mas));
    b.allocated_test(bluestore_pextent_t(5, mas));
    b.allocated_test(bluestore_pextent_t(6, mas));
    B.get_ref(coll.get(), 0, mas*6);
    ASSERT_EQ(mas * 6, B.get_referenced_bytes());
    ASSERT_FALSE(B.put_ref(coll.get(), mas, mas, &r));
    ASSERT_EQ(mas * 5, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*6));
    ASSERT_FALSE(B.put_ref(coll.get(), mas*2, mas, &r));
    ASSERT_EQ(mas * 4, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*6));
    ASSERT_FALSE(B.put_ref(coll.get(), mas*3, mas, &r));
    ASSERT_EQ(mas * 3, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(2u, r.size());
    ASSERT_EQ(3u, r[0].offset);
    ASSERT_EQ(mas, r[0].length);
    ASSERT_EQ(4u, r[1].offset);
    ASSERT_EQ(mas, r[1].length);
    ASSERT_TRUE(b.is_allocated(0, mas*2));
    ASSERT_FALSE(b.is_allocated(mas*2, mas*2));
    ASSERT_TRUE(b.is_allocated(mas*4, mas*2));
    ASSERT_EQ(5u, b.get_extents().size());
    ASSERT_TRUE(b.get_extents()[0].is_valid());
    ASSERT_TRUE(b.get_extents()[1].is_valid());
    ASSERT_FALSE(b.get_extents()[2].is_valid());
    ASSERT_TRUE(b.get_extents()[3].is_valid());
    ASSERT_TRUE(b.get_extents()[4].is_valid());
  }
  {
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    PExtentVector r;
    b.allocated_test(bluestore_pextent_t(1, mas * 6));
    B.get_ref(coll.get(), 0, mas*6);
    ASSERT_EQ(mas * 6, B.get_referenced_bytes());
    ASSERT_FALSE(B.put_ref(coll.get(), mas, mas, &r));
    ASSERT_EQ(mas * 5, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*6));
    ASSERT_FALSE(B.put_ref(coll.get(), mas*2, mas, &r));
    ASSERT_EQ(mas * 4, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*6));
    ASSERT_FALSE(B.put_ref(coll.get(), mas*3, mas, &r));
    ASSERT_EQ(mas * 3, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x2001u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_TRUE(b.is_allocated(0, mas*2));
    ASSERT_FALSE(b.is_allocated(mas*2, mas*2));
    ASSERT_TRUE(b.is_allocated(mas*4, mas*2));
    ASSERT_EQ(3u, b.get_extents().size());
    ASSERT_TRUE(b.get_extents()[0].is_valid());
    ASSERT_FALSE(b.get_extents()[1].is_valid());
    ASSERT_TRUE(b.get_extents()[2].is_valid());
  }
  {
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    PExtentVector r;
    b.allocated_test(bluestore_pextent_t(1, mas * 4));
    b.allocated_test(bluestore_pextent_t(2, mas * 4));
    b.allocated_test(bluestore_pextent_t(3, mas * 4));
    B.get_ref(coll.get(), 0, mas*12);
    ASSERT_EQ(mas * 12, B.get_referenced_bytes());
    ASSERT_FALSE(B.put_ref(coll.get(), mas, mas, &r));
    ASSERT_EQ(mas * 11, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*12));
    ASSERT_FALSE(B.put_ref(coll.get(), mas*9, mas, &r));
    ASSERT_EQ(mas * 10, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*12));
    ASSERT_FALSE(B.put_ref(coll.get(), mas*2, mas*7, &r));
    ASSERT_EQ(mas * 3, B.get_referenced_bytes());
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
    ASSERT_EQ(3u, b.get_extents().size());
    ASSERT_TRUE(b.get_extents()[0].is_valid());
    ASSERT_FALSE(b.get_extents()[1].is_valid());
    ASSERT_TRUE(b.get_extents()[2].is_valid());
  }
  {
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    PExtentVector r;
    b.allocated_test(bluestore_pextent_t(1, mas * 4));
    b.allocated_test(bluestore_pextent_t(2, mas * 4));
    b.allocated_test(bluestore_pextent_t(3, mas * 4));
    B.get_ref(coll.get(), 0, mas*12);
    ASSERT_EQ(mas * 12, B.get_referenced_bytes());
    ASSERT_FALSE(B.put_ref(coll.get(), mas, mas, &r));
    ASSERT_EQ(mas * 11, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*12));
    ASSERT_FALSE(B.put_ref(coll.get(), mas*9, mas, &r));
    ASSERT_EQ(mas * 10, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*12));
    ASSERT_FALSE(B.put_ref(coll.get(), mas*2, mas*7, &r));
    ASSERT_EQ(mas * 3, B.get_referenced_bytes());
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
    ASSERT_EQ(3u, b.get_extents().size());
    ASSERT_TRUE(b.get_extents()[0].is_valid());
    ASSERT_FALSE(b.get_extents()[1].is_valid());
    ASSERT_TRUE(b.get_extents()[2].is_valid());
    ASSERT_FALSE(B.put_ref(coll.get(), 0, mas, &r));
    ASSERT_EQ(mas * 2, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x1u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_EQ(2u, b.get_extents().size());
    ASSERT_FALSE(b.get_extents()[0].is_valid());
    ASSERT_TRUE(b.get_extents()[1].is_valid());
    ASSERT_TRUE(B.put_ref(coll.get(), mas*10, mas*2, &r));
    ASSERT_EQ(mas * 0, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x2003u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_EQ(1u, b.get_extents().size());
    ASSERT_FALSE(b.get_extents()[0].is_valid());
  }
  {
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    PExtentVector r;
    b.allocated_test(bluestore_pextent_t(1, mas * 4));
    b.allocated_test(bluestore_pextent_t(2, mas * 4));
    b.allocated_test(bluestore_pextent_t(3, mas * 4));
    B.get_ref(coll.get(), 0, mas*12);
    ASSERT_EQ(mas * 12, B.get_referenced_bytes());
    ASSERT_FALSE(B.put_ref(coll.get(), mas, mas, &r));
    ASSERT_EQ(mas * 11, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*12));
    ASSERT_FALSE(B.put_ref(coll.get(), mas*9, mas, &r));
    ASSERT_EQ(mas * 10, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*12));
    ASSERT_FALSE(B.put_ref(coll.get(), mas*2, mas*7, &r));
    ASSERT_EQ(mas * 3, B.get_referenced_bytes());
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
    ASSERT_EQ(3u, b.get_extents().size());
    ASSERT_TRUE(b.get_extents()[0].is_valid());
    ASSERT_FALSE(b.get_extents()[1].is_valid());
    ASSERT_TRUE(b.get_extents()[2].is_valid());
    ASSERT_FALSE(B.put_ref(coll.get(), mas*10, mas*2, &r));
    ASSERT_EQ(mas * 1, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x2003u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_EQ(2u, b.get_extents().size());
    ASSERT_TRUE(b.get_extents()[0].is_valid());
    ASSERT_FALSE(b.get_extents()[1].is_valid());
    ASSERT_TRUE(B.put_ref(coll.get(), 0, mas, &r));
    ASSERT_EQ(mas * 0, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x1u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_EQ(1u, b.get_extents().size());
    ASSERT_FALSE(b.get_extents()[0].is_valid());
  }
  {
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    PExtentVector r;
    b.allocated_test(bluestore_pextent_t(1, mas * 8));
    B.get_ref(coll.get(), 0, mas*8);
    ASSERT_EQ(mas * 8, B.get_referenced_bytes());
    ASSERT_FALSE(B.put_ref(coll.get(), 0, mas, &r));
    ASSERT_EQ(mas * 7, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*8));
    ASSERT_FALSE(B.put_ref(coll.get(), mas*7, mas, &r));
    ASSERT_EQ(mas * 6, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*8));
    ASSERT_FALSE(B.put_ref(coll.get(), mas*2, mas, &r));
    ASSERT_EQ(mas * 5, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, 8));
    ASSERT_FALSE(B.put_ref(coll.get(), mas*3, mas*4, &r));
    ASSERT_EQ(mas * 1, B.get_referenced_bytes());
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x2001u, r[0].offset);
    ASSERT_EQ(mas*6, r[0].length);
    ASSERT_TRUE(b.is_allocated(0, mas*2));
    ASSERT_FALSE(b.is_allocated(mas*2, mas*6));
    ASSERT_EQ(2u, b.get_extents().size());
    ASSERT_TRUE(b.get_extents()[0].is_valid());
    ASSERT_FALSE(b.get_extents()[1].is_valid());
    ASSERT_TRUE(B.put_ref(coll.get(), mas, mas, &r));
    ASSERT_EQ(mas * 0, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x1u, r[0].offset);
    ASSERT_EQ(mas*2, r[0].length);
    ASSERT_EQ(1u, b.get_extents().size());
    ASSERT_FALSE(b.get_extents()[0].is_valid());
  }
  // verify csum chunk size if factored in properly
  {
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    PExtentVector r;
    b.allocated_test(bluestore_pextent_t(0, mas*4));
    b.init_csum(Checksummer::CSUM_CRC32C, 14, mas * 4);
    B.get_ref(coll.get(), 0, mas*4);
    ASSERT_EQ(mas * 4, B.get_referenced_bytes());
    ASSERT_TRUE(b.is_allocated(0, mas*4));
    ASSERT_FALSE(B.put_ref(coll.get(), 0, mas*3, &r));
    ASSERT_EQ(mas * 1, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas*4));
    ASSERT_TRUE(b.get_extents()[0].is_valid());
    ASSERT_EQ(mas*4, b.get_extents()[0].length);
  }
  {
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    b.allocated_test(bluestore_pextent_t(0x40101000, 0x4000));
    b.allocated_test(bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET,
					    0x13000));

    b.allocated_test(bluestore_pextent_t(0x40118000, 0x7000));
    B.get_ref(coll.get(), 0x0, 0x3800);
    B.get_ref(coll.get(), 0x17c00, 0x6400);
    ASSERT_EQ(0x3800u + 0x6400u, B.get_referenced_bytes());
    b.set_flag(bluestore_blob_t::FLAG_SHARED);
    b.init_csum(Checksummer::CSUM_CRC32C, 12, 0x1e000);

    cout << "before: " << B << std::endl;
    PExtentVector r;
    ASSERT_FALSE(B.put_ref(coll.get(), 0x1800, 0x2000, &r));
    ASSERT_EQ(0x3800u + 0x6400u - 0x2000u, B.get_referenced_bytes());
    cout << "after: " << B << std::endl;
    cout << "r " << r << std::endl;
  }
  {
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    b.allocated_test(bluestore_pextent_t(1, 0x5000));
    b.allocated_test(bluestore_pextent_t(2, 0x5000));
    B.get_ref(coll.get(), 0x0, 0xa000);
    ASSERT_EQ(0xa000u, B.get_referenced_bytes());
    cout << "before: " << B << std::endl;
    PExtentVector r;
    ASSERT_FALSE(B.put_ref(coll.get(), 0x8000, 0x2000, &r));
    cout << "after: " << B << std::endl;
    cout << "r " << r << std::endl;
    ASSERT_EQ(0x8000u, B.get_referenced_bytes());
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x3002u, r[0].offset);
    ASSERT_EQ(0x2000u, r[0].length);
  }
  {
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    b.allocated_test(bluestore_pextent_t(1, 0x7000));
    b.allocated_test(bluestore_pextent_t(2, 0x7000));
    B.get_ref(coll.get(), 0x0, 0xe000);
    ASSERT_EQ(0xe000u, B.get_referenced_bytes());
    cout << "before: " << B << std::endl;
    PExtentVector r;
    ASSERT_FALSE(B.put_ref(coll.get(), 0, 0xb000, &r));
    ASSERT_EQ(0x3000u, B.get_referenced_bytes());
    cout << "after: " << B << std::endl;
    cout << "r " << r << std::endl;
    ASSERT_EQ(0x3000u, B.get_referenced_bytes());
    ASSERT_EQ(2u, r.size());
    ASSERT_EQ(1u, r[0].offset);
    ASSERT_EQ(0x7000u, r[0].length);
    ASSERT_EQ(2u, r[1].offset);
    ASSERT_EQ(0x3000u, r[1].length); // we have 0x1000 bytes less due to 
                                     // alignment caused by min_alloc_size = 0x2000
  }
  {
    BlueStore store(g_ceph_context, "", 0x4000);
    BlueStore::Cache *cache = BlueStore::Cache::create(
      g_ceph_context, "lru", NULL);
    BlueStore::CollectionRef coll(new BlueStore::Collection(&store, cache, coll_t()));
    BlueStore::Blob B;
    B.shared_blob = new BlueStore::SharedBlob(nullptr);
    B.shared_blob->get();  // hack to avoid dtor from running
    bluestore_blob_t& b = B.dirty_blob();
    b.allocated_test(bluestore_pextent_t(1, 0x5000));
    b.allocated_test(bluestore_pextent_t(2, 0x7000));
    B.get_ref(coll.get(), 0x0, 0xc000);
    ASSERT_EQ(0xc000u, B.get_referenced_bytes());
    cout << "before: " << B << std::endl;
    PExtentVector r;
    ASSERT_FALSE(B.put_ref(coll.get(), 0x2000, 0xa000, &r));
    cout << "after: " << B << std::endl;
    cout << "r " << r << std::endl;
    ASSERT_EQ(0x2000u, B.get_referenced_bytes());
    ASSERT_EQ(2u, r.size());
    ASSERT_EQ(0x4001u, r[0].offset);
    ASSERT_EQ(0x1000u, r[0].length);
    ASSERT_EQ(2u, r[1].offset);
    ASSERT_EQ(0x7000u, r[1].length);
    ASSERT_EQ(1u, b.get_extents()[0].offset);
    ASSERT_EQ(0x4000u, b.get_extents()[0].length);
  }
}

TEST(bluestore_blob_t, can_split)
{
  bluestore_blob_t a;
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
  a.allocated_test(bluestore_pextent_t(0x10000, 0x2000));
  a.allocated_test(bluestore_pextent_t(0x20000, 0x2000));
  ASSERT_TRUE(a.can_split_at(0x1000));
  ASSERT_TRUE(a.can_split_at(0x1800));
  a.init_csum(Checksummer::CSUM_CRC32C, 12, 0x4000);
  ASSERT_TRUE(a.can_split_at(0x1000));
  ASSERT_TRUE(a.can_split_at(0x2000));
  ASSERT_TRUE(a.can_split_at(0x3000));
  ASSERT_FALSE(a.can_split_at(0x2800));
}

TEST(bluestore_blob_t, prune_tail)
{
  bluestore_blob_t a;
  a.allocated_test(bluestore_pextent_t(0x10000, 0x2000));
  a.allocated_test(bluestore_pextent_t(0x20000, 0x2000));
  ASSERT_FALSE(a.can_prune_tail());
  a.allocated_test(
    bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, 0x2000));
  ASSERT_TRUE(a.can_prune_tail());
  a.prune_tail();
  ASSERT_FALSE(a.can_prune_tail());
  ASSERT_EQ(2u, a.get_extents().size());
  ASSERT_EQ(0x4000u, a.get_logical_length());

  a.allocated_test(
    bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, 0x2000));
  a.init_csum(Checksummer::CSUM_CRC32C_8, 12, 0x6000);
  ASSERT_EQ(6u, a.csum_data.length());
  ASSERT_TRUE(a.can_prune_tail());
  a.prune_tail();
  ASSERT_FALSE(a.can_prune_tail());
  ASSERT_EQ(2u, a.get_extents().size());
  ASSERT_EQ(0x4000u, a.get_logical_length());
  ASSERT_EQ(4u, a.csum_data.length());

  bluestore_blob_t b;
  b.allocated_test(
    bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, 0x2000));
  ASSERT_FALSE(a.can_prune_tail());
}

TEST(Blob, split)
{
  BlueStore store(g_ceph_context, "", 4096);
  BlueStore::Cache *cache = BlueStore::Cache::create(
    g_ceph_context, "lru", NULL);
  BlueStore::CollectionRef coll(new BlueStore::Collection(&store, cache, coll_t()));
  {
    BlueStore::Blob L, R;
    L.shared_blob = new BlueStore::SharedBlob(coll.get());
    L.shared_blob->get();  // hack to avoid dtor from running
    R.shared_blob = new BlueStore::SharedBlob(coll.get());
    R.shared_blob->get();  // hack to avoid dtor from running
    L.dirty_blob().allocated_test(bluestore_pextent_t(0x2000, 0x2000));
    L.dirty_blob().init_csum(Checksummer::CSUM_CRC32C, 12, 0x2000);
    L.get_ref(coll.get(), 0, 0x2000);
    L.split(coll.get(), 0x1000, &R);
    ASSERT_EQ(0x1000u, L.get_blob().get_logical_length());
    ASSERT_EQ(4u, L.get_blob().csum_data.length());
    ASSERT_EQ(1u, L.get_blob().get_extents().size());
    ASSERT_EQ(0x2000u, L.get_blob().get_extents().front().offset);
    ASSERT_EQ(0x1000u, L.get_blob().get_extents().front().length);
    ASSERT_EQ(0x1000u, L.get_referenced_bytes());
    ASSERT_EQ(0x1000u, R.get_blob().get_logical_length());
    ASSERT_EQ(4u, R.get_blob().csum_data.length());
    ASSERT_EQ(1u, R.get_blob().get_extents().size());
    ASSERT_EQ(0x3000u, R.get_blob().get_extents().front().offset);
    ASSERT_EQ(0x1000u, R.get_blob().get_extents().front().length);
    ASSERT_EQ(0x1000u, R.get_referenced_bytes());
  }
  {
    BlueStore::Blob L, R;
    L.shared_blob = new BlueStore::SharedBlob(coll.get());
    L.shared_blob->get();  // hack to avoid dtor from running
    R.shared_blob = new BlueStore::SharedBlob(coll.get());
    R.shared_blob->get();  // hack to avoid dtor from running
    L.dirty_blob().allocated_test(bluestore_pextent_t(0x2000, 0x1000));
    L.dirty_blob().allocated_test(bluestore_pextent_t(0x12000, 0x1000));
    L.dirty_blob().init_csum(Checksummer::CSUM_CRC32C, 12, 0x2000);
    L.get_ref(coll.get(), 0, 0x1000);
    L.get_ref(coll.get(), 0x1000, 0x1000);
    L.split(coll.get(), 0x1000, &R);
    ASSERT_EQ(0x1000u, L.get_blob().get_logical_length());
    ASSERT_EQ(4u, L.get_blob().csum_data.length());
    ASSERT_EQ(1u, L.get_blob().get_extents().size());
    ASSERT_EQ(0x2000u, L.get_blob().get_extents().front().offset);
    ASSERT_EQ(0x1000u, L.get_blob().get_extents().front().length);
    ASSERT_EQ(0x1000u, L.get_referenced_bytes());
    ASSERT_EQ(0x1000u, R.get_blob().get_logical_length());
    ASSERT_EQ(4u, R.get_blob().csum_data.length());
    ASSERT_EQ(1u, R.get_blob().get_extents().size());
    ASSERT_EQ(0x12000u, R.get_blob().get_extents().front().offset);
    ASSERT_EQ(0x1000u, R.get_blob().get_extents().front().length);
    ASSERT_EQ(0x1000u, R.get_referenced_bytes());
  }
}

TEST(Blob, legacy_decode)
{
  BlueStore store(g_ceph_context, "", 4096);
  BlueStore::Cache *cache = BlueStore::Cache::create(
    g_ceph_context, "lru", NULL);
  BlueStore::CollectionRef coll(new BlueStore::Collection(&store, cache, coll_t()));
  bufferlist bl, bl2;
  {
    BlueStore::Blob B;

    B.shared_blob = new BlueStore::SharedBlob(coll.get());
    B.dirty_blob().allocated_test(bluestore_pextent_t(0x1, 0x2000));
    B.dirty_blob().init_csum(Checksummer::CSUM_CRC32C, 12, 0x2000);
    B.get_ref(coll.get(), 0, 0xff0);
    B.get_ref(coll.get(), 0x1fff, 1);

    bluestore_extent_ref_map_t fake_ref_map;
    fake_ref_map.get(0, 0xff0);
    fake_ref_map.get(0x1fff, 1);

    size_t bound = 0, bound2 = 0;

    B.bound_encode(
      bound,
      1, /*struct_v*/
      0, /*sbid*/
      false);
    fake_ref_map.bound_encode(bound);

    B.bound_encode(
      bound2,
      2, /*struct_v*/
      0, /*sbid*/
      true);

    {
      auto app = bl.get_contiguous_appender(bound);
      auto app2 = bl2.get_contiguous_appender(bound2);
      B.encode(
        app,
        1, /*struct_v*/
        0, /*sbid*/
        false);
      fake_ref_map.encode(app);

      B.encode(
        app2,
        2, /*struct_v*/
        0, /*sbid*/
        true);
    }

    auto p = bl.front().begin_deep();
    auto p2 = bl2.front().begin_deep();
    BlueStore::Blob Bres, Bres2;
    Bres.shared_blob = new BlueStore::SharedBlob(coll.get());
    Bres2.shared_blob = new BlueStore::SharedBlob(coll.get());

    uint64_t sbid, sbid2;
    Bres.decode(
      coll.get(),
      p,
      1, /*struct_v*/
      &sbid,
      true);
    Bres2.decode(
      coll.get(),
      p2,
      2, /*struct_v*/
      &sbid2,
      true);

    ASSERT_EQ(0xff0u + 1u, Bres.get_blob_use_tracker().get_referenced_bytes());
    ASSERT_EQ(0xff0u + 1u, Bres2.get_blob_use_tracker().get_referenced_bytes());
    ASSERT_TRUE(Bres.get_blob_use_tracker().equal(Bres2.get_blob_use_tracker()));
  }
}

TEST(ExtentMap, seek_lextent)
{
  BlueStore store(g_ceph_context, "", 4096);
  BlueStore::LRUCache cache(g_ceph_context);
  BlueStore::CollectionRef coll(new BlueStore::Collection(&store, &cache, coll_t()));
  BlueStore::Onode onode(coll.get(), ghobject_t(), "");
  BlueStore::ExtentMap em(&onode);
  BlueStore::BlobRef br(new BlueStore::Blob);
  br->shared_blob = new BlueStore::SharedBlob(coll.get());

  ASSERT_EQ(em.extent_map.end(), em.seek_lextent(0));
  ASSERT_EQ(em.extent_map.end(), em.seek_lextent(100));

  em.extent_map.insert(*new BlueStore::Extent(100, 0, 100, br));
  auto a = em.find(100);
  ASSERT_EQ(a, em.seek_lextent(0));
  ASSERT_EQ(a, em.seek_lextent(99));
  ASSERT_EQ(a, em.seek_lextent(100));
  ASSERT_EQ(a, em.seek_lextent(101));
  ASSERT_EQ(a, em.seek_lextent(199));
  ASSERT_EQ(em.extent_map.end(), em.seek_lextent(200));

  em.extent_map.insert(*new BlueStore::Extent(200, 0, 100, br));
  auto b = em.find(200);
  ASSERT_EQ(a, em.seek_lextent(0));
  ASSERT_EQ(a, em.seek_lextent(99));
  ASSERT_EQ(a, em.seek_lextent(100));
  ASSERT_EQ(a, em.seek_lextent(101));
  ASSERT_EQ(a, em.seek_lextent(199));
  ASSERT_EQ(b, em.seek_lextent(200));
  ASSERT_EQ(b, em.seek_lextent(299));
  ASSERT_EQ(em.extent_map.end(), em.seek_lextent(300));

  em.extent_map.insert(*new BlueStore::Extent(400, 0, 100, br));
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
  BlueStore store(g_ceph_context, "", 4096);
  BlueStore::LRUCache cache(g_ceph_context);
  BlueStore::CollectionRef coll(new BlueStore::Collection(&store, &cache, coll_t()));
  BlueStore::Onode onode(coll.get(), ghobject_t(), "");
  BlueStore::ExtentMap em(&onode);
  BlueStore::BlobRef b(new BlueStore::Blob);
  b->shared_blob = new BlueStore::SharedBlob(coll.get());

  ASSERT_FALSE(em.has_any_lextents(0, 0));
  ASSERT_FALSE(em.has_any_lextents(0, 1000));
  ASSERT_FALSE(em.has_any_lextents(1000, 1000));

  em.extent_map.insert(*new BlueStore::Extent(100, 0, 100, b));
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

  em.extent_map.insert(*new BlueStore::Extent(200, 0, 100, b));
  ASSERT_TRUE(em.has_any_lextents(199, 1));
  ASSERT_TRUE(em.has_any_lextents(199, 2));
  ASSERT_TRUE(em.has_any_lextents(200, 2));
  ASSERT_TRUE(em.has_any_lextents(200, 200));
  ASSERT_TRUE(em.has_any_lextents(299, 1));
  ASSERT_FALSE(em.has_any_lextents(300, 1));

  em.extent_map.insert(*new BlueStore::Extent(400, 0, 100, b));
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
  BlueStore store(g_ceph_context, "", 4096);
  BlueStore::LRUCache cache(g_ceph_context);
  BlueStore::CollectionRef coll(new BlueStore::Collection(&store, &cache, coll_t()));
  BlueStore::Onode onode(coll.get(), ghobject_t(), "");
  BlueStore::ExtentMap em(&onode);
  BlueStore::BlobRef b1(new BlueStore::Blob);
  BlueStore::BlobRef b2(new BlueStore::Blob);
  BlueStore::BlobRef b3(new BlueStore::Blob);
  b1->shared_blob = new BlueStore::SharedBlob(coll.get());
  b2->shared_blob = new BlueStore::SharedBlob(coll.get());
  b3->shared_blob = new BlueStore::SharedBlob(coll.get());

  em.extent_map.insert(*new BlueStore::Extent(0, 0, 100, b1));
  em.extent_map.insert(*new BlueStore::Extent(100, 0, 100, b2));
  ASSERT_EQ(0, em.compress_extent_map(0, 10000));
  ASSERT_EQ(2u, em.extent_map.size());

  em.extent_map.insert(*new BlueStore::Extent(200, 100, 100, b2));
  em.extent_map.insert(*new BlueStore::Extent(300, 200, 100, b2));
  ASSERT_EQ(0, em.compress_extent_map(0, 0));
  ASSERT_EQ(0, em.compress_extent_map(100000, 1000));
  ASSERT_EQ(2, em.compress_extent_map(0, 100000));
  ASSERT_EQ(2u, em.extent_map.size());

  em.extent_map.erase(em.find(100));
  em.extent_map.insert(*new BlueStore::Extent(100, 0, 100, b2));
  em.extent_map.insert(*new BlueStore::Extent(200, 100, 100, b3));
  em.extent_map.insert(*new BlueStore::Extent(300, 200, 100, b2));
  ASSERT_EQ(0, em.compress_extent_map(0, 1));
  ASSERT_EQ(0, em.compress_extent_map(0, 100000));
  ASSERT_EQ(4u, em.extent_map.size());

  em.extent_map.insert(*new BlueStore::Extent(400, 300, 100, b2));
  em.extent_map.insert(*new BlueStore::Extent(500, 500, 100, b2));
  em.extent_map.insert(*new BlueStore::Extent(600, 600, 100, b2));
  em.extent_map.insert(*new BlueStore::Extent(700, 0, 100, b1));
  em.extent_map.insert(*new BlueStore::Extent(800, 0, 100, b3));
  ASSERT_EQ(0, em.compress_extent_map(0, 99));
  ASSERT_EQ(0, em.compress_extent_map(800, 1000));
  ASSERT_EQ(2, em.compress_extent_map(100, 500));
  ASSERT_EQ(7u, em.extent_map.size());
  em.extent_map.erase(em.find(300));
  em.extent_map.erase(em.find(500));  
  em.extent_map.erase(em.find(700));
  em.extent_map.insert(*new BlueStore::Extent(400, 300, 100, b2));
  em.extent_map.insert(*new BlueStore::Extent(500, 400, 100, b2));
  em.extent_map.insert(*new BlueStore::Extent(700, 500, 100, b2));
  ASSERT_EQ(1, em.compress_extent_map(0, 1000));
  ASSERT_EQ(6u, em.extent_map.size());
}

TEST(GarbageCollector, BasicTest)
{
  BlueStore::LRUCache cache(g_ceph_context);
  BlueStore store(g_ceph_context, "", 4096);
  BlueStore::CollectionRef coll(new BlueStore::Collection(&store, &cache, coll_t()));
  BlueStore::Onode onode(coll.get(), ghobject_t(), "");
  BlueStore::ExtentMap em(&onode);

  BlueStore::old_extent_map_t old_extents;


 /*
  min_alloc_size = 4096
  original disposition
  extent1 <loffs = 100, boffs = 100, len  = 10>
    -> blob1<compressed, len_on_disk=4096, logical_len=8192>
  extent2 <loffs = 200, boffs = 200, len  = 10>
    -> blob2<raw, len_on_disk=4096, llen=4096>
  extent3 <loffs = 300, boffs = 300, len  = 10>
    -> blob1<compressed, len_on_disk=4096, llen=8192>
  extent4 <loffs = 4096, boffs = 0, len  = 10>
    -> blob3<raw, len_on_disk=4096, llen=4096>
  on write(300~100) resulted in
  extent1 <loffs = 100, boffs = 100, len  = 10>
    -> blob1<compressed, len_on_disk=4096, logical_len=8192>
  extent2 <loffs = 200, boffs = 200, len  = 10>
    -> blob2<raw, len_on_disk=4096, llen=4096>
  extent3 <loffs = 300, boffs = 300, len  = 100>
    -> blob4<raw, len_on_disk=4096, llen=4096>
  extent4 <loffs = 4096, boffs = 0, len  = 10>
    -> blob3<raw, len_on_disk=4096, llen=4096>
  */  
  {
    BlueStore::GarbageCollector gc(g_ceph_context);
    int64_t saving;
    BlueStore::BlobRef b1(new BlueStore::Blob);
    BlueStore::BlobRef b2(new BlueStore::Blob);
    BlueStore::BlobRef b3(new BlueStore::Blob);
    BlueStore::BlobRef b4(new BlueStore::Blob);
    b1->shared_blob = new BlueStore::SharedBlob(coll.get());
    b2->shared_blob = new BlueStore::SharedBlob(coll.get());
    b3->shared_blob = new BlueStore::SharedBlob(coll.get());
    b4->shared_blob = new BlueStore::SharedBlob(coll.get());
    b1->dirty_blob().set_compressed(0x2000, 0x1000);
    b1->dirty_blob().allocated_test(bluestore_pextent_t(0, 0x1000));
    b2->dirty_blob().allocated_test(bluestore_pextent_t(1, 0x1000));
    b3->dirty_blob().allocated_test(bluestore_pextent_t(2, 0x1000));
    b4->dirty_blob().allocated_test(bluestore_pextent_t(3, 0x1000));
    em.extent_map.insert(*new BlueStore::Extent(100, 100, 10, b1));
    b1->get_ref(coll.get(), 100, 10);
    em.extent_map.insert(*new BlueStore::Extent(200, 200, 10, b2));
    b2->get_ref(coll.get(), 200, 10);
    em.extent_map.insert(*new BlueStore::Extent(300, 300, 100, b4));
    b4->get_ref(coll.get(), 300, 100);
    em.extent_map.insert(*new BlueStore::Extent(4096, 0, 10, b3));
    b3->get_ref(coll.get(), 0, 10);

    old_extents.push_back(*new BlueStore::OldExtent(300, 300, 10, b1)); 

    saving = gc.estimate(300, 100, em, old_extents, 4096);
    ASSERT_EQ(saving, 1);
    auto& to_collect = gc.get_extents_to_collect();
    ASSERT_EQ(to_collect.size(), 1u);
    ASSERT_EQ(to_collect[0], bluestore_pextent_t(100,10) );

    em.clear();
    old_extents.clear();
  }
 /*
  original disposition
  min_alloc_size = 0x10000
  extent1 <loffs = 0, boffs = 0, len  = 0x40000>
    -> blob1<compressed, len_on_disk=0x20000, logical_len=0x40000>
  Write 0x8000~37000 resulted in the following extent map prior to GC
  for the last write_small(0x30000~0xf000):

  extent1 <loffs = 0, boffs = 0, len  = 0x8000>
    -> blob1<compressed, len_on_disk=0x20000, logical_len=0x40000>
  extent2 <loffs = 0x8000, boffs = 0x8000, len  = 0x8000>
    -> blob2<raw, len_on_disk=0x10000, llen=0x10000>
  extent3 <loffs = 0x10000, boffs = 0, len  = 0x20000>
    -> blob3<raw, len_on_disk=0x20000, llen=0x20000>
  extent4 <loffs = 0x30000, boffs = 0, len  = 0xf000>
    -> blob4<raw, len_on_disk=0x10000, llen=0x10000>
  extent5 <loffs = 0x3f000, boffs = 0x3f000, len  = 0x1000>
    -> blob1<compressed, len_on_disk=0x20000, llen=0x40000>
  */  
  {
    BlueStore store(g_ceph_context, "", 0x10000);
    BlueStore::CollectionRef coll(new BlueStore::Collection(&store, &cache, coll_t()));
    BlueStore::Onode onode(coll.get(), ghobject_t(), "");
    BlueStore::ExtentMap em(&onode);

    BlueStore::old_extent_map_t old_extents;
    BlueStore::GarbageCollector gc(g_ceph_context);
    int64_t saving;
    BlueStore::BlobRef b1(new BlueStore::Blob);
    BlueStore::BlobRef b2(new BlueStore::Blob);
    BlueStore::BlobRef b3(new BlueStore::Blob);
    BlueStore::BlobRef b4(new BlueStore::Blob);
    b1->shared_blob = new BlueStore::SharedBlob(coll.get());
    b2->shared_blob = new BlueStore::SharedBlob(coll.get());
    b3->shared_blob = new BlueStore::SharedBlob(coll.get());
    b4->shared_blob = new BlueStore::SharedBlob(coll.get());
    b1->dirty_blob().set_compressed(0x40000, 0x20000);
    b1->dirty_blob().allocated_test(bluestore_pextent_t(0, 0x20000));
    b2->dirty_blob().allocated_test(bluestore_pextent_t(1, 0x10000));
    b3->dirty_blob().allocated_test(bluestore_pextent_t(2, 0x20000));
    b4->dirty_blob().allocated_test(bluestore_pextent_t(3, 0x10000));

    em.extent_map.insert(*new BlueStore::Extent(0, 0, 0x8000, b1));
    b1->get_ref(coll.get(), 0, 0x8000);
    em.extent_map.insert(
      *new BlueStore::Extent(0x8000, 0x8000, 0x8000, b2)); // new extent
    b2->get_ref(coll.get(), 0x8000, 0x8000);
    em.extent_map.insert(
      *new BlueStore::Extent(0x10000, 0, 0x20000, b3)); // new extent
    b3->get_ref(coll.get(), 0, 0x20000);
    em.extent_map.insert(
      *new BlueStore::Extent(0x30000, 0, 0xf000, b4)); // new extent
    b4->get_ref(coll.get(), 0, 0xf000);
    em.extent_map.insert(*new BlueStore::Extent(0x3f000, 0x3f000, 0x1000, b1));
    b1->get_ref(coll.get(), 0x3f000, 0x1000);

    old_extents.push_back(*new BlueStore::OldExtent(0x8000, 0x8000, 0x8000, b1)); 
    old_extents.push_back(
      *new BlueStore::OldExtent(0x10000, 0x10000, 0x20000, b1));
    old_extents.push_back(*new BlueStore::OldExtent(0x30000, 0x30000, 0xf000, b1)); 

    saving = gc.estimate(0x30000, 0xf000, em, old_extents, 0x10000);
    ASSERT_EQ(saving, 2);
    auto& to_collect = gc.get_extents_to_collect();
    ASSERT_EQ(to_collect.size(), 2u);
    ASSERT_TRUE(to_collect[0] == bluestore_pextent_t(0x0,0x8000) ||
		  to_collect[1] == bluestore_pextent_t(0x0,0x8000));
    ASSERT_TRUE(to_collect[0] == bluestore_pextent_t(0x3f000,0x1000) ||
		  to_collect[1] == bluestore_pextent_t(0x3f000,0x1000));

    em.clear();
    old_extents.clear();
  }
 /*
  original disposition
  min_alloc_size = 0x1000
  extent1 <loffs = 0, boffs = 0, len  = 0x4000>
    -> blob1<compressed, len_on_disk=0x2000, logical_len=0x4000>
  write 0x3000~4000 resulted in the following extent map
  (future feature - suppose we can compress incoming write prior to
  GC invocation)

  extent1 <loffs = 0, boffs = 0, len  = 0x4000>
    -> blob1<compressed, len_on_disk=0x2000, logical_len=0x4000>
  extent2 <loffs = 0x3000, boffs = 0, len  = 0x4000>
    -> blob2<compressed, len_on_disk=0x2000, llen=0x4000>
  */  
  {
    BlueStore::GarbageCollector gc(g_ceph_context);
    int64_t saving;
    BlueStore::BlobRef b1(new BlueStore::Blob);
    BlueStore::BlobRef b2(new BlueStore::Blob);
    b1->shared_blob = new BlueStore::SharedBlob(coll.get());
    b2->shared_blob = new BlueStore::SharedBlob(coll.get());
    b1->dirty_blob().set_compressed(0x4000, 0x2000);
    b1->dirty_blob().allocated_test(bluestore_pextent_t(0, 0x2000));
    b2->dirty_blob().set_compressed(0x4000, 0x2000);
    b2->dirty_blob().allocated_test(bluestore_pextent_t(0, 0x2000));

    em.extent_map.insert(*new BlueStore::Extent(0, 0, 0x3000, b1));
    b1->get_ref(coll.get(), 0, 0x3000);
    em.extent_map.insert(
      *new BlueStore::Extent(0x3000, 0, 0x4000, b2)); // new extent
    b2->get_ref(coll.get(), 0, 0x4000);

    old_extents.push_back(*new BlueStore::OldExtent(0x3000, 0x3000, 0x1000, b1)); 

    saving = gc.estimate(0x3000, 0x4000, em, old_extents, 0x1000);
    ASSERT_EQ(saving, 0);
    auto& to_collect = gc.get_extents_to_collect();
    ASSERT_EQ(to_collect.size(), 0u);
    em.clear();
    old_extents.clear();
  }
 /*
  original disposition
  min_alloc_size = 0x10000
  extent0 <loffs = 0, boffs = 0, len  = 0x20000>
    -> blob0<compressed, len_on_disk=0x10000, logical_len=0x20000>
  extent1 <loffs = 0x20000, boffs = 0, len  = 0x20000>
     -> blob1<compressed, len_on_disk=0x10000, logical_len=0x20000>
  write 0x8000~37000 resulted in the following extent map prior
  to GC for the last write_small(0x30000~0xf000)

  extent0 <loffs = 0, boffs = 0, len  = 0x8000>
    -> blob0<compressed, len_on_disk=0x10000, logical_len=0x20000>
  extent2 <loffs = 0x8000, boffs = 0x8000, len  = 0x8000>
    -> blob2<raw, len_on_disk=0x10000, llen=0x10000>
  extent3 <loffs = 0x10000, boffs = 0, len  = 0x20000>
    -> blob3<raw, len_on_disk=0x20000, llen=0x20000>
  extent4 <loffs = 0x30000, boffs = 0, len  = 0xf000>
    -> blob4<raw, len_on_disk=0x1000, llen=0x1000>
  extent5 <loffs = 0x3f000, boffs = 0x1f000, len  = 0x1000>
   -> blob1<compressed, len_on_disk=0x10000, llen=0x20000>
  */  
  {
    BlueStore store(g_ceph_context, "", 0x10000);
    BlueStore::CollectionRef coll(new BlueStore::Collection(&store, &cache, coll_t()));
    BlueStore::Onode onode(coll.get(), ghobject_t(), "");
    BlueStore::ExtentMap em(&onode);

    BlueStore::old_extent_map_t old_extents;
    BlueStore::GarbageCollector gc(g_ceph_context);
    int64_t saving;
    BlueStore::BlobRef b0(new BlueStore::Blob);
    BlueStore::BlobRef b1(new BlueStore::Blob);
    BlueStore::BlobRef b2(new BlueStore::Blob);
    BlueStore::BlobRef b3(new BlueStore::Blob);
    BlueStore::BlobRef b4(new BlueStore::Blob);
    b0->shared_blob = new BlueStore::SharedBlob(coll.get());
    b1->shared_blob = new BlueStore::SharedBlob(coll.get());
    b2->shared_blob = new BlueStore::SharedBlob(coll.get());
    b3->shared_blob = new BlueStore::SharedBlob(coll.get());
    b4->shared_blob = new BlueStore::SharedBlob(coll.get());
    b0->dirty_blob().set_compressed(0x2000, 0x1000);
    b0->dirty_blob().allocated_test(bluestore_pextent_t(0, 0x10000));
    b1->dirty_blob().set_compressed(0x20000, 0x10000);
    b1->dirty_blob().allocated_test(bluestore_pextent_t(0, 0x10000));
    b2->dirty_blob().allocated_test(bluestore_pextent_t(1, 0x10000));
    b3->dirty_blob().allocated_test(bluestore_pextent_t(2, 0x20000));
    b4->dirty_blob().allocated_test(bluestore_pextent_t(3, 0x1000));

    em.extent_map.insert(*new BlueStore::Extent(0, 0, 0x8000, b0));
    b0->get_ref(coll.get(), 0, 0x8000);
    em.extent_map.insert(
      *new BlueStore::Extent(0x8000, 0x8000, 0x8000, b2)); // new extent
    b2->get_ref(coll.get(), 0x8000, 0x8000);
    em.extent_map.insert(
      *new BlueStore::Extent(0x10000, 0, 0x20000, b3)); // new extent
    b3->get_ref(coll.get(), 0, 0x20000);
    em.extent_map.insert(
      *new BlueStore::Extent(0x30000, 0, 0xf000, b4)); // new extent
    b4->get_ref(coll.get(), 0, 0xf000);
    em.extent_map.insert(*new BlueStore::Extent(0x3f000, 0x1f000, 0x1000, b1));
    b1->get_ref(coll.get(), 0x1f000, 0x1000);

    old_extents.push_back(*new BlueStore::OldExtent(0x8000, 0x8000, 0x8000, b0)); 
    old_extents.push_back(
      *new BlueStore::OldExtent(0x10000, 0x10000, 0x10000, b0)); 
    old_extents.push_back(
      *new BlueStore::OldExtent(0x20000, 0x00000, 0x1f000, b1)); 

    saving = gc.estimate(0x30000, 0xf000, em, old_extents, 0x10000);
    ASSERT_EQ(saving, 2);
    auto& to_collect = gc.get_extents_to_collect();
    ASSERT_EQ(to_collect.size(), 2u);
    ASSERT_TRUE(to_collect[0] == bluestore_pextent_t(0x0,0x8000) ||
		  to_collect[1] == bluestore_pextent_t(0x0,0x8000));
    ASSERT_TRUE(to_collect[0] == bluestore_pextent_t(0x3f000,0x1000) ||
		  to_collect[1] == bluestore_pextent_t(0x3f000,0x1000));

    em.clear();
    old_extents.clear();
  }
}

TEST(BlueStoreRepairer, StoreSpaceTracker)
{
  BlueStoreRepairer::StoreSpaceTracker bmap0;
  bmap0.init((uint64_t)4096 * 1024 * 1024 * 1024, 0x1000);
  ASSERT_EQ(bmap0.granularity, 2 * 1024 * 1024);
  ASSERT_EQ(bmap0.collections_bfs.size(), 2048 * 1024);
  ASSERT_EQ(bmap0.objects_bfs.size(), 2048 * 1024);

  BlueStoreRepairer::StoreSpaceTracker bmap;
  bmap.init(0x2000 * 0x1000 - 1, 0x1000, 512 * 1024);
  ASSERT_EQ(bmap.granularity, 0x1000);
  ASSERT_EQ(bmap.collections_bfs.size(), 0x2000);
  ASSERT_EQ(bmap.objects_bfs.size(), 0x2000);

  coll_t cid;
  ghobject_t hoid;

  ASSERT_FALSE(bmap.is_used(cid, 0));
  ASSERT_FALSE(bmap.is_used(hoid, 0));
  bmap.set_used(0, 1, cid, hoid);
  ASSERT_TRUE(bmap.is_used(cid, 0));
  ASSERT_TRUE(bmap.is_used(hoid, 0));

  ASSERT_FALSE(bmap.is_used(cid, 0x1023));
  ASSERT_FALSE(bmap.is_used(hoid, 0x1023));
  ASSERT_FALSE(bmap.is_used(cid, 0x2023));
  ASSERT_FALSE(bmap.is_used(hoid, 0x2023));
  ASSERT_FALSE(bmap.is_used(cid, 0x3023));
  ASSERT_FALSE(bmap.is_used(hoid, 0x3023));
  bmap.set_used(0x1023, 0x3000, cid, hoid);
  ASSERT_TRUE(bmap.is_used(cid, 0x1023));
  ASSERT_TRUE(bmap.is_used(hoid, 0x1023));
  ASSERT_TRUE(bmap.is_used(cid, 0x2023));
  ASSERT_TRUE(bmap.is_used(hoid, 0x2023));
  ASSERT_TRUE(bmap.is_used(cid, 0x3023));
  ASSERT_TRUE(bmap.is_used(hoid, 0x3023));

  ASSERT_FALSE(bmap.is_used(cid, 0x9001));
  ASSERT_FALSE(bmap.is_used(hoid, 0x9001));
  ASSERT_FALSE(bmap.is_used(cid, 0xa001));
  ASSERT_FALSE(bmap.is_used(hoid, 0xa001));
  ASSERT_FALSE(bmap.is_used(cid, 0xb000));
  ASSERT_FALSE(bmap.is_used(hoid, 0xb000));
  ASSERT_FALSE(bmap.is_used(cid, 0xc000));
  ASSERT_FALSE(bmap.is_used(hoid, 0xc000));
  bmap.set_used(0x9001, 0x2fff, cid, hoid);
  ASSERT_TRUE(bmap.is_used(cid, 0x9001));
  ASSERT_TRUE(bmap.is_used(hoid, 0x9001));
  ASSERT_TRUE(bmap.is_used(cid, 0xa001));
  ASSERT_TRUE(bmap.is_used(hoid, 0xa001));
  ASSERT_TRUE(bmap.is_used(cid, 0xb001));
  ASSERT_TRUE(bmap.is_used(hoid, 0xb001));
  ASSERT_FALSE(bmap.is_used(cid, 0xc000));
  ASSERT_FALSE(bmap.is_used(hoid, 0xc000));

  bmap.set_used(0xa001, 0x2, cid, hoid);
  ASSERT_TRUE(bmap.is_used(cid, 0x9001));
  ASSERT_TRUE(bmap.is_used(hoid, 0x9001));
  ASSERT_TRUE(bmap.is_used(cid, 0xa001));
  ASSERT_TRUE(bmap.is_used(hoid, 0xa001));
  ASSERT_TRUE(bmap.is_used(cid, 0xb001));
  ASSERT_TRUE(bmap.is_used(hoid, 0xb001));
  ASSERT_FALSE(bmap.is_used(cid, 0xc000));
  ASSERT_FALSE(bmap.is_used(hoid, 0xc000));

  ASSERT_FALSE(bmap.is_used(cid, 0xc0000));
  ASSERT_FALSE(bmap.is_used(hoid, 0xc0000));
  ASSERT_FALSE(bmap.is_used(cid, 0xc1000));
  ASSERT_FALSE(bmap.is_used(hoid, 0xc1000));

  bmap.set_used(0xc0000, 0x2000, cid, hoid);
  ASSERT_TRUE(bmap.is_used(cid, 0xc0000));
  ASSERT_TRUE(bmap.is_used(hoid, 0xc0000));
  ASSERT_TRUE(bmap.is_used(cid, 0xc1000));
  ASSERT_TRUE(bmap.is_used(hoid, 0xc1000));

  interval_set<uint64_t> extents;
  extents.insert(0,0x500);
  extents.insert(0x800,0x100);
  extents.insert(0x1000,0x1000);
  extents.insert(0xa001,1);
  extents.insert(0xa0000,0xff8);

  ASSERT_EQ(bmap.filter_out(extents), 3);
  ASSERT_TRUE(bmap.is_used(cid));
  ASSERT_TRUE(bmap.is_used(hoid));
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
