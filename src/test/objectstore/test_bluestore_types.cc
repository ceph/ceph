// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "os/bluestore/bluestore_types.h"
#include "gtest/gtest.h"
#include "include/stringify.h"
#include "common/ceph_time.h"
#include "os/bluestore/BlueStore.h"
#include "os/bluestore/simple_bitmap.h"
#include "os/bluestore/AvlAllocator.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/global_context.h"
#include "perfglue/heap_profiler.h"
#include "os/bluestore/Writer.h"
#include "common/pretty_binary.h"

#include <sstream>

#define _STR(x) #x
#define STRINGIFY(x) _STR(x)

using namespace std;

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
  P(ghobject_t);
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
  P(range_seg_t);
  P(sb_info_t);
  P(SimpleBitmap);
  cout << "map<uint64_t,uint64_t>\t" << sizeof(map<uint64_t, uint64_t>)
       << std::endl;
  cout << "map<char,char>\t" << sizeof(map<char, char>) << std::endl;
}

void dump_mempools() {
  ostringstream ostr;
  auto f =
      Formatter::create_unique("json-pretty", "json-pretty", "json-pretty");
  ostr << "Mempools: ";
  f->open_object_section("mempools");
  mempool::dump(f.get());
  f->close_section();
  f->flush(ostr);
  cout << ostr.str() << std::endl;
}
/*void get_mempool_stats(uint64_t* total_bytes, uint64_t* total_items)
{
  uint64_t meta_allocated = mempool::bluestore_cache_meta::allocated_bytes();
  uint64_t onode_allocated = mempool::bluestore_cache_onode::allocated_bytes();
  uint64_t other_allocated = mempool::bluestore_cache_other::allocated_bytes();

  uint64_t meta_items = mempool::bluestore_cache_meta::allocated_items();
  uint64_t onode_items = mempool::bluestore_cache_onode::allocated_items();
  uint64_t other_items = mempool::bluestore_cache_other::allocated_items();
  cout << "meta(" << meta_allocated << "/" << meta_items
       << ") onode(" << onode_allocated << "/" << onode_items
       << ") other(" << other_allocated << "/" << other_items
       << ")" << std::endl;
  *total_bytes = meta_allocated + onode_allocated + other_allocated;
  *total_items = onode_items;
}*/

TEST(sb_info_space_efficient_map_t, basic) {
  sb_info_space_efficient_map_t sb_info;
  const size_t num_shared = 1000;
  for (size_t i = 0; i < num_shared; i += 2) {
    auto &sbi = sb_info.add_maybe_stray(i);
    sbi.pool_id = i;
  }
  ASSERT_TRUE(sb_info.find(0) != sb_info.end());
  ASSERT_TRUE(sb_info.find(1) == sb_info.end());
  ASSERT_TRUE(sb_info.find(2) != sb_info.end());
  ASSERT_TRUE(sb_info.find(4)->pool_id == 4);
  ASSERT_TRUE(sb_info.find(num_shared) == sb_info.end());

  // ordered insertion
  sb_info.add_or_adopt(num_shared).pool_id = num_shared;
  ASSERT_TRUE(sb_info.find(num_shared) != sb_info.end());
  ASSERT_TRUE(sb_info.find(num_shared)->pool_id == num_shared);

  // out of order insertion
  sb_info.add_or_adopt(1).pool_id = 1;
  ASSERT_TRUE(sb_info.find(1) != sb_info.end());
  ASSERT_TRUE(sb_info.find(1)->pool_id == 1);

  // ordered insertion
  sb_info.add_maybe_stray(num_shared + 1).pool_id = num_shared + 1;
  ASSERT_TRUE(sb_info.find(num_shared + 1) != sb_info.end());
  ASSERT_TRUE(sb_info.find(num_shared + 1)->pool_id == num_shared + 1);

  // out of order insertion
  sb_info.add_maybe_stray(105).pool_id = 105;
  ASSERT_TRUE(sb_info.find(105) != sb_info.end());
  ASSERT_TRUE(sb_info.find(105)->pool_id == 105);
}

TEST(sb_info_space_efficient_map_t, size) {
  const size_t num_shared = 10000000;
  sb_info_space_efficient_map_t sb_info;

  BlueStore store(g_ceph_context, "", 4096);
  BlueStore::OnodeCacheShard *oc =
      BlueStore::OnodeCacheShard::create(g_ceph_context, "lru", NULL);
  BlueStore::BufferCacheShard *bc =
      BlueStore::BufferCacheShard::create(&store, "lru", NULL);

  auto coll = ceph::make_ref<BlueStore::Collection>(&store, oc, bc, coll_t());

  for (size_t i = 0; i < num_shared; i++) {
    auto &sbi = sb_info.add_or_adopt(i);
    // primarily to silent the 'unused' warning
    ceph_assert(sbi.pool_id == sb_info_t::INVALID_POOL_ID);
  }
  dump_mempools();
}

TEST(bluestore_extent_ref_map_t, add) {
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

TEST(bluestore_extent_ref_map_t, get) {
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

TEST(bluestore_extent_ref_map_t, put) {
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

TEST(bluestore_extent_ref_map_t, contains) {
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

TEST(bluestore_extent_ref_map_t, intersects) {
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

TEST(bluestore_blob_t, calc_csum) {
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
       csum_type < Checksummer::CSUM_MAX; ++csum_type) {
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

TEST(bluestore_blob_t, csum_bench) {
  bufferlist bl;
  bufferptr bp(10485760);
  for (char *a = bp.c_str(); a < bp.c_str() + bp.length(); ++a)
    *a = (unsigned long)a & 0xff;
  bl.append(bp);
  int count = 256;
  for (unsigned csum_type = 1; csum_type < Checksummer::CSUM_MAX; ++csum_type) {
    bluestore_blob_t b;
    b.init_csum(csum_type, 12, bl.length());
    ceph::mono_clock::time_point start = ceph::mono_clock::now();
    for (int i = 0; i < count; ++i) {
      b.calc_csum(0, bl);
    }
    ceph::mono_clock::time_point end = ceph::mono_clock::now();
    auto dur = std::chrono::duration_cast<ceph::timespan>(end - start);
    double mbsec = (double)count * (double)bl.length() / 1000000.0 /
                   (double)dur.count() * 1000000000.0;
    cout << "csum_type " << Checksummer::get_csum_type_string(csum_type) << ", "
         << dur << " seconds, " << mbsec << " MB/sec" << std::endl;
  }
}

TEST(Blob, put_ref) {
  {
    BlueStore store(g_ceph_context, "", 4096);
    BlueStore::OnodeCacheShard *oc =
        BlueStore::OnodeCacheShard::create(g_ceph_context, "lru", NULL);
    BlueStore::BufferCacheShard *bc =
        BlueStore::BufferCacheShard::create(&store, "lru", NULL);

    auto coll = ceph::make_ref<BlueStore::Collection>(&store, oc, bc, coll_t());
    BlueStore::Blob b(coll.get());
    b.dirty_blob().allocated_test(bluestore_pextent_t(0x40715000, 0x2000));
    b.dirty_blob().allocated_test(
        bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, 0x8000));
    b.dirty_blob().allocated_test(bluestore_pextent_t(0x4071f000, 0x5000));
    b.get_ref(coll.get(), 0, 0x1200);
    b.get_ref(coll.get(), 0xae00, 0x4200);
    ASSERT_EQ(0x5400u, b.get_referenced_bytes());
    cout << b << std::endl;
    PExtentVector r;

    ASSERT_FALSE(b.put_ref(coll.get(), 0, 0x1200, &r));
    ASSERT_EQ(0x4200u, b.get_referenced_bytes());
    cout << " r " << r << std::endl;
    cout << b << std::endl;

    r.clear();
    ASSERT_TRUE(b.put_ref(coll.get(), 0xae00, 0x4200, &r));
    ASSERT_EQ(0u, b.get_referenced_bytes());
    cout << " r " << r << std::endl;
    cout << b << std::endl;
  }

  unsigned mas = 4096;
  BlueStore store(g_ceph_context, "", 8192);
  BlueStore::OnodeCacheShard *oc =
      BlueStore::OnodeCacheShard::create(g_ceph_context, "lru", NULL);
  BlueStore::BufferCacheShard *bc =
      BlueStore::BufferCacheShard::create(&store, "lru", NULL);
  auto coll = ceph::make_ref<BlueStore::Collection>(&store, oc, bc, coll_t());

  {
    BlueStore::Blob B(coll.get());
    bluestore_blob_t &b = B.dirty_blob();
    PExtentVector r;
    b.allocated_test(bluestore_pextent_t(0, mas * 2));
    B.get_ref(coll.get(), 0, mas * 2);
    ASSERT_EQ(mas * 2, B.get_referenced_bytes());
    ASSERT_TRUE(b.is_allocated(0, mas * 2));
    ASSERT_TRUE(B.put_ref(coll.get(), 0, mas * 2, &r));
    ASSERT_EQ(0u, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0u, r[0].offset);
    ASSERT_EQ(mas * 2, r[0].length);
    ASSERT_FALSE(b.is_allocated(0, mas * 2));
    ASSERT_FALSE(b.is_allocated(0, mas));
    ASSERT_FALSE(b.is_allocated(mas, 0));
    ASSERT_FALSE(b.get_extents()[0].is_valid());
    ASSERT_EQ(mas * 2, b.get_extents()[0].length);
  }
  {
    BlueStore::Blob B(coll.get());
    bluestore_blob_t &b = B.dirty_blob();
    PExtentVector r;
    b.allocated_test(bluestore_pextent_t(123, mas * 2));
    B.get_ref(coll.get(), 0, mas * 2);
    ASSERT_EQ(mas * 2, B.get_referenced_bytes());
    ASSERT_FALSE(B.put_ref(coll.get(), 0, mas, &r));
    ASSERT_EQ(mas, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas * 2));
    ASSERT_TRUE(B.put_ref(coll.get(), mas, mas, &r));
    ASSERT_EQ(0u, B.get_referenced_bytes());
    ASSERT_EQ(0u, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(123u, r[0].offset);
    ASSERT_EQ(mas * 2, r[0].length);
    ASSERT_FALSE(b.is_allocated(0, mas * 2));
    ASSERT_FALSE(b.get_extents()[0].is_valid());
    ASSERT_EQ(mas * 2, b.get_extents()[0].length);
  }
  {
    BlueStore::Blob B(coll.get());
    bluestore_blob_t &b = B.dirty_blob();
    PExtentVector r;
    b.allocated_test(bluestore_pextent_t(1, mas));
    b.allocated_test(bluestore_pextent_t(2, mas));
    b.allocated_test(bluestore_pextent_t(3, mas));
    b.allocated_test(bluestore_pextent_t(4, mas));
    B.get_ref(coll.get(), 0, mas * 4);
    ASSERT_EQ(mas * 4, B.get_referenced_bytes());
    ASSERT_FALSE(B.put_ref(coll.get(), mas, mas, &r));
    ASSERT_EQ(mas * 3, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas * 4));
    ASSERT_TRUE(b.is_allocated(mas, mas));
    ASSERT_FALSE(B.put_ref(coll.get(), mas * 2, mas, &r));
    ASSERT_EQ(mas * 2, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(mas * 2, mas));
    ASSERT_TRUE(b.is_allocated(0, mas * 4));
    ASSERT_FALSE(B.put_ref(coll.get(), mas * 3, mas, &r));
    ASSERT_EQ(mas, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(2u, r.size());
    ASSERT_EQ(3u, r[0].offset);
    ASSERT_EQ(mas, r[0].length);
    ASSERT_EQ(4u, r[1].offset);
    ASSERT_EQ(mas, r[1].length);
    ASSERT_TRUE(b.is_allocated(0, mas * 2));
    ASSERT_FALSE(b.is_allocated(mas * 2, mas * 2));
    ASSERT_TRUE(b.get_extents()[0].is_valid());
    ASSERT_TRUE(b.get_extents()[1].is_valid());
    ASSERT_FALSE(b.get_extents()[2].is_valid());
    ASSERT_EQ(3u, b.get_extents().size());
  }
  {
    BlueStore::Blob B(coll.get());
    bluestore_blob_t &b = B.dirty_blob();
    PExtentVector r;
    b.allocated_test(bluestore_pextent_t(1, mas));
    b.allocated_test(bluestore_pextent_t(2, mas));
    b.allocated_test(bluestore_pextent_t(3, mas));
    b.allocated_test(bluestore_pextent_t(4, mas));
    b.allocated_test(bluestore_pextent_t(5, mas));
    b.allocated_test(bluestore_pextent_t(6, mas));
    B.get_ref(coll.get(), 0, mas * 6);
    ASSERT_EQ(mas * 6, B.get_referenced_bytes());
    ASSERT_FALSE(B.put_ref(coll.get(), mas, mas, &r));
    ASSERT_EQ(mas * 5, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas * 6));
    ASSERT_FALSE(B.put_ref(coll.get(), mas * 2, mas, &r));
    ASSERT_EQ(mas * 4, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas * 6));
    ASSERT_FALSE(B.put_ref(coll.get(), mas * 3, mas, &r));
    ASSERT_EQ(mas * 3, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(2u, r.size());
    ASSERT_EQ(3u, r[0].offset);
    ASSERT_EQ(mas, r[0].length);
    ASSERT_EQ(4u, r[1].offset);
    ASSERT_EQ(mas, r[1].length);
    ASSERT_TRUE(b.is_allocated(0, mas * 2));
    ASSERT_FALSE(b.is_allocated(mas * 2, mas * 2));
    ASSERT_TRUE(b.is_allocated(mas * 4, mas * 2));
    ASSERT_EQ(5u, b.get_extents().size());
    ASSERT_TRUE(b.get_extents()[0].is_valid());
    ASSERT_TRUE(b.get_extents()[1].is_valid());
    ASSERT_FALSE(b.get_extents()[2].is_valid());
    ASSERT_TRUE(b.get_extents()[3].is_valid());
    ASSERT_TRUE(b.get_extents()[4].is_valid());
  }
  {
    BlueStore::Blob B(coll);
    bluestore_blob_t &b = B.dirty_blob();
    PExtentVector r;
    b.allocated_test(bluestore_pextent_t(1, mas * 6));
    B.get_ref(coll.get(), 0, mas * 6);
    ASSERT_EQ(mas * 6, B.get_referenced_bytes());
    ASSERT_FALSE(B.put_ref(coll.get(), mas, mas, &r));
    ASSERT_EQ(mas * 5, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas * 6));
    ASSERT_FALSE(B.put_ref(coll.get(), mas * 2, mas, &r));
    ASSERT_EQ(mas * 4, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas * 6));
    ASSERT_FALSE(B.put_ref(coll.get(), mas * 3, mas, &r));
    ASSERT_EQ(mas * 3, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x2001u, r[0].offset);
    ASSERT_EQ(mas * 2, r[0].length);
    ASSERT_TRUE(b.is_allocated(0, mas * 2));
    ASSERT_FALSE(b.is_allocated(mas * 2, mas * 2));
    ASSERT_TRUE(b.is_allocated(mas * 4, mas * 2));
    ASSERT_EQ(3u, b.get_extents().size());
    ASSERT_TRUE(b.get_extents()[0].is_valid());
    ASSERT_FALSE(b.get_extents()[1].is_valid());
    ASSERT_TRUE(b.get_extents()[2].is_valid());
  }
  {
    BlueStore::Blob B(coll);
    bluestore_blob_t &b = B.dirty_blob();
    PExtentVector r;
    b.allocated_test(bluestore_pextent_t(1, mas * 4));
    b.allocated_test(bluestore_pextent_t(2, mas * 4));
    b.allocated_test(bluestore_pextent_t(3, mas * 4));
    B.get_ref(coll.get(), 0, mas * 12);
    ASSERT_EQ(mas * 12, B.get_referenced_bytes());
    ASSERT_FALSE(B.put_ref(coll.get(), mas, mas, &r));
    ASSERT_EQ(mas * 11, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas * 12));
    ASSERT_FALSE(B.put_ref(coll.get(), mas * 9, mas, &r));
    ASSERT_EQ(mas * 10, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas * 12));
    ASSERT_FALSE(B.put_ref(coll.get(), mas * 2, mas * 7, &r));
    ASSERT_EQ(mas * 3, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(3u, r.size());
    ASSERT_EQ(0x2001u, r[0].offset);
    ASSERT_EQ(mas * 2, r[0].length);
    ASSERT_EQ(0x2u, r[1].offset);
    ASSERT_EQ(mas * 4, r[1].length);
    ASSERT_EQ(0x3u, r[2].offset);
    ASSERT_EQ(mas * 2, r[2].length);
    ASSERT_TRUE(b.is_allocated(0, mas * 2));
    ASSERT_FALSE(b.is_allocated(mas * 2, mas * 8));
    ASSERT_TRUE(b.is_allocated(mas * 10, mas * 2));
    ASSERT_EQ(3u, b.get_extents().size());
    ASSERT_TRUE(b.get_extents()[0].is_valid());
    ASSERT_FALSE(b.get_extents()[1].is_valid());
    ASSERT_TRUE(b.get_extents()[2].is_valid());
  }
  {
    BlueStore::Blob B(coll);
    bluestore_blob_t &b = B.dirty_blob();
    PExtentVector r;
    b.allocated_test(bluestore_pextent_t(1, mas * 4));
    b.allocated_test(bluestore_pextent_t(2, mas * 4));
    b.allocated_test(bluestore_pextent_t(3, mas * 4));
    B.get_ref(coll.get(), 0, mas * 12);
    ASSERT_EQ(mas * 12, B.get_referenced_bytes());
    ASSERT_FALSE(B.put_ref(coll.get(), mas, mas, &r));
    ASSERT_EQ(mas * 11, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas * 12));
    ASSERT_FALSE(B.put_ref(coll.get(), mas * 9, mas, &r));
    ASSERT_EQ(mas * 10, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas * 12));
    ASSERT_FALSE(B.put_ref(coll.get(), mas * 2, mas * 7, &r));
    ASSERT_EQ(mas * 3, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(3u, r.size());
    ASSERT_EQ(0x2001u, r[0].offset);
    ASSERT_EQ(mas * 2, r[0].length);
    ASSERT_EQ(0x2u, r[1].offset);
    ASSERT_EQ(mas * 4, r[1].length);
    ASSERT_EQ(0x3u, r[2].offset);
    ASSERT_EQ(mas * 2, r[2].length);
    ASSERT_TRUE(b.is_allocated(0, mas * 2));
    ASSERT_FALSE(b.is_allocated(mas * 2, mas * 8));
    ASSERT_TRUE(b.is_allocated(mas * 10, mas * 2));
    ASSERT_EQ(3u, b.get_extents().size());
    ASSERT_TRUE(b.get_extents()[0].is_valid());
    ASSERT_FALSE(b.get_extents()[1].is_valid());
    ASSERT_TRUE(b.get_extents()[2].is_valid());
    ASSERT_FALSE(B.put_ref(coll.get(), 0, mas, &r));
    ASSERT_EQ(mas * 2, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x1u, r[0].offset);
    ASSERT_EQ(mas * 2, r[0].length);
    ASSERT_EQ(2u, b.get_extents().size());
    ASSERT_FALSE(b.get_extents()[0].is_valid());
    ASSERT_TRUE(b.get_extents()[1].is_valid());
    ASSERT_TRUE(B.put_ref(coll.get(), mas * 10, mas * 2, &r));
    ASSERT_EQ(mas * 0, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x2003u, r[0].offset);
    ASSERT_EQ(mas * 2, r[0].length);
    ASSERT_EQ(1u, b.get_extents().size());
    ASSERT_FALSE(b.get_extents()[0].is_valid());
  }
  {
    BlueStore::Blob B(coll);
    bluestore_blob_t &b = B.dirty_blob();
    PExtentVector r;
    b.allocated_test(bluestore_pextent_t(1, mas * 4));
    b.allocated_test(bluestore_pextent_t(2, mas * 4));
    b.allocated_test(bluestore_pextent_t(3, mas * 4));
    B.get_ref(coll.get(), 0, mas * 12);
    ASSERT_EQ(mas * 12, B.get_referenced_bytes());
    ASSERT_FALSE(B.put_ref(coll.get(), mas, mas, &r));
    ASSERT_EQ(mas * 11, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas * 12));
    ASSERT_FALSE(B.put_ref(coll.get(), mas * 9, mas, &r));
    ASSERT_EQ(mas * 10, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas * 12));
    ASSERT_FALSE(B.put_ref(coll.get(), mas * 2, mas * 7, &r));
    ASSERT_EQ(mas * 3, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(3u, r.size());
    ASSERT_EQ(0x2001u, r[0].offset);
    ASSERT_EQ(mas * 2, r[0].length);
    ASSERT_EQ(0x2u, r[1].offset);
    ASSERT_EQ(mas * 4, r[1].length);
    ASSERT_EQ(0x3u, r[2].offset);
    ASSERT_EQ(mas * 2, r[2].length);
    ASSERT_TRUE(b.is_allocated(0, mas * 2));
    ASSERT_FALSE(b.is_allocated(mas * 2, mas * 8));
    ASSERT_TRUE(b.is_allocated(mas * 10, mas * 2));
    ASSERT_EQ(3u, b.get_extents().size());
    ASSERT_TRUE(b.get_extents()[0].is_valid());
    ASSERT_FALSE(b.get_extents()[1].is_valid());
    ASSERT_TRUE(b.get_extents()[2].is_valid());
    ASSERT_FALSE(B.put_ref(coll.get(), mas * 10, mas * 2, &r));
    ASSERT_EQ(mas * 1, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x2003u, r[0].offset);
    ASSERT_EQ(mas * 2, r[0].length);
    ASSERT_EQ(2u, b.get_extents().size());
    ASSERT_TRUE(b.get_extents()[0].is_valid());
    ASSERT_FALSE(b.get_extents()[1].is_valid());
    ASSERT_TRUE(B.put_ref(coll.get(), 0, mas, &r));
    ASSERT_EQ(mas * 0, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x1u, r[0].offset);
    ASSERT_EQ(mas * 2, r[0].length);
    ASSERT_EQ(1u, b.get_extents().size());
    ASSERT_FALSE(b.get_extents()[0].is_valid());
  }
  {
    BlueStore::Blob B(coll.get());
    bluestore_blob_t &b = B.dirty_blob();
    PExtentVector r;
    b.allocated_test(bluestore_pextent_t(1, mas * 8));
    B.get_ref(coll.get(), 0, mas * 8);
    ASSERT_EQ(mas * 8, B.get_referenced_bytes());
    ASSERT_FALSE(B.put_ref(coll.get(), 0, mas, &r));
    ASSERT_EQ(mas * 7, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas * 8));
    ASSERT_FALSE(B.put_ref(coll.get(), mas * 7, mas, &r));
    ASSERT_EQ(mas * 6, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas * 8));
    ASSERT_FALSE(B.put_ref(coll.get(), mas * 2, mas, &r));
    ASSERT_EQ(mas * 5, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, 8));
    ASSERT_FALSE(B.put_ref(coll.get(), mas * 3, mas * 4, &r));
    ASSERT_EQ(mas * 1, B.get_referenced_bytes());
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x2001u, r[0].offset);
    ASSERT_EQ(mas * 6, r[0].length);
    ASSERT_TRUE(b.is_allocated(0, mas * 2));
    ASSERT_FALSE(b.is_allocated(mas * 2, mas * 6));
    ASSERT_EQ(2u, b.get_extents().size());
    ASSERT_TRUE(b.get_extents()[0].is_valid());
    ASSERT_FALSE(b.get_extents()[1].is_valid());
    ASSERT_TRUE(B.put_ref(coll.get(), mas, mas, &r));
    ASSERT_EQ(mas * 0, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(1u, r.size());
    ASSERT_EQ(0x1u, r[0].offset);
    ASSERT_EQ(mas * 2, r[0].length);
    ASSERT_EQ(1u, b.get_extents().size());
    ASSERT_FALSE(b.get_extents()[0].is_valid());
  }
  // verify csum chunk size if factored in properly
  {
    BlueStore::Blob B(coll.get());
    bluestore_blob_t &b = B.dirty_blob();
    PExtentVector r;
    b.allocated_test(bluestore_pextent_t(0, mas * 4));
    b.init_csum(Checksummer::CSUM_CRC32C, 14, mas * 4);
    B.get_ref(coll.get(), 0, mas * 4);
    ASSERT_EQ(mas * 4, B.get_referenced_bytes());
    ASSERT_TRUE(b.is_allocated(0, mas * 4));
    ASSERT_FALSE(B.put_ref(coll.get(), 0, mas * 3, &r));
    ASSERT_EQ(mas * 1, B.get_referenced_bytes());
    cout << "r " << r << " " << b << std::endl;
    ASSERT_EQ(0u, r.size());
    ASSERT_TRUE(b.is_allocated(0, mas * 4));
    ASSERT_TRUE(b.get_extents()[0].is_valid());
    ASSERT_EQ(mas * 4, b.get_extents()[0].length);
  }
  {
    BlueStore::Blob B(coll.get());
    bluestore_blob_t &b = B.dirty_blob();
    b.allocated_test(bluestore_pextent_t(0x40101000, 0x4000));
    b.allocated_test(
        bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET, 0x13000));

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
    BlueStore::Blob B(coll.get());
    bluestore_blob_t &b = B.dirty_blob();
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
    BlueStore::Blob B(coll.get());
    bluestore_blob_t &b = B.dirty_blob();
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
    ASSERT_EQ(0x3000u,
              r[1].length); // we have 0x1000 bytes less due to
                            // alignment caused by min_alloc_size = 0x2000
  }
  {
    BlueStore store(g_ceph_context, "", 0x4000);
    BlueStore::OnodeCacheShard *oc =
        BlueStore::OnodeCacheShard::create(g_ceph_context, "lru", NULL);
    BlueStore::BufferCacheShard *bc =
        BlueStore::BufferCacheShard::create(&store, "lru", NULL);

    auto coll = ceph::make_ref<BlueStore::Collection>(&store, oc, bc, coll_t());
    BlueStore::Blob B(coll.get());
    bluestore_blob_t &b = B.dirty_blob();
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

TEST(bluestore_blob_t, can_split) {
  bluestore_blob_t a;
  ASSERT_TRUE(a.can_split());
  a.flags = bluestore_blob_t::FLAG_SHARED;
  ASSERT_FALSE(a.can_split());
  a.flags = bluestore_blob_t::FLAG_COMPRESSED;
  ASSERT_FALSE(a.can_split());
  a.flags = bluestore_blob_t::FLAG_HAS_UNUSED;
  ASSERT_FALSE(a.can_split());
}

TEST(bluestore_blob_t, can_split_at) {
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

TEST(bluestore_blob_t, prune_tail) {
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

TEST(Blob, split) {
  BlueStore store(g_ceph_context, "", 4096);
  BlueStore::OnodeCacheShard *oc =
      BlueStore::OnodeCacheShard::create(g_ceph_context, "lru", NULL);
  BlueStore::BufferCacheShard *bc =
      BlueStore::BufferCacheShard::create(&store, "lru", NULL);
  auto coll = ceph::make_ref<BlueStore::Collection>(&store, oc, bc, coll_t());
  {
    BlueStore::Blob L(coll.get());
    BlueStore::Blob R(coll.get());
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
    BlueStore::Blob L(coll.get());
    BlueStore::Blob R(coll.get());
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

TEST(Blob, legacy_decode) {
  BlueStore store(g_ceph_context, "", 4096);
  BlueStore::OnodeCacheShard *oc =
      BlueStore::OnodeCacheShard::create(g_ceph_context, "lru", NULL);
  BlueStore::BufferCacheShard *bc =
      BlueStore::BufferCacheShard::create(&store, "lru", NULL);
  auto coll = ceph::make_ref<BlueStore::Collection>(&store, oc, bc, coll_t());
  bufferlist bl, bl2;
  {
    BlueStore::Blob B(coll.get());

    B.dirty_blob().allocated_test(bluestore_pextent_t(0x1, 0x2000));
    B.dirty_blob().init_csum(Checksummer::CSUM_CRC32C, 12, 0x2000);
    B.get_ref(coll.get(), 0, 0xff0);
    B.get_ref(coll.get(), 0x1fff, 1);

    bluestore_extent_ref_map_t fake_ref_map;
    fake_ref_map.get(0, 0xff0);
    fake_ref_map.get(0x1fff, 1);

    size_t bound = 0, bound2 = 0;

    B.bound_encode(bound, 1, /*struct_v*/
                   0,        /*sbid*/
                   false);
    fake_ref_map.bound_encode(bound);

    B.bound_encode(bound2, 2, /*struct_v*/
                   0,         /*sbid*/
                   true);

    {
      auto app = bl.get_contiguous_appender(bound);
      auto app2 = bl2.get_contiguous_appender(bound2);
      B.encode(app, 1, /*struct_v*/
               0,      /*sbid*/
               false);
      fake_ref_map.encode(app);

      B.encode(app2, 2, /*struct_v*/
               0,       /*sbid*/
               true);
    }

    auto p = bl.front().begin_deep();
    auto p2 = bl2.front().begin_deep();
    BlueStore::Blob Bres(coll.get());
    BlueStore::Blob Bres2(coll.get());

    uint64_t sbid, sbid2;
    Bres.decode(p, 1, /*struct_v*/
                &sbid, true, coll.get());
    Bres2.decode(p2, 2, /*struct_v*/
                 &sbid2, true, coll.get());

    ASSERT_EQ(0xff0u + 1u, Bres.get_blob_use_tracker().get_referenced_bytes());
    ASSERT_EQ(0xff0u + 1u, Bres2.get_blob_use_tracker().get_referenced_bytes());
    ASSERT_TRUE(
        Bres.get_blob_use_tracker().equal(Bres2.get_blob_use_tracker()));
  }
}

TEST(ExtentMap, seek_lextent) {
  BlueStore store(g_ceph_context, "", 4096);
  BlueStore::OnodeCacheShard *oc =
      BlueStore::OnodeCacheShard::create(g_ceph_context, "lru", NULL);
  BlueStore::BufferCacheShard *bc =
      BlueStore::BufferCacheShard::create(&store, "lru", NULL);

  auto coll = ceph::make_ref<BlueStore::Collection>(&store, oc, bc, coll_t());
  BlueStore::Onode onode(coll.get(), ghobject_t(), "");
  BlueStore::ExtentMap em(
      &onode,
      g_ceph_context->_conf->bluestore_extent_map_inline_shard_prealloc_size);
  BlueStore::BlobRef br(coll->new_blob());

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

TEST(ExtentMap, has_any_lextents) {
  BlueStore store(g_ceph_context, "", 4096);
  BlueStore::OnodeCacheShard *oc =
      BlueStore::OnodeCacheShard::create(g_ceph_context, "lru", NULL);
  BlueStore::BufferCacheShard *bc =
      BlueStore::BufferCacheShard::create(&store, "lru", NULL);
  auto coll = ceph::make_ref<BlueStore::Collection>(&store, oc, bc, coll_t());
  BlueStore::Onode onode(coll.get(), ghobject_t(), "");
  BlueStore::ExtentMap em(
      &onode,
      g_ceph_context->_conf->bluestore_extent_map_inline_shard_prealloc_size);
  BlueStore::BlobRef b(coll->new_blob());

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

void erase_and_delete(BlueStore::ExtentMap &em, size_t v) {
  auto d = em.find(v);
  ASSERT_NE(d, em.extent_map.end());
  em.extent_map.erase(d);
  delete &*d;
}

TEST(ExtentMap, compress_extent_map) {
  BlueStore store(g_ceph_context, "", 4096);
  BlueStore::OnodeCacheShard *oc =
      BlueStore::OnodeCacheShard::create(g_ceph_context, "lru", NULL);
  BlueStore::BufferCacheShard *bc =
      BlueStore::BufferCacheShard::create(&store, "lru", NULL);

  auto coll = ceph::make_ref<BlueStore::Collection>(&store, oc, bc, coll_t());
  BlueStore::Onode onode(coll.get(), ghobject_t(), "");
  BlueStore::ExtentMap em(
      &onode,
      g_ceph_context->_conf->bluestore_extent_map_inline_shard_prealloc_size);
  BlueStore::BlobRef b1(coll->new_blob());
  BlueStore::BlobRef b2(coll->new_blob());
  BlueStore::BlobRef b3(coll->new_blob());
  b1->set_shared_blob(new BlueStore::SharedBlob(coll.get()));
  b2->set_shared_blob(new BlueStore::SharedBlob(coll.get()));
  b3->set_shared_blob(new BlueStore::SharedBlob(coll.get()));

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
  erase_and_delete(em, 100);
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
  erase_and_delete(em, 300);
  erase_and_delete(em, 500);
  erase_and_delete(em, 700);
  em.extent_map.insert(*new BlueStore::Extent(400, 300, 100, b2));
  em.extent_map.insert(*new BlueStore::Extent(500, 400, 100, b2));
  em.extent_map.insert(*new BlueStore::Extent(700, 500, 100, b2));
  ASSERT_EQ(1, em.compress_extent_map(0, 1000));
  ASSERT_EQ(6u, em.extent_map.size());
}

class BlueStoreFixture :
  virtual public ::testing::Test,
  virtual public ::testing::WithParamInterface<std::vector<int>>
 {
public:
  BlueStore* store;
  BlueStore::OnodeCacheShard *oc;
  BlueStore::BufferCacheShard *bc;
  BlueStore::CollectionRef coll;
  uint32_t au_size = 0;
  uint32_t csum_order = 12;
  BlueStore::OnodeRef onode;

  explicit BlueStoreFixture() {}
  void Init(uint32_t _au_size) {
    au_size = _au_size;
    store = new BlueStore(g_ceph_context, "", au_size);
    oc = BlueStore::OnodeCacheShard::create(g_ceph_context, "lru", NULL);
    bc = BlueStore::BufferCacheShard::create(store, "lru", NULL);
    coll = ceph::make_ref<BlueStore::Collection>(store, oc, bc, coll_t());
  }
  void SetUp() override {
    std::vector param = GetParam();
    Init(param[0]);
    onode = new BlueStore::Onode(coll.get(), ghobject_t(), "");
  }
  void TearDown() override {
    onode.reset(nullptr);
    coll.reset(nullptr);
    delete bc;
    delete oc;
    if (store->debug_get_alloc()) {
      delete store->debug_get_alloc();
      store->debug_get_alloc() = nullptr;
    }
    delete store;
  }
};

class PunchHoleFixture : public BlueStoreFixture
{
  public:
  struct logical_range_t {
    uint32_t offset = 0;
    uint32_t length = 0;
    uint32_t compressed = 0;
  };
  struct punch_range_t {
    uint32_t offset = 0;
    uint32_t length = 0;
  };

  interval_set<uint64_t> disk_allocated;
  std::set<BlueStore::BlobRef> blobs_created;
  std::set<BlueStore::SharedBlobRef> blobs_shared_created;
  BlueStore::volatile_statfs statfs;

  interval_set<uint64_t> disk_to_free;
  std::set<BlueStore::BlobRef> blobs_to_free;
  std::set<BlueStore::SharedBlobRef> blobs_shared_to_free;
  BlueStore::volatile_statfs statfs_to_free;

  uint32_t allocate_block = 10; //let's not start from 0
  uint32_t allocate_offset = 0;

  uint32_t align_nom;
  uint32_t align_denom;
  uint32_t compr_nom = 0;
  uint32_t compr_denom = 0;
  uint32_t compr_low = 0;
  uint32_t compr_high = 0;
  uint32_t shared_nom = 0;
  uint32_t shared_denom = 0;
  // random maybe aligned
  uint32_t rma(
    uint32_t low,
    uint32_t high = 0) {
      if (high == 0) {
        high = low;
        low = 0;
      }
      if (low == high) {
        return low;
      }
      if (rand() % align_denom < align_nom) {
        if (a2align(high) > a2roundup(low)) {
          uint32_t v = rand() % (a2align(high) - a2roundup(low));
          return a2align(v) + a2roundup(low);
        }
      }
      return rand() % (high - low) + low;
    }

  void SetUp() override {
    std::vector param = GetParam();
    BlueStoreFixture::SetUp(); //uses param[0]
    align_nom = param[1];
    align_denom = param[2];
    if (param.size() > 7) {
      ceph_assert(param.size() >= 11);
      compr_nom = param[7];
      compr_denom = param[8];
      compr_low = param[9];
      compr_high = param[10];
    }
    if (param.size() > 11) {
      ceph_assert(param.size() >= 13);
      shared_nom = param[11];
      shared_denom = param[12];
    }
  }
  void clear() {
    disk_allocated.clear();
    blobs_created.clear();
    blobs_shared_created.clear();
    statfs.reset();
    disk_to_free.clear();
    blobs_to_free.clear();
    blobs_shared_to_free.clear();
    statfs_to_free.reset();
    allocate_block = 10;
    allocate_offset = 0;
  }

  void append(interval_set<uint64_t>& d, const PExtentVector& vec) {
    for (auto v: vec) {
      if (v.length > 0)
        d.insert(v.offset, v.length);
    }
  }
  interval_set<uint64_t> to_iset(const PExtentVector& vec) {
    interval_set<uint64_t> set;
    for (auto& v : vec) {
      set.insert(v.offset, v.length);
    }
    return set;
  }
  std::set<BlueStore::BlobRef> to_set(const std::vector<BlueStore::BlobRef>& vec) {
    std::set<BlueStore::BlobRef> set;
    for (auto& b : vec) {
      ceph_assert(!set.contains(b));
      set.insert(b);
    }
    return set;
  }
  PExtentVector allocate(uint32_t size) {
    if (rand()% 6 < 5) {
      return allocate_continue(size);
    }
    ++allocate_block;
    allocate_offset = 0;
    PExtentVector v;
    if (size > 0)
      v.emplace_back((uint64_t)allocate_block << 32 | allocate_offset, size);
    allocate_offset += size;
    return v;
  }
  PExtentVector allocate_continue(uint32_t size) {
    PExtentVector v;
    if (size > 0)
      v.emplace_back((uint64_t)allocate_block << 32 | allocate_offset, size);
    allocate_offset += size;
    return v;
  }
  uint32_t a2roundup(uint32_t x) {
    return p2roundup(x, au_size);
  }
  uint32_t a2align(uint32_t x) {
    return p2align(x, au_size);
  }
  uint32_t a2phase(uint32_t x) {
    return p2phase(x, au_size);
  }

// Fills onode data on specific range.
// Simulates blob-like operation.
// Specifies which part of the data will be punched in future.
  void populate(logical_range_t blob_like, punch_range_t will_punch)
  {
    if (compr_denom > 0 && rand() % compr_denom < compr_nom) {
      blob_like.compressed = blob_like.length *
        (rand() % (compr_high - compr_low) + compr_low);
      populate_compressed(blob_like, will_punch);
      return;
    }
    uint32_t al_start = a2align(blob_like.offset);
    uint32_t al_hole_begin = a2roundup(will_punch.offset);
    uint32_t al_hole_end = a2align(will_punch.offset + will_punch.length);
    uint32_t al_end = a2roundup(blob_like.offset + blob_like.length);
    if (will_punch.length == 0) {
      // no punch, no hole
      al_hole_begin = al_end;
      al_hole_end = al_end;
    } else {
      if (blob_like.offset == will_punch.offset) {
        al_hole_begin = al_start;
      }
      if (will_punch.offset + will_punch.length == blob_like.offset + blob_like.length) {
        al_hole_end = al_end;
      }
      if (al_hole_end < al_hole_begin) {
        al_hole_begin = al_end;
        al_hole_end = al_end;
      }
    }
    PExtentVector disk;
    PExtentVector d_a = allocate(al_hole_begin - al_start);
    PExtentVector d_b = allocate_continue(al_hole_end - al_hole_begin);
    PExtentVector d_c = allocate_continue(al_end - al_hole_end);

    bool blob_remains = al_start != al_hole_begin || al_hole_end != al_end;
    BlueStore::BlobRef b(coll->new_blob());
    coll->open_shared_blob(0, b);
    blobs_created.insert(b);
    if (!blob_remains) {
      blobs_to_free.insert(b);
    }
    append(disk_allocated, d_a);
    append(disk_allocated, d_b);
    append(disk_allocated, d_c);

    disk.insert(disk.end(), d_a.begin(), d_a.end());
    disk.insert(disk.end(), d_b.begin(), d_b.end());
    disk.insert(disk.end(), d_c.begin(), d_c.end());

    uint32_t blob_length = al_end - al_start;
    bluestore_blob_t &bb = b->dirty_blob();
    bb.init_csum(Checksummer::CSUM_CRC32C, csum_order, blob_length);
    ceph_assert(p2phase(blob_length, au_size) == 0);
    uint32_t num_aus = blob_length / au_size;
    for (size_t i = 0; i < num_aus; ++i) {
      bb.set_csum_item(i, 0);
    }
    bb.allocated(0, blob_length, disk);
    BlueStore::Extent *ext = new BlueStore::Extent(
      blob_like.offset, a2phase(blob_like.offset), blob_like.length, b);
    onode->extent_map.extent_map.insert(*ext);
    b->get_ref(coll.get(), a2phase(blob_like.offset), blob_like.length);
    bb.mark_used(a2phase(blob_like.offset), blob_like.length);

    //when shared is triggered, select how much is will be shared
    bool do_shared = shared_denom !=0 && rand() % shared_denom < shared_nom;
    uint32_t hole_range = al_hole_end - al_hole_begin;
    if (do_shared && hole_range > 0) {
      uint32_t x = (rand() % shared_denom < shared_nom)
        ? 0 : a2align(rand() % hole_range);
      uint32_t y = (rand() % shared_denom < shared_nom)
        ? hole_range : hole_range - a2roundup(rand() % (hole_range - x));
      if (x == y) {
        x = 0;
        y = hole_range;
      }
      coll->make_blob_shared(rand(), b);
      BlueStore::BlobRef cb = coll->new_blob();
      b->dup(*cb);
      ceph_assert(d_b.size() == 1);
      ceph_assert(d_b[0].length == hole_range);
      PExtentVector d_b_non_shared;
      if (x != 0) {
        d_b_non_shared.emplace_back(d_b[0].offset, x);
      }
      if (y != hole_range) {
        d_b_non_shared.emplace_back(d_b[0].offset + y, hole_range - y);
      }
      append(disk_to_free, d_b_non_shared);
      if (x < y) {
        cb->get_dirty_shared_blob()->get_ref(d_b[0].offset + x, y - x);
        statfs_to_free.allocated() += y - x;
      }
    } else {
      append(disk_to_free, d_b);
    }
    statfs_to_free.stored() -= will_punch.length;
    statfs_to_free.allocated() -= al_hole_end - al_hole_begin;
  }

  void populate_compressed(logical_range_t blob_like, punch_range_t will_punch)
  {
    //compressed are never aligning data
    ceph_assert(blob_like.compressed > 0);
    uint32_t al_size = a2roundup(blob_like.compressed);
    bool blob_remains =
      will_punch.offset > blob_like.offset ||
      will_punch.offset + will_punch.length < blob_like.offset + blob_like.length;
    PExtentVector disk = allocate(al_size);

    BlueStore::BlobRef b(coll->new_blob());
    coll->open_shared_blob(0, b);
    blobs_created.insert(b);
    if (!blob_remains) {
      blobs_to_free.insert(b);
    }
    append(disk_allocated, disk);

    uint32_t blob_length = al_size;
    bluestore_blob_t &bb = b->dirty_blob();
    bb.set_compressed(blob_like.length, blob_like.compressed);
    bb.init_csum(Checksummer::CSUM_CRC32C, csum_order, blob_length);
    ceph_assert(p2phase(blob_length, au_size) == 0);
    uint32_t num_aus = blob_length / au_size;
    for (size_t i = 0; i < num_aus; ++i) {
      bb.set_csum_item(i, 0);
    }
    bb.allocated(0, blob_length, disk);
    BlueStore::Extent *ext = new BlueStore::Extent(
      blob_like.offset, 0, blob_like.length, b);
    onode->extent_map.extent_map.insert(*ext);
    b->get_ref(coll.get(), 0, blob_like.length);

    //when shared is triggered, it is all or nothing
    bool do_shared = shared_denom !=0 && rand() % shared_denom < shared_nom;
    bool create_additional_ref = false;
    if (do_shared) {
      create_additional_ref = rand() % 2 == 0;
      coll->make_blob_shared(rand(), b);
      BlueStore::BlobRef cb = coll->new_blob();
      b->dup(*cb);
      ceph_assert(disk.size() == 1);
      ceph_assert(disk[0].length == al_size);
      if (create_additional_ref) {
        cb->get_dirty_shared_blob()->get_ref(disk[0].offset, al_size);
      }
    }
    statfs_to_free.stored() -= will_punch.length;
    statfs_to_free.compressed_original() -= will_punch.length;
    if (!blob_remains) {
      statfs_to_free.compressed() -= blob_like.compressed;
      if (!create_additional_ref) {
        statfs_to_free.allocated() -= al_size;
        statfs_to_free.compressed_allocated() -= al_size;
        append(disk_to_free, disk);
      }
    }
  }
};


class ExtentMapFixture : virtual public ::testing::Test {

public:
  BlueStore store;
  BlueStore::OnodeCacheShard *oc;
  BlueStore::BufferCacheShard *bc;
  BlueStore::CollectionRef coll;

  static constexpr uint32_t au_size = 4096;
  uint32_t blob_size = 65536;
  size_t csum_order = 12; // 1^12 = 4096 bytes

  struct au {
    uint32_t chksum;
    uint32_t refs;
  };
  std::vector<au> disk;

  // test onode that glues some simplifications in representation
  // with actual BlueStore's onode
  struct t_onode {
    BlueStore::OnodeRef onode;  // actual BS onode
    std::vector<uint32_t> data; // map to AUs
    static constexpr uint32_t empty = std::numeric_limits<uint32_t>::max();
  };
  void print(std::ostream &out, t_onode &onode) {
    for (size_t i = 0; i < onode.data.size(); ++i) {
      if (i != 0)
        out << " ";
      if (onode.data[i] == t_onode::empty) {
        out << "-";
      } else {
        out << std::hex << onode.data[i] << "/" << disk[onode.data[i]].chksum
            << ":" << std::dec << disk[onode.data[i]].refs;
      }
    }
  }
  explicit ExtentMapFixture() : store(g_ceph_context, "", au_size) {
    oc = BlueStore::OnodeCacheShard::create(g_ceph_context, "lru", NULL);
    bc = BlueStore::BufferCacheShard::create(&store, "lru", NULL);
    coll = ceph::make_ref<BlueStore::Collection>(&store, oc, bc, coll_t());
  }

  void SetUp() override {}
  void TearDown() override {}

  // takes new space from disk, initializes csums
  // returns index of first au
  uint32_t allocate(uint32_t num_au) {
    uint32_t pos = disk.size();
    disk.resize(pos + num_au);
    for (uint32_t i = 0; i < num_au; i++) {
      uint32_t p = pos + i;
      disk[p].chksum = 2 * p + 1;
      disk[p].refs = 0;
    }
    return pos;
  }
  void release(uint32_t &au_idx) {
    if (au_idx != t_onode::empty) {
      disk_unref(au_idx);
    }
    au_idx = t_onode::empty;
  }
  void disk_ref(uint32_t au_idx) { ++disk[au_idx].refs; }
  void disk_unref(uint32_t au_idx) {
    ceph_assert(disk[au_idx].refs > 0);
    --disk[au_idx].refs;
  }

  t_onode create() {
    t_onode res;
    res.onode = new BlueStore::Onode(coll.get(), ghobject_t(), "");
    return res;
  }

  void fillup(t_onode &onode, uint32_t end) {
    if (end > onode.data.size()) {
      size_t e = onode.data.size();
      onode.data.resize(end);
      for (; e < end; ++e) {
        onode.data[e] = t_onode::empty;
      }
    }
  }
  void punch_hole(t_onode &onode, uint32_t off, uint32_t len) {
    ceph_assert((off % au_size) == 0);
    ceph_assert((len % au_size) == 0);
    uint32_t i = off / au_size;
    uint32_t end = (off + len) / au_size;
    fillup(onode, end);
    while (i < end && i < onode.data.size()) {
      if (onode.data[i] != t_onode::empty)
        release(onode.data[i]);
      onode.data[i] = t_onode::empty;
      i++;
    }
    store.debug_punch_hole(coll, onode.onode, off, len);
  }

  void write(t_onode &onode, uint32_t off, uint32_t len) {
    ceph_assert((off % au_size) == 0);
    ceph_assert((len % au_size) == 0);
    punch_hole(onode, off, len);

    uint32_t i = off / au_size;
    uint32_t end = (off + len) / au_size;
    fillup(onode, end);

    uint32_t au_idx = allocate(end - i);
    uint32_t idx = au_idx;
    while (i < end) {
      onode.data[i] = idx;
      disk_ref(idx);
      ++idx;
      ++i;
    }

    // below simulation of write performed by BlueStore::do_write()
    auto helper_blob_write =
        [&](uint32_t log_off,   // logical offset of blob to put to onode
            uint32_t empty_aus, // amount of unreferenced aus in the beginning
            uint32_t first_au,  // first au that will be referenced
            uint32_t num_aus // number of aus, first, first+1.. first+num_au-1
        ) {
          uint32_t blob_length = (empty_aus + num_aus) * au_size;
          BlueStore::BlobRef b(coll->new_blob());
          bluestore_blob_t &bb = b->dirty_blob();
          bb.init_csum(Checksummer::CSUM_CRC32C, csum_order, blob_length);
          for (size_t i = 0; i < num_aus; ++i) {
            bb.set_csum_item(empty_aus + i, disk[first_au + i].chksum);
          }

          PExtentVector pextents;
          pextents.emplace_back(first_au * au_size, num_aus * au_size);
          bb.allocated(empty_aus * au_size, num_aus * au_size, pextents);

          auto *ext = new BlueStore::Extent(log_off, empty_aus * au_size,
                                            num_aus * au_size, b);
          onode.onode->extent_map.extent_map.insert(*ext);
          b->get_ref(coll.get(), empty_aus * au_size, num_aus * au_size);
          bb.mark_used(empty_aus * au_size, num_aus * au_size);
        };

    size_t off_blob_aligned = p2align(off, blob_size);
    size_t off_blob_roundup = p2align(off + blob_size, blob_size);
    uint32_t skip_aus = (off - off_blob_aligned) / au_size;
    size_t l = std::min<size_t>(off_blob_roundup - off, len);
    uint32_t num_aus = l / au_size;

    while (len > 0) {
      helper_blob_write(off, skip_aus, au_idx, num_aus);
      skip_aus = 0;
      au_idx += num_aus;
      len -= num_aus * au_size;
      off += (skip_aus + num_aus) * au_size;
      l = std::min<size_t>(blob_size, len);
      num_aus = l / au_size;
    };
  }

  void dup(t_onode &ofrom, t_onode &oto, uint64_t off, uint64_t len) {
    ceph_assert((off % au_size) == 0);
    ceph_assert((len % au_size) == 0);
    punch_hole(oto, off, len);

    uint32_t i = off / au_size;
    uint32_t end = (off + len) / au_size;
    fillup(ofrom, end);
    ceph_assert(end <= ofrom.data.size());
    while (i < end) {
      oto.data[i] = ofrom.data[i];
      if (oto.data[i] != t_onode::empty) {
        disk_ref(oto.data[i]);
      }
      ++i;
    }
    BlueStore::TransContext txc(store.cct, coll.get(), nullptr, nullptr);
    ofrom.onode->extent_map.dup_esb(&store, &txc, coll, ofrom.onode, oto.onode,
                                    off, len, off);
  }

  int32_t compare(t_onode &onode) {
    BlueStore::ExtentMap::debug_au_vector_t debug =
        onode.onode->extent_map.debug_list_disk_layout();
    size_t pos = 0;
    for (size_t i = 0; i < debug.size(); ++i) {
      if (debug[i].disk_offset == -1ULL) {
        size_t len = debug[i].disk_length;
        size_t l = len / au_size;
        if (pos + l > onode.data.size()) {
          return pos + l;
        }
        while (l > 0) {
          if (onode.data[pos] != t_onode::empty) {
            return pos;
          }
          --l;
          ++pos;
        };
      } else {
        ceph_assert(pos < onode.data.size());
        uint32_t au = onode.data[pos];
        if (debug[i].disk_offset != au * au_size ||
            debug[i].disk_length != au_size ||
            debug[i].chksum != disk[au].chksum) {
          return pos;
        }
        if ((int32_t)debug[i].ref_cnts == -1) {
          if (disk[au].refs != 1) {
            return pos;
          }
        } else {
          if (disk[au].refs != debug[i].ref_cnts) {
            return pos;
          }
        }
        ++pos;
      }
    }
    // remaining aus must be empty
    while (pos < onode.data.size()) {
      if (onode.data[pos] != t_onode::empty) {
        return pos;
      }
      ++pos;
    }
    return -1;
  }

  bool check(t_onode &onode) {
    int32_t res = compare(onode);
    if (res != -1) {
      cout << "Discrepancy at 0x" << std::hex << res * au_size << std::dec
           << std::endl;
      cout << "Simulated: ";
      print(cout, onode);
      cout << std::endl;
      cout << "Onode: " << onode.onode->extent_map.debug_list_disk_layout()
           << std::endl;
      return false;
    }
    return true;
  }
  void print(t_onode &onode) {
    cout << "Simulated: ";
    print(cout, onode);
    cout << std::endl;
    cout << "Onode: " << onode.onode->extent_map.debug_list_disk_layout()
         << std::endl;
  }
};

TEST_F(ExtentMapFixture, walk) {
  std::vector<t_onode> X;
  for (size_t i = 0; i < 100; i++) {
    X.push_back(create());
  }

  for (size_t i = 0; i < 100 - 1; i++) {
    write(X[i], (i + 2) * au_size, 4 * au_size);
    dup(X[i], X[i + 1], (i + 1) * au_size, 8 * au_size);
  }
  for (size_t i = 0; i < 100; i++) {
    ASSERT_EQ(check(X[i]), true);
  }
}

TEST_F(ExtentMapFixture, pyramid) {
  constexpr size_t H = 100;
  std::vector<t_onode> X;
  for (size_t i = 0; i < H; i++) {
    X.push_back(create());
  }
  write(X[0], 0, (H * 2 + 1) * au_size);

  for (size_t i = 0; i < H - 1; i++) {
    dup(X[i], X[i + 1], i * au_size, (H * 2 + 1 - i * 2) * au_size);
  }
  for (size_t i = 0; i < H; i++) {
    ASSERT_EQ(check(X[i]), true);
  }
}

TEST_F(ExtentMapFixture, rain) {
  constexpr size_t H = 100;
  constexpr size_t W = 100;
  std::vector<t_onode> X;
  for (size_t i = 0; i < H; i++) {
    X.push_back(create());
  }
  for (size_t i = 0; i < H - 1; i++) {
    write(X[i], (rand() % W - 1) * au_size, au_size);
    dup(X[i], X[i + 1], 0, W * au_size);
  }
  for (size_t i = 0; i < H; i++) {
    ASSERT_EQ(check(X[i]), true);
  }
}

TEST_F(ExtentMapFixture, pollock) {
  constexpr size_t H = 100;
  constexpr size_t W = 100;
  std::vector<t_onode> X;
  for (size_t i = 0; i < H; i++) {
    X.push_back(create());
  }
  for (size_t i = 0; i < H - 1; i++) {
    size_t w = rand() % (W / 3) + 1;
    size_t l = rand() % (W - w);
    write(X[i], l * au_size, w * au_size);
    w = rand() % (W / 3) + 1;
    l = rand() % (W - w);
    dup(X[i], X[i + 1], l * au_size, w * au_size);
  }
  for (size_t i = 0; i < H; i++) {
    ASSERT_EQ(check(X[i]), true);
  }
}

TEST_F(ExtentMapFixture, carousel) {
  constexpr size_t R = 10;
  constexpr size_t CNT = 300;
  constexpr size_t W = 100;
  std::vector<t_onode> X;
  for (size_t i = 0; i < R; i++) {
    X.push_back(create());
  }
  for (size_t i = 0; i < CNT; i++) {
    size_t w = rand() % (W / 3) + 1;
    size_t l = rand() % (W - w);
    write(X[i % R], l * au_size, w * au_size);
    w = rand() % (W / 3) + 1;
    l = rand() % (W - w);
    dup(X[i % R], X[(i + 1) % R], l * au_size, w * au_size);
  }
  for (size_t i = 0; i < R; i++) {
    ASSERT_EQ(check(X[i]), true);
  }
}

TEST_F(ExtentMapFixture, petri) {
  constexpr size_t R = 10;
  constexpr size_t CNT = 300;
  constexpr size_t W = 100;
  std::vector<t_onode> X;
  for (size_t i = 0; i < R; i++) {
    X.push_back(create());
    write(X[i], 0 * au_size, W * au_size);
  }
  for (size_t i = 0; i < CNT; i++) {
    size_t from = rand() % R;
    size_t to = from;
    while (to == from) {
      to = rand() % R;
    }
    size_t w = rand() % (W / 5) + 1;
    size_t l = rand() % (W - w);
    dup(X[from], X[to], l * au_size, w * au_size);
  }
  for (size_t i = 0; i < R; i++) {
    ASSERT_EQ(check(X[i]), true);
  }
}

TEST_P(PunchHoleFixture, selftest)
{
  populate({1000, 32000}, {11000, 9000});
  PExtentVector released;
  std::vector<BlueStore::BlobRef> pruned_blobs;
  std::set<BlueStore::SharedBlobRef> shared_changed;
  BlueStore::volatile_statfs statfs_delta;
  store->debug_punch_hole_2(coll, onode, 1000, 32000,
    released, pruned_blobs, shared_changed, statfs_delta);
  clear();
}

TEST_P(PunchHoleFixture, all)
{
  for (int i = 0; i < 1000; i++) {
    onode = new BlueStore::Onode(coll.get(), ghobject_t(), "");

    uint32_t start = (rand() % 30000) + 1;
    uint32_t end = start + (rand() % 100000) + 1;
    uint32_t blob_length;
    uint32_t pos = start;
    while (pos < end) {
      blob_length = (rand() % 30000) + 1;
      if (pos + blob_length > end) {
        blob_length = end - pos;
      }
      populate({pos, blob_length}, {pos, blob_length});
      pos = pos + blob_length;
    }
    PExtentVector released;
    std::vector<BlueStore::BlobRef> pruned_blobs;
    std::set<BlueStore::SharedBlobRef> shared_changed;
    BlueStore::volatile_statfs statfs_delta;
    store->debug_punch_hole_2(coll, onode, start, end - start,
                         released, pruned_blobs, shared_changed, statfs_delta);
    EXPECT_EQ(to_iset(released), disk_to_free);
    EXPECT_EQ(to_set(pruned_blobs), blobs_to_free);
    EXPECT_EQ(statfs_delta, statfs_to_free);
    clear();
  }
}

TEST_P(PunchHoleFixture, some)
{
  for (int i = 0; i < 1000; i++) {
    onode = new BlueStore::Onode(coll.get(), ghobject_t(), "");

    uint32_t start = (rand() % 30000) + 1;
    uint32_t hole_start = start + (rand() % 30000);
    uint32_t hole_end = hole_start + (rand() % 100000) + 1;
    uint32_t end = hole_end  + (rand() % 30000);
    uint32_t blob_length;
    uint32_t pos = start;
    while (pos < end) {
      blob_length = (rand() % 30000) + 1;
      if (pos + blob_length > end) {
        blob_length = end - pos;
      }
      uint32_t a = hole_start;
      uint32_t b = hole_end;
      if (a < pos) a = pos;
      if (b > pos + blob_length) b = pos + blob_length;
      if (a < b)
        populate({pos, blob_length}, {a, b - a});
      else
        populate({pos, blob_length}, {});
      pos = pos + blob_length;
    }
    PExtentVector released;
    std::vector<BlueStore::BlobRef> pruned_blobs;
    std::set<BlueStore::SharedBlobRef> shared_changed;
    BlueStore::volatile_statfs statfs_delta;
    store->debug_punch_hole_2(coll, onode, hole_start, hole_end - hole_start,
                         released, pruned_blobs, shared_changed, statfs_delta);
    EXPECT_EQ(to_iset(released), disk_to_free);
    EXPECT_EQ(to_set(pruned_blobs), blobs_to_free);
    EXPECT_EQ(statfs_delta, statfs_to_free);
    clear();
  }
}

TEST_P(PunchHoleFixture, multipunch)
{
  std::vector param = GetParam();
  ceph_assert(param.size() >= 7);
  //param[0] for au_size
  uint32_t object_size_low = param[3];
  uint32_t object_size_high = param[4];
  uint32_t blob_size_low = param[5];
  uint32_t blob_size_high = param[6];

  for (int i = 0; i < 1000; i++) {
    onode = new BlueStore::Onode(coll.get(), ghobject_t(), "");
    uint32_t step = object_size_high - object_size_low / 4;
    uint32_t start =      rma(30000);
    uint32_t hole_start = rma(start, start + step);
    uint32_t hole_end;
    do {
      hole_end = rma(hole_start, hole_start + step * 2 + 1);
    } while (hole_end == hole_start);
    uint32_t end =        rma(hole_end, hole_end + step);
    uint32_t blob_length;
    uint32_t pos = start;
    while (pos < end) {
      blob_length = rma(blob_size_low, blob_size_high);
      if (pos + blob_length > end) {
        blob_length = end - pos;
      }
      uint32_t a = hole_start;
      uint32_t b = hole_end;
      if (a < pos) a = pos;
      if (b > pos + blob_length) b = pos + blob_length;
      if (a < b)
        populate({pos, blob_length}, {a, b - a});
      else
        populate({pos, blob_length}, {});
      pos = pos + blob_length;
    }
    PExtentVector released;
    std::vector<BlueStore::BlobRef> pruned_blobs;
    std::set<BlueStore::SharedBlobRef> shared_changed;
    BlueStore::volatile_statfs statfs_delta;

    for (int j = 0; j < 10; j++) {
      uint32_t s = rand() % ((hole_end - hole_start) / 5 + 1);
      uint32_t p = rand() % (hole_end - hole_start - s) + hole_start;
      store->debug_punch_hole_2(
        coll, onode, p, s,
        released, pruned_blobs, shared_changed, statfs_delta);
    }
    // and mandatory full clear at the end
    store->debug_punch_hole_2(
      coll, onode, hole_start, hole_end - hole_start,
      released, pruned_blobs, shared_changed, statfs_delta);
    EXPECT_EQ(to_iset(released), disk_to_free);
    EXPECT_EQ(to_set(pruned_blobs), blobs_to_free);
    EXPECT_EQ(statfs_delta, statfs_to_free);
    clear();
  }
}

//0 = au_size, 1/2 = %is_aligned, 3-4 = min-max object
//5-6 = min-max blob, 7/8 = %is_compressed, 9-10 = min-max %compressed
INSTANTIATE_TEST_SUITE_P(
  BlueStore,
  PunchHoleFixture,
  ::testing::Values(
    std::vector<int>({4096, 2, 7, 10000, 100000, 20000, 40000}),
    std::vector<int>({4096, 11, 13, 30000, 300000, 65536, 65536}),
    std::vector<int>({8192, 3, 4, 20000, 150000, 10000, 25000}),
    std::vector<int>({32768, 3, 4, 40000, 400000, 65536, 65536}),
    std::vector<int>({4096, 2, 7, 10000, 100000, 20000, 40000, 1, 2, 10, 50}),
    std::vector<int>({4096, 11, 13, 30000, 300000, 65536, 65536, 2, 3, 20, 70}),
    std::vector<int>({8192, 3, 4, 20000, 150000, 10000, 25000, 2, 3 ,10, 50}),
    std::vector<int>({32768, 3, 4, 40000, 400000, 65536, 65536, 1, 2, 20, 70}),
    std::vector<int>({4096, 2, 7, 10000, 100000, 20000, 40000, 1, 2, 10, 50, 2, 3}),
    std::vector<int>({4096, 11, 13, 30000, 300000, 65536, 65536, 2, 3, 20, 70, 1, 5}),
    std::vector<int>({8192, 3, 4, 20000, 150000, 10000, 25000, 2, 3 ,10, 50, 5, 7}),
    std::vector<int>({32768, 3, 4, 40000, 400000, 65536, 65536, 1, 2, 20, 70, 1, 3})
    )
);



class BlueStoreWriteFixture : public BlueStoreFixture
{
public:
  uint32_t block_size;
  uint32_t blob_size;
  uint32_t checksum_type;
  uint32_t checksum_order;
  uint32_t size_range;

  void SetUp() override {
    std::vector param = GetParam();
    BlueStoreFixture::SetUp(); //uses param[0]
    block_size = param[1];
    blob_size = param[2];
    checksum_type = param[3];
    checksum_order = param[4];
    size_range = param[5];
  }
  // 0 = au_size, 1 = block_size, 2 = blob size
  // 3 = checksum type, 4 = checksum order
  // 5 = size range
  uint32_t get_offset(uint32_t value) {
    switch (rand() % 3) {
    case 0:
      value = p2align<uint32_t>(value, au_size);
      break;
    case 1:
      value = p2align<uint32_t>(value, block_size);
      break;
    case 2:
      ;
    }
    return value;
  }
  uint32_t get_length(uint32_t value) {
    switch (rand() % 3) {
    case 0:
      value = p2roundup<uint32_t>(value, au_size);
      break;
    case 1:
      value = p2roundup<uint32_t>(value, block_size);
      break;
    case 2:
      ;
    }
    return value;
  }
};

TEST_P(BlueStoreWriteFixture, expand_lr)
{
  struct print_writer : BlueStore::Writer::write_divertor {
    ~print_writer() {};
    void write(
      uint64_t disk_offset,
      const bufferlist& data,
      bool deferred) override {
    }
  };
  struct zero_reader: BlueStore::Writer::read_divertor {
    ~zero_reader() {};
    uint32_t read_cnt = 0;
    bufferlist read(uint32_t offset, uint32_t length) override {
      ++read_cnt;
      bufferlist tmp;
      tmp.append_zero(length);
      return tmp;
    }
  };
  store->debug_set_block_size(block_size);
  uint64_t disk_size = (uint64_t)1024 * 1024 * 1024 * 1024;
  store->debug_get_alloc() = Allocator::create(g_ceph_context, "avl", disk_size, au_size);
  store->debug_get_alloc()->init_add_free(0, disk_size);

  for (int i = 0; i < 1000; i++) {
    BlueStore::TransContext txc(g_ceph_context, coll.get(), nullptr, nullptr);
    BlueStore::WriteContext wctx;
    wctx.csum_type = checksum_type;
    wctx.csum_order = checksum_order;
    BlueStore::Onode* o = new BlueStore::Onode(coll.get(), ghobject_t(), "");
    BlueStore::Writer w(store, &txc, &wctx, o);
    print_writer pw;
    zero_reader zr;
    w.test_write_divertor = &pw;
    w.test_read_divertor = &zr;
    wctx.target_blob_size = blob_size;

    // step 1: select chsum, order
    // step 2: write object
    // step 3:
    uint32_t primary_offset = get_offset(rand() % size_range);
    uint32_t primary_length = get_length((rand() % size_range) + 1);
    bufferlist primary_data;
    primary_data.append(std::string(primary_length, 'a'));
    uint32_t primary_end = primary_offset + primary_length;
    w.do_write(primary_offset, primary_data);

    uint32_t secondary_offset = get_offset(rand() % size_range);
    uint32_t secondary_length = get_length((rand() % size_range) + 1);
    bufferlist secondary_data;
    secondary_data.append(std::string(secondary_length, 'a'));
    uint32_t secondary_end = secondary_offset + secondary_length;

    uint32_t min_read = 0;
    if (primary_offset <= secondary_offset && secondary_offset < primary_end &&
        p2phase(secondary_offset, block_size) != 0) {
      min_read++;
    }
    if (primary_offset <= secondary_end && secondary_end < primary_end &&
        p2phase(secondary_end, block_size) != 0) {
      min_read++;
    }
    w.do_write(secondary_offset, secondary_data);
    o->extent_map.clear();
    ASSERT_LE(min_read, zr.read_cnt);
  }
}

TEST_P(BlueStoreWriteFixture, buffer_check)
{
  char* ref_data = nullptr;
  struct print_writer : BlueStore::Writer::write_divertor {
    ~print_writer() {};
    void write(
      uint64_t disk_offset,
      const bufferlist& data,
      bool deferred) override {
    }
  };
  struct ref_reader: BlueStore::Writer::read_divertor {
    ~ref_reader() {};
    uint32_t read_cnt = 0;
    char* ref_data = nullptr;
    bufferlist read(uint32_t offset, uint32_t length) override {
      ++read_cnt;
      bufferlist tmp;
      tmp.append(std::string(ref_data + offset, length));
      return tmp;
    }
  };
  store->debug_set_block_size(block_size);
  uint64_t disk_size = (uint64_t)1024 * 1024 * 1024 * 1024;
  store->debug_get_alloc() = Allocator::create(g_ceph_context, "avl", disk_size, au_size);
  store->debug_get_alloc()->init_add_free(0, disk_size);

  for (int i = 0; i < 1000; i++) {
    BlueStore::TransContext txc(g_ceph_context, coll.get(), nullptr, nullptr);
    BlueStore::WriteContext wctx;
    wctx.csum_type = checksum_type;
    wctx.csum_order = checksum_order;
    BlueStore::Onode* o = new BlueStore::Onode(coll.get(), ghobject_t(), "");
    BlueStore::Writer w(store, &txc, &wctx, o);
    print_writer pw;
    ref_reader zr;
    w.test_write_divertor = &pw;
    w.test_read_divertor = &zr;
    wctx.target_blob_size = blob_size;

    ref_data = (char *)malloc(size_range * 3);
    zr.ref_data = ref_data;
    uint8_t ref_none = 0;
    memset(ref_data, ref_none, size_range * 3);

    int write_cnt = (rand() % 5) + 1;
    for (int j = 0; j < write_cnt; j++) {
      uint32_t offset = get_offset(rand() % size_range);
      uint32_t length = get_length((rand() % size_range) + 1);
      bufferlist data;
      uint8_t data_val = rand() % 256;
      data.append(std::string(length, data_val));
      memcpy(ref_data + offset, data.c_str(), length);
      w.do_write(offset, data);
    }

    bool equal = true;
    auto check_buffer = [&](uint32_t offset, const bufferlist data) {
      lsubdout(g_ceph_context, bluestore, 20)
          << std::hex << "CHECK AT 0x" << offset << "~" << data.length() << dendl;
      bufferlist ref;
      ref.append(std::string(ref_data + offset, data.length()));
      if (ref.to_str() != data.to_str()) {
        equal = false;
      }
    };
    w.debug_iterate_buffers(check_buffer);
    ASSERT_TRUE(equal);

    free(ref_data);
    o->extent_map.clear();
  }
}

TEST_P(BlueStoreWriteFixture, deferred_check)
{
  struct check_writer : BlueStore::Writer::write_divertor {
    ~check_writer() {};
    interval_set<uint64_t> already_written;
    uint64_t bad_direct = 0;
    uint64_t needless_deferred = 0;
    void write(
      uint64_t disk_offset,
      const bufferlist& data,
      bool deferred) override {
        lsubdout(g_ceph_context, bluestore, 20)
              << std::hex << "write: 0x"
              << disk_offset << "~" << data.length() << std::dec
              << (deferred ? " deferred" : " direct") << dendl;
      if (!deferred) {
        interval_set<uint64_t> res;
        res.insert(disk_offset, data.length());
        res.intersection_of(already_written);
        if (!res.empty()) {
          for (auto& r : res) {
            lsubdout(g_ceph_context, bluestore, 10)
              << std::hex << "direct on used: 0x"
              << r.first << "~" << r.second << std::dec << dendl;
            bad_direct += r.second;
          }
        }
        already_written.union_insert(disk_offset, data.length());
      } else {
        interval_set<uint64_t> res, a;
        a.insert(disk_offset, data.length());
        res = a;
        a.intersection_of(already_written);
        res.subtract(a);
        if (!res.empty()) {
          //it is warning, not error to write deferred to unused space
          for (auto& r : res) {
            lsubdout(g_ceph_context, bluestore, 10)
              << std::hex << "deferred on not used: 0x"
              << r.first << "~" << r.second << std::dec << dendl;
            needless_deferred = r.second;
          }
        }
        already_written.union_insert(disk_offset, data.length());
      }
    }
  };
  struct ref_reader: BlueStore::Writer::read_divertor {
    ~ref_reader() {};
    bufferlist read(uint32_t offset, uint32_t length) override {
      bufferlist tmp;
      tmp.append_zero(length);
      return tmp;
    }
  };
  store->debug_set_block_size(block_size);
  uint64_t disk_size = (uint64_t)1024 * 1024 * 1024 * 1024;
  store->debug_get_alloc() = Allocator::create(g_ceph_context, "avl", disk_size, au_size);
  store->debug_get_alloc()->init_add_free(0, disk_size);
  store->debug_set_prefer_deferred_size(65536);
  uint64_t needless_deferred = 0;
  uint64_t needless_deferred_cnt = 0;
  for (int i = 0; i < 1000; i++) {
    BlueStore::TransContext txc(g_ceph_context, coll.get(), nullptr, nullptr);
    BlueStore::WriteContext wctx;
    wctx.csum_type = checksum_type;
    wctx.csum_order = checksum_order;
    BlueStore::Onode* o = new BlueStore::Onode(coll.get(), ghobject_t(), "");
    BlueStore::Writer w(store, &txc, &wctx, o);
    check_writer pw;
    ref_reader zr;
    w.test_write_divertor = &pw;
    w.test_read_divertor = &zr;
    wctx.target_blob_size = blob_size;

    int write_cnt = (rand() % 5) + 1;
    for (int j = 0; j < write_cnt; j++) {
      uint32_t offset = get_offset(rand() % size_range);
      uint32_t length = get_length((rand() % size_range) + 1);
      bufferlist data;
      data.append(std::string(length, 0));
      w.do_write(offset, data);
      ASSERT_EQ(pw.bad_direct, 0);
    }
    o->extent_map.clear();
    if (pw.needless_deferred != 0) {
      needless_deferred += pw.needless_deferred;
      needless_deferred_cnt++;
    }
  }
  if (needless_deferred != 0) {
    std::cout << "note! " << needless_deferred_cnt
      << " deferred events over never-used regions for total 0x"
      << std::hex << needless_deferred << std::dec << " bytes" << std::endl;
  }
}

TEST_P(BlueStoreWriteFixture, statfs_zero)
{
  struct check_writer : BlueStore::Writer::write_divertor {
    ~check_writer() {};
    void write(
      uint64_t disk_offset,
      const bufferlist& data,
      bool deferred) override {
    }
  };
  struct ref_reader: BlueStore::Writer::read_divertor {
    ~ref_reader() {};
    bufferlist read(uint32_t offset, uint32_t length) override {
      bufferlist tmp;
      tmp.append_zero(length);
      return tmp;
    }
  };
  store->debug_set_block_size(block_size);
  uint64_t disk_size = (uint64_t)1024 * 1024 * 1024 * 1024;
  store->debug_get_alloc() = Allocator::create(g_ceph_context, "avl", disk_size, au_size);
  store->debug_get_alloc()->init_add_free(0, disk_size);
  for (int i = 0; i < 1000; i++) {
    BlueStore::TransContext txc(g_ceph_context, coll.get(), nullptr, nullptr);
    BlueStore::WriteContext wctx;
    wctx.csum_type = checksum_type;
    wctx.csum_order = checksum_order;
    BlueStore::OnodeRef o = new BlueStore::Onode(coll.get(), ghobject_t(), "");
    BlueStore::Writer w(store, &txc, &wctx, o);
    check_writer pw;
    ref_reader zr;
    w.test_write_divertor = &pw;
    w.test_read_divertor = &zr;
    wctx.target_blob_size = blob_size;

    BlueStore::volatile_statfs statfs_delta;
    int write_cnt = (rand() % 10) + 1;
    for (int j = 0; j < write_cnt; j++) {
      uint32_t offset = get_offset(rand() % size_range);
      uint32_t length = get_length((rand() % size_range) + 1);
      bufferlist data;
      data.append(std::string(length, 0));
      w.do_write(offset, data);
    }
    PExtentVector released;
    std::vector<BlueStore::BlobRef> pruned_blobs;
    std::set<BlueStore::SharedBlobRef> shared_changed;

    store->debug_punch_hole_2(coll, o, 0, size_range * 3,
      released, pruned_blobs, shared_changed,
      w.statfs_delta);
    ASSERT_EQ(w.statfs_delta.allocated(), 0);
    ASSERT_EQ(w.statfs_delta.stored(), 0);
    o->extent_map.clear();
  }
}


// 0 = au_size, 1 = block_size, 2 = blob size
// 3 = checksum type, 4 = checksum order
// 5 = size range
INSTANTIATE_TEST_SUITE_P(
  BlueStore,
  BlueStoreWriteFixture,
  ::testing::Values(
    std::vector<int>({4096, 4096, 64 * 1024, Checksummer::CSUM_CRC32C, 12, 100000}),
    std::vector<int>({4096, 4096, 64 * 1024, Checksummer::CSUM_CRC32C, 12, 200000}),
    std::vector<int>({4096, 4096, 64 * 1024, Checksummer::CSUM_NONE, 12, 100000}),
    std::vector<int>({4096, 4096, 64 * 1024, Checksummer::CSUM_CRC32C, 10, 100000}),
    std::vector<int>({4 * 4096, 4096, 64 * 1024, Checksummer::CSUM_CRC32C, 12, 100000}),
    std::vector<int>({16 * 4096, 4096, 128 * 1024, Checksummer::CSUM_CRC32C, 12, 100000}),
    std::vector<int>({16 * 4096, 4096, 128 * 1024, Checksummer::CSUM_CRC32C, 14, 100000}),
    std::vector<int>({4096, 4096, 64 * 1024, Checksummer::CSUM_CRC32C, 14, 100000}),
    std::vector<int>({4 * 4096, 4096, 64 * 1024, Checksummer::CSUM_CRC32C, 12, 200000}),
    std::vector<int>({16 * 4096, 4096, 128 * 1024, Checksummer::CSUM_CRC32C, 12, 300000}),
    std::vector<int>({16 * 4096, 4096, 128 * 1024, Checksummer::CSUM_CRC32C, 14, 200000}),
    std::vector<int>({16 * 4096, 4096, 128 * 1024, Checksummer::CSUM_NONE, 12, 200000}),
    std::vector<int>({4096, 4096, 64 * 1024, Checksummer::CSUM_CRC32C, 14, 250000}),
    std::vector<int>({4096, 4096, 64 * 1024, Checksummer::CSUM_NONE, 12, 250000})
    )
);


TEST(ExtentMap, dup_extent_map)
{
  BlueStore store(g_ceph_context, "", 4096);
  BlueStore::OnodeCacheShard *oc =
      BlueStore::OnodeCacheShard::create(g_ceph_context, "lru", NULL);
  BlueStore::BufferCacheShard *bc =
      BlueStore::BufferCacheShard::create(&store, "lru", NULL);

  size_t csum_order = 12; // 1^12 = 4096 bytes
  auto coll = ceph::make_ref<BlueStore::Collection>(&store, oc, bc, coll_t());
  std::unique_ptr<ceph::Formatter> formatter(Formatter::create("json"));

  ///////////////////////////
  // constructing onode1
  BlueStore::OnodeRef onode1(
      new BlueStore::Onode(coll.get(), ghobject_t(), ""));

  // BlueStore::ExtentMap em1(&onode1,
  //   g_ceph_context->_conf->bluestore_extent_map_inline_shard_prealloc_size);
  BlueStore::ExtentMap &em1 = onode1->extent_map;
  ///////////////////////////
  // constructing extent/Blob: 0x0~2000 at <0x100000~2000>
  size_t ext1_offs = 0x0;
  size_t ext1_len = 0x2000;
  size_t ext1_boffs = 0x0;
  BlueStore::BlobRef b1 = coll->new_blob();
  auto &_b1 = b1->dirty_blob();
  _b1.init_csum(Checksummer::CSUM_CRC32C, csum_order, ext1_len);
  for (size_t i = 0; i < _b1.get_csum_count(); i++) {
    *(_b1.get_csum_item_ptr(i)) = i + 1;
  }
  PExtentVector pextents;
  pextents.emplace_back(0x100000, ext1_len);
  _b1.allocated(0, ext1_len, pextents);

  auto *ext1 = new BlueStore::Extent(ext1_offs, ext1_boffs, ext1_len, b1);
  em1.extent_map.insert(*ext1);
  b1->get_ref(coll.get(), ext1->blob_offset, ext1->length);
  _b1.mark_used(ext1->blob_offset, ext1->length);

  ///////////////////////////
  // constructing onode2 which is a full clone from onode1
  BlueStore::OnodeRef onode2(
      new BlueStore::Onode(coll.get(), ghobject_t(), ""));
  // BlueStore::ExtentMap em2(&onode2,
  //   g_ceph_context->_conf->bluestore_extent_map_inline_shard_prealloc_size);
  BlueStore::ExtentMap &em2 = onode2->extent_map;
  {
    BlueStore::TransContext txc(store.cct, coll.get(), nullptr, nullptr);

    // em1.dup(&store, &txc, coll, em2, ext1_offs, ext1_len, ext1_offs);
    onode1->extent_map.dup_esb(&store, &txc, coll, onode1, onode2, ext1_offs,
                               ext1_len, ext1_offs);

    em1.dump(formatter.get()); // see the log if any
    formatter->flush(std::cout);
    std::cout << std::endl;
    em2.dump(formatter.get());
    formatter->flush(std::cout);
    std::cout << std::endl;

    ASSERT_TRUE(b1->get_blob().is_shared());
    ASSERT_EQ(b1->get_referenced_bytes(), ext1_len);

    BlueStore::BlobRef b2 = em2.seek_lextent(ext1_offs)->blob;
    ASSERT_TRUE(b2->get_blob().is_shared());
    ASSERT_EQ(b2->get_referenced_bytes(), ext1_len);
    ASSERT_EQ(b1->get_shared_blob(), b2->get_shared_blob());
    auto &_b2 = b2->get_blob();
    ASSERT_EQ(_b1.get_csum_count(), _b2.get_csum_count());
    for (size_t i = 0; i < _b2.get_csum_count(); i++) {
      ASSERT_EQ(*(_b1.get_csum_item_ptr(i)), *(_b2.get_csum_item_ptr(i)));
    }
  }

  ///////////////////////////
  // constructing onode3 which is partial clone (tail part) from onode2
  BlueStore::OnodeRef onode3(
      new BlueStore::Onode(coll.get(), ghobject_t(), ""));
  // BlueStore::ExtentMap em3(&onode3,
  //   g_ceph_context->_conf->bluestore_extent_map_inline_shard_prealloc_size);
  BlueStore::ExtentMap &em3 = onode3->extent_map;
  {
    size_t clone_shift = 0x1000;
    ceph_assert(ext1_len > clone_shift);
    size_t clone_offs = ext1_offs + clone_shift;
    size_t clone_len = ext1_len - clone_shift;
    BlueStore::TransContext txc(store.cct, coll.get(), nullptr, nullptr);

    onode1->extent_map.dup_esb(&store, &txc, coll, onode1, onode3, clone_offs,
                               clone_len, clone_offs);
    em1.dump(formatter.get()); // see the log if any
    formatter->flush(std::cout);
    std::cout << std::endl;
    em3.dump(formatter.get());
    formatter->flush(std::cout);
    std::cout << std::endl;

    // make sure (basically) onode1&2 are unmodified
    BlueStore::BlobRef b1 = em1.seek_lextent(ext1_offs)->blob;
    BlueStore::BlobRef b2 = em2.seek_lextent(ext1_offs)->blob;
    ASSERT_EQ(b1->get_shared_blob(), b2->get_shared_blob());

    BlueStore::Extent &ext3 = *em3.seek_lextent(clone_offs);
    ASSERT_EQ(ext3.blob_offset, clone_shift);
    ASSERT_EQ(ext3.length, clone_len);
    BlueStore::BlobRef b3 = ext3.blob;
    ASSERT_TRUE(b3->get_blob().is_shared());
    ASSERT_EQ(b3->get_shared_blob(), b1->get_shared_blob());
    ASSERT_EQ(b3->get_referenced_bytes(), clone_len);
    auto ll = b3->get_blob().get_logical_length();
    ASSERT_EQ(ll, ext1_len);
    auto &_b3 = b3->get_blob();
    ASSERT_EQ(_b1.get_csum_count(), _b3.get_csum_count());
    for (size_t i = 0; i < _b3.get_csum_count(); i++) {
      ASSERT_EQ(*(_b1.get_csum_item_ptr(i)), *(_b3.get_csum_item_ptr(i)));
    }
  }

  ///////////////////////////
  // constructing onode4 which is partial clone (head part) from onode2
  BlueStore::OnodeRef onode4(
      new BlueStore::Onode(coll.get(), ghobject_t(), ""));
  // BlueStore::ExtentMap em4(&onode4,
  //   g_ceph_context->_conf->bluestore_extent_map_inline_shard_prealloc_size);
  BlueStore::ExtentMap &em4 = onode4->extent_map;

  {
    size_t clone_shift = 0;
    size_t clone_len = 0x1000;
    ceph_assert(ext1_len >= clone_shift + clone_len);
    size_t clone_offs = ext1_offs + clone_shift;
    BlueStore::TransContext txc(store.cct, coll.get(), nullptr, nullptr);

    onode2->extent_map.dup_esb(&store, &txc, coll, onode2, onode4, clone_offs,
                               clone_len, clone_offs);
    em2.dump(formatter.get()); // see the log if any
    formatter->flush(std::cout);
    std::cout << std::endl;
    em4.dump(formatter.get());
    formatter->flush(std::cout);
    std::cout << std::endl;

    // make sure (basically) onode1&2 are unmodified
    BlueStore::BlobRef b1 = em1.seek_lextent(ext1_offs)->blob;
    BlueStore::BlobRef b2 = em2.seek_lextent(ext1_offs)->blob;
    BlueStore::BlobRef b3 = em3.seek_lextent(ext1_offs)->blob;
    ASSERT_EQ(b1->get_shared_blob(), b2->get_shared_blob());
    ASSERT_EQ(b1->get_shared_blob(), b3->get_shared_blob());
    auto &_b2 = b2->get_blob();

    BlueStore::Extent &ext4 = *em4.seek_lextent(clone_offs);
    ASSERT_EQ(ext4.blob_offset, clone_shift);
    ASSERT_EQ(ext4.length, clone_len);
    BlueStore::BlobRef b4 = ext4.blob;
    ASSERT_TRUE(b4->get_blob().is_shared());
    ASSERT_EQ(b4->get_shared_blob(), b2->get_shared_blob());
    ASSERT_EQ(b4->get_referenced_bytes(), clone_len);
    auto &_b4 = b4->get_blob();
    auto ll = _b4.get_logical_length();
    auto csum_entries = ll / (1 << csum_order);
    ASSERT_EQ(ll, clone_len);
    ASSERT_EQ(csum_entries, _b4.get_csum_count());

    ASSERT_GT(_b2.get_csum_count(), csum_entries);
    for (size_t i = 0; i < csum_entries; i++) {
      ASSERT_EQ(*(_b2.get_csum_item_ptr(i)), *(_b4.get_csum_item_ptr(i)));
    }
  }
}

void clear_and_dispose(BlueStore::old_extent_map_t &old_em) {
  auto oep = old_em.begin();
  while (oep != old_em.end()) {
    auto &lo = *oep;
    oep = old_em.erase(oep);
    delete &lo;
  }
}

TEST(GarbageCollector, BasicTest) {
  BlueStore store(g_ceph_context, "", 4096);
  BlueStore::OnodeCacheShard *oc =
      BlueStore::OnodeCacheShard::create(g_ceph_context, "lru", NULL);
  BlueStore::BufferCacheShard *bc =
      BlueStore::BufferCacheShard::create(&store, "lru", NULL);

  auto coll = ceph::make_ref<BlueStore::Collection>(&store, oc, bc, coll_t());
  BlueStore::Onode onode(coll.get(), ghobject_t(), "");
  BlueStore::ExtentMap em(
      &onode,
      g_ceph_context->_conf->bluestore_extent_map_inline_shard_prealloc_size);

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
    BlueStore::BlobRef b1(coll->new_blob());
    BlueStore::BlobRef b2(coll->new_blob());
    BlueStore::BlobRef b3(coll->new_blob());
    BlueStore::BlobRef b4(coll->new_blob());
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
    auto &to_collect = gc.get_extents_to_collect();
    ASSERT_EQ(to_collect.num_intervals(), 1u);
    {
      auto it = to_collect.begin();
      using p = decltype(*it);
      auto v = p{100ul, 10ul};
      ASSERT_EQ(*it, v);
    }
    em.clear();
    clear_and_dispose(old_extents);
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
    auto coll = ceph::make_ref<BlueStore::Collection>(&store, oc, bc, coll_t());
    BlueStore::Onode onode(coll.get(), ghobject_t(), "");
    BlueStore::ExtentMap em(
        &onode,
        g_ceph_context->_conf->bluestore_extent_map_inline_shard_prealloc_size);

    BlueStore::old_extent_map_t old_extents;
    BlueStore::GarbageCollector gc(g_ceph_context);
    int64_t saving;
    BlueStore::BlobRef b1(coll->new_blob());
    BlueStore::BlobRef b2(coll->new_blob());
    BlueStore::BlobRef b3(coll->new_blob());
    BlueStore::BlobRef b4(coll->new_blob());
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

    old_extents.push_back(
        *new BlueStore::OldExtent(0x8000, 0x8000, 0x8000, b1));
    old_extents.push_back(
        *new BlueStore::OldExtent(0x10000, 0x10000, 0x20000, b1));
    old_extents.push_back(
        *new BlueStore::OldExtent(0x30000, 0x30000, 0xf000, b1));

    saving = gc.estimate(0x30000, 0xf000, em, old_extents, 0x10000);
    ASSERT_EQ(saving, 2);
    auto &to_collect = gc.get_extents_to_collect();
    ASSERT_EQ(to_collect.num_intervals(), 2u);
    {
      auto it1 = to_collect.begin();
      auto it2 = ++to_collect.begin();
      using p = decltype(*it1);
      {
        auto v1 = p{0x0ul, 0x8000ul};
        auto v2 = p{0x0ul, 0x8000ul};
        ASSERT_TRUE(*it1 == v1 || *it2 == v2);
      }
      {
        auto v1 = p{0x3f000ul, 0x1000ul};
        auto v2 = p{0x3f000ul, 0x1000ul};
        ASSERT_TRUE(*it1 == v1 || *it2 == v2);
      }
    }

    em.clear();
    clear_and_dispose(old_extents);
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
    BlueStore::BlobRef b1(coll->new_blob());
    BlueStore::BlobRef b2(coll->new_blob());
    b1->dirty_blob().set_compressed(0x4000, 0x2000);
    b1->dirty_blob().allocated_test(bluestore_pextent_t(0, 0x2000));
    b2->dirty_blob().set_compressed(0x4000, 0x2000);
    b2->dirty_blob().allocated_test(bluestore_pextent_t(0, 0x2000));

    em.extent_map.insert(*new BlueStore::Extent(0, 0, 0x3000, b1));
    b1->get_ref(coll.get(), 0, 0x3000);
    em.extent_map.insert(
        *new BlueStore::Extent(0x3000, 0, 0x4000, b2)); // new extent
    b2->get_ref(coll.get(), 0, 0x4000);

    old_extents.push_back(
        *new BlueStore::OldExtent(0x3000, 0x3000, 0x1000, b1));

    saving = gc.estimate(0x3000, 0x4000, em, old_extents, 0x1000);
    ASSERT_EQ(saving, 0);
    auto &to_collect = gc.get_extents_to_collect();
    ASSERT_EQ(to_collect.num_intervals(), 0u);
    em.clear();
    clear_and_dispose(old_extents);
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
    auto coll = ceph::make_ref<BlueStore::Collection>(&store, oc, bc, coll_t());
    BlueStore::Onode onode(coll.get(), ghobject_t(), "");
    BlueStore::ExtentMap em(
        &onode,
        g_ceph_context->_conf->bluestore_extent_map_inline_shard_prealloc_size);

    BlueStore::old_extent_map_t old_extents;
    BlueStore::GarbageCollector gc(g_ceph_context);
    int64_t saving;
    BlueStore::BlobRef b0(coll->new_blob());
    BlueStore::BlobRef b1(coll->new_blob());
    BlueStore::BlobRef b2(coll->new_blob());
    BlueStore::BlobRef b3(coll->new_blob());
    BlueStore::BlobRef b4(coll->new_blob());
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

    old_extents.push_back(
        *new BlueStore::OldExtent(0x8000, 0x8000, 0x8000, b0));
    old_extents.push_back(
        *new BlueStore::OldExtent(0x10000, 0x10000, 0x10000, b0));
    old_extents.push_back(
        *new BlueStore::OldExtent(0x20000, 0x00000, 0x1f000, b1));

    saving = gc.estimate(0x30000, 0xf000, em, old_extents, 0x10000);
    ASSERT_EQ(saving, 2);
    auto &to_collect = gc.get_extents_to_collect();
    ASSERT_EQ(to_collect.num_intervals(), 2u);
    {
      auto it1 = to_collect.begin();
      auto it2 = ++to_collect.begin();
      using p = decltype(*it1);
      {
        auto v1 = p{0x0ul, 0x8000ul};
        auto v2 = p{0x0ul, 0x8000ul};
        ASSERT_TRUE(*it1 == v1 || *it2 == v2);
      }
      {
        auto v1 = p{0x3f000ul, 0x1000ul};
        auto v2 = p{0x3f000ul, 0x1000ul};
        ASSERT_TRUE(*it1 == v1 || *it2 == v2);
      }
    }

    em.clear();
    clear_and_dispose(old_extents);
  }
}

TEST(BlueStoreRepairer, StoreSpaceTracker) {
  BlueStoreRepairer::StoreSpaceTracker bmap0;
  bmap0.init((uint64_t)4096 * 1024 * 1024 * 1024, 0x1000);
  ASSERT_EQ(bmap0.granularity, 2 * 1024 * 1024U);
  ASSERT_EQ(bmap0.collections_bfs.size(), 2048u * 1024u);
  ASSERT_EQ(bmap0.objects_bfs.size(), 2048u * 1024u);

  BlueStoreRepairer::StoreSpaceTracker bmap;
  bmap.init(0x2000 * 0x1000 - 1, 0x1000, 512 * 1024);
  ASSERT_EQ(bmap.granularity, 0x1000u);
  ASSERT_EQ(bmap.collections_bfs.size(), 0x2000u);
  ASSERT_EQ(bmap.objects_bfs.size(), 0x2000u);

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
  extents.insert(0, 0x500);
  extents.insert(0x800, 0x100);
  extents.insert(0x1000, 0x1000);
  extents.insert(0xa001, 1);
  extents.insert(0xa0000, 0xff8);

  ASSERT_EQ(3u, bmap.filter_out(extents));
  ASSERT_TRUE(bmap.is_used(cid));
  ASSERT_TRUE(bmap.is_used(hoid));

  BlueStoreRepairer::StoreSpaceTracker bmap2;
  bmap2.init((uint64_t)0x3223b1d1000, 0x10000);
  ASSERT_EQ(0x1a0000u, bmap2.granularity);
  ASSERT_EQ(0x1edae4u, bmap2.collections_bfs.size());
  ASSERT_EQ(0x1edae4u, bmap2.objects_bfs.size());
  bmap2.set_used(0x3223b190000, 0x10000, cid, hoid);
  ASSERT_TRUE(bmap2.is_used(cid, 0x3223b190000));
  ASSERT_TRUE(bmap2.is_used(hoid, 0x3223b190000));
  ASSERT_TRUE(bmap2.is_used(cid, 0x3223b19f000));
  ASSERT_TRUE(bmap2.is_used(hoid, 0x3223b19ffff));
}

TEST(bluestore_blob_t, unused) {
  {
    bluestore_blob_t b;
    uint64_t min_alloc_size = 64 << 10; // 64 kB

    // _do_write_small 0x0~1000
    uint64_t offset = 0x0;
    uint64_t length = 0x1000; // 4kB
    uint64_t suggested_boff = 0;
    PExtentVector extents;
    extents.emplace_back(0x1a560000, min_alloc_size);
    b.allocated(p2align(suggested_boff, min_alloc_size), 0 /*no matter*/,
                extents);
    b.mark_used(offset, length);
    ASSERT_FALSE(b.is_unused(offset, length));

    // _do_write_small 0x2000~1000
    offset = 0x2000;
    length = 0x1000;
    b.add_unused(0, 0x10000);
    ASSERT_TRUE(b.is_unused(offset, length));
    b.mark_used(offset, length);
    ASSERT_FALSE(b.is_unused(offset, length));

    // _do_write_small 0xc000~2000
    offset = 0xc000;
    length = 0x2000;
    ASSERT_TRUE(b.is_unused(offset, length));
    b.mark_used(offset, length);
    ASSERT_FALSE(b.is_unused(offset, length));
  }

  {
    bluestore_blob_t b;
    uint64_t min_alloc_size = 64 << 10; // 64 kB

    // _do_write_small 0x11000~1000
    uint64_t offset = 0x11000;
    uint64_t length = 0x1000; // 4kB
    uint64_t suggested_boff = 0x11000;
    PExtentVector extents;
    extents.emplace_back(0x1a560000, min_alloc_size);
    b.allocated(p2align(suggested_boff, min_alloc_size), 0 /*no matter*/,
                extents);
    b.add_unused(0, offset);
    b.add_unused(offset + length, min_alloc_size * 2 - offset - length);
    b.mark_used(offset, length);
    ASSERT_FALSE(b.is_unused(offset, length));

    // _do_write_small 0x15000~3000
    offset = 0x15000;
    length = 0x3000;
    ASSERT_TRUE(b.is_unused(offset, length));
    b.mark_used(offset, length);
    ASSERT_FALSE(b.is_unused(offset, length));
  }

  {
    // reuse blob
    bluestore_blob_t b;
    uint64_t min_alloc_size = 64 << 10; // 64 kB

    // _do_write_small 0x2a000~1000
    // and 0x1d000~1000
    uint64_t unused_granularity = 0x3000;
    // offsets and lenght below are selected to
    // be aligned with unused_granularity
    uint64_t offset0 = 0x2a000;
    uint64_t offset = 0x1d000;
    uint64_t length = 0x1000; // 4kB
    PExtentVector extents;
    extents.emplace_back(0x410000, min_alloc_size);
    b.allocated(p2align(offset0, min_alloc_size), min_alloc_size, extents);
    b.add_unused(0, min_alloc_size * 3);
    b.mark_used(offset0, length);
    ASSERT_FALSE(b.is_unused(offset0, length));
    ASSERT_TRUE(b.is_unused(offset, length));

    extents.clear();
    extents.emplace_back(0x430000, min_alloc_size);
    b.allocated(p2align(offset, min_alloc_size), min_alloc_size, extents);
    b.mark_used(offset, length);
    ASSERT_FALSE(b.is_unused(offset0, length));
    ASSERT_FALSE(b.is_unused(offset, length));
    ASSERT_FALSE(b.is_unused(offset, unused_granularity));

    ASSERT_TRUE(
        b.is_unused(0, offset / unused_granularity * unused_granularity));
    ASSERT_TRUE(b.is_unused(offset + length, offset0 - offset - length));
    auto end0_aligned = round_up_to(offset0 + length, unused_granularity);
    ASSERT_TRUE(b.is_unused(end0_aligned, min_alloc_size * 3 - end0_aligned));
  }
}
// This UT is primarily intended to show how repair procedure
// causes erroneous write to INVALID_OFFSET which is reported in
// https://tracker.ceph.com/issues/51682
// Basic map_any functionality is tested as well though.
//
TEST(bluestore_blob_t, wrong_map_bl_in_51682) {
  {
    bluestore_blob_t b;
    uint64_t min_alloc_size = 4 << 10; // 64 kB

    b.allocated_test(bluestore_pextent_t(0x17ba000, 4 * min_alloc_size));
    b.allocated_test(bluestore_pextent_t(0x17bf000, 4 * min_alloc_size));
    b.allocated_test(bluestore_pextent_t(bluestore_pextent_t::INVALID_OFFSET,
                                         1 * min_alloc_size));
    b.allocated_test(bluestore_pextent_t(0x153c44d000, 7 * min_alloc_size));

    b.mark_used(0, 0x8000);
    b.mark_used(0x9000, 0x7000);

    string s(0x7000, 'a');
    bufferlist bl;
    bl.append(s);
    const size_t num_expected_entries = 5;
    uint64_t expected[num_expected_entries][2] = {{0x17ba000, 0x4000},
                                                  {0x17bf000, 0x3000},
                                                  {0x17c0000, 0x3000},
                                                  {0xffffffffffffffff, 0x1000},
                                                  {0x153c44d000, 0x3000}};
    size_t expected_pos = 0;
    b.map_bl(0, bl, [&](uint64_t o, bufferlist &bl) {
      ASSERT_EQ(o, expected[expected_pos][0]);
      ASSERT_EQ(bl.length(), expected[expected_pos][1]);
      ++expected_pos;
    });
    // 0x5000 is an improper offset presumably provided when doing a repair
    b.map_bl(0x5000, bl, [&](uint64_t o, bufferlist &bl) {
      ASSERT_EQ(o, expected[expected_pos][0]);
      ASSERT_EQ(bl.length(), expected[expected_pos][1]);
      ++expected_pos;
    });
    ASSERT_EQ(expected_pos, num_expected_entries);
  }
}
class bluestore_blob_t_test :
  public ::testing::Test,
  public ::testing::WithParamInterface<std::vector<int>>
{
};

TEST_P(bluestore_blob_t_test, release_extents)
{
  // how to construct valid release input
  // 1. pre-release area (might be empty)
  // 2. release area (cannot be empty anywhere), likely to continue extents
  // 3. post-release area (might be empty)
  // result:
  // pre-release + empty_region + post-release
  // +
  // release area

  std::vector<int> param = GetParam();
  ASSERT_EQ(param.size(), 8);
  uint32_t alloc_unit =         param[0];
  uint32_t test_region_range =  param[1];
  uint32_t test_is_empty_nom =  param[2];
  uint32_t test_is_empty_denom = param[3];
  uint32_t test_pmp_range =     param[4];
  uint32_t test_pmp_iszero =    param[5];
  uint32_t test_pmp_cont_nom =  param[6];
  uint32_t test_pmp_cont_denom = param[7];

  auto generate = [&](PExtentVector* cont, PExtentVector& v, uint32_t num_aus) {
    bool prev_is_empty = false;
    uint32_t illegal_pos = (uint32_t)-1;
    while (num_aus > 0) {
      uint32_t a = (rand() % test_region_range) + 1;
      if (a > num_aus) a = num_aus;
      if (prev_is_empty) {
        prev_is_empty = false;
      } else {
        prev_is_empty = (rand() % test_is_empty_denom) < test_is_empty_nom;
      }
      if (prev_is_empty) {
        v.emplace_back(bluestore_pextent_t::INVALID_OFFSET, a * alloc_unit);
      } else {
        if (cont && cont->size() > 0 && cont->back().is_valid()) {
          v.emplace_back(cont->back().end(), a * alloc_unit);
          cont = nullptr;
        } else {
          uint32_t pos;
          do {
            pos = (rand() % 1000000) * alloc_unit;
          } while (pos == illegal_pos);
          v.emplace_back(pos, a * alloc_unit);
          illegal_pos = pos + a * alloc_unit;
        }
      }
      num_aus -= a;
    }
  };
  auto generate_nonempty = [&](PExtentVector* cont, PExtentVector& v, uint32_t num_aus) {
    uint32_t illegal_pos = (uint32_t)-1;
    while (num_aus > 0) {
      uint32_t a = (rand() % test_region_range) + 1;
      if (a > num_aus) a = num_aus;
      if (cont && cont->size() > 0 && cont->back().is_valid()) {
        illegal_pos = cont->back().end() + a * alloc_unit;
        v.emplace_back(cont->back().end(), a * alloc_unit);
        cont = nullptr;
      } else {
        uint32_t pos;
        do {
          pos = (rand() % 1000000) * alloc_unit;
        } while (pos == illegal_pos);
        v.emplace_back(pos, a * alloc_unit);
        illegal_pos = pos + a * alloc_unit;
      }
      num_aus -= a;
    }
  };
  auto append = [&](PExtentVector& dest, const PExtentVector& src) {
    for (auto s: src) {
      if (dest.size() > 0 &&
        ((dest.back().is_valid() && dest.back().end() == s.offset) ||
        (!dest.back().is_valid() && !s.is_valid()) ) ) {
        dest.back().length += s.length;
      } else {
        dest.push_back(s);
      }
    }
  };

  for (int i = 0; i < 10000; i++) {
    PExtentVector pre;
    PExtentVector mid;
    PExtentVector post;

    uint32_t aus1 = std::rand() % (test_pmp_range + test_pmp_iszero);
    uint32_t punch_offset = 0;
    if (aus1 > test_pmp_iszero) {
      aus1 -= test_pmp_iszero;
      generate(nullptr, pre, aus1);
      punch_offset = aus1 * alloc_unit;
    } else {
      aus1 = 0;
    }
    uint32_t aus2 = (std::rand() % test_pmp_range) + 1;
    uint32_t punch_length = aus2 * alloc_unit;
    bool cont2 = std::rand() % test_pmp_cont_denom < test_pmp_cont_nom;
    generate_nonempty(cont2 ? &pre: nullptr, mid, aus2);
    uint32_t aus3 = std::rand() % (test_pmp_range + test_pmp_iszero);
    bool cont3 = std::rand() % test_pmp_cont_denom < test_pmp_cont_nom;
    if (aus3 > test_pmp_iszero) {
      aus3 -= test_pmp_iszero;
      generate(cont3 ? &mid: nullptr, post, aus3);
    } else {
      aus3 = 0;
    }
    uint32_t total_length = (aus1 + aus2 + aus3) * alloc_unit;

    PExtentVector input;
    input.insert(input.end(), pre.begin(), pre.end());
    append(input, mid);
    append(input, post);
    PExtentVector output;
    output.insert(output.end(), pre.begin(), pre.end());
    PExtentVector empty;
    empty.emplace_back(bluestore_pextent_t::INVALID_OFFSET, punch_length);
    append(output, empty);
    append(output, post);

    bluestore_blob_t blob;
    blob.allocated(0, total_length, input);
    PExtentVector result;
//    std::cout << "inp=" << blob.get_extents() << std::endl;
//    std::cout << "punch=0x" << std::hex << punch_offset << "~" << punch_length
//              << std::dec << std::endl;
    //PExtentVector punch;
    //punch.emplace_back(punch_offset, punch_length);
    //blob.release_extents(false, punch, &result);
    blob.release_extents(punch_offset, punch_length, &result);
//    std::cout << "rel=" << result << std::endl;
//    std::cout << "out=" << blob.get_extents() << std::endl;
//    std::cout << std::endl;
    ASSERT_EQ(result, mid);
    ASSERT_EQ(blob.get_extents(), output);
  }
}

/*
  uint32_t alloc_unit = 4096;
  uint32_t test_region_range = 5;
  uint32_t test_is_empty_nom = 1;
  uint32_t test_is_empty_denom = 3;
  uint32_t test_pmp_range = 10;
  uint32_t test_pmp_iszero = 3;
  uint32_t test_pmp_cont_nom = 2;
  uint32_t test_pmp_cont_denom = 4;
  */
INSTANTIATE_TEST_SUITE_P(
  ObjectStore,
  bluestore_blob_t_test,
  ::testing::Values(
    std::vector<int>({4096, 5, 1, 3, 10, 3, 2, 4}),
    std::vector<int>({4096, 10, 2, 3, 30, 10, 3, 5}),
    std::vector<int>({8192, 10, 2, 5, 30, 10, 4, 5}),
    std::vector<int>({32768, 15, 1, 6, 60, 30, 3, 4})
    )
);

class bluestore_blob_use_tracker_t_test :
  public ::testing::Test,
  public ::testing::WithParamInterface<std::vector<int>>
{
};

TEST_P(bluestore_blob_use_tracker_t_test, put_simple)
{
  // generate offset / length
  // get region [offset~length] in tracker
  // choose randomly x points in range [offset~length]
  // in random order
  // put them into tracker
  // check if all is cleared as it should

  std::vector<int> param = GetParam();
  ASSERT_EQ(param.size(), 6);
  uint32_t alloc_unit =         param[0];
  uint32_t test_size_range =    param[1];
  uint32_t test_offset_range =  param[2];
  uint32_t test_offset_length = param[3];
  uint32_t test_aligned_nom =   param[4];
  uint32_t test_aligned_denom = param[5];

  auto rand_next = [&](uint32_t prev) -> uint32_t {
    uint32_t next;
    uint32_t len = rand() % test_size_range + 1;
    if (rand() % test_aligned_denom < test_aligned_nom) {
      // go for aligned
      next = p2roundup(prev + len, alloc_unit);
    } else {
      // unaligned
      next = prev + len;
    }
    return next;
  };
  auto rand_pos = [&](uint32_t range) -> uint32_t {
    if (rand() % test_aligned_denom < test_aligned_nom) {
      return p2align(rand() % range, alloc_unit);
    } else {
      return rand() % range;
    }
  };

  for (int k = 0; k < 10000; k++) {
    std::map<int, std::pair<int, int>> regions;
    uint32_t offset = rand_pos(test_offset_range);
    uint32_t length = 0;
    while (length == 0) {
      length = rand_pos(test_offset_length);
    }
    //std::cout << std::hex << "offset=" << offset
    //          << " length=" << length << std::dec << std::endl;
    uint32_t i = offset;
    uint32_t j = offset;
    while (i < offset + length) {
      j = rand_next(i);
      if (j > offset + length)
        j = offset + length;
      regions[rand() * 10000 + regions.size()] = std::make_pair(i, j - i);
      i = j;
    }
    bluestore_blob_use_tracker_t t;
    t.init(offset + length, alloc_unit);
    t.get(offset, length);

    interval_set<uint32_t> released;
    for (auto r : regions) {
      auto v = t.put_simple(r.second.first, r.second.second);
      //std::cout << std::hex << "0x" << r.second.first << "~" << r.second.second
      //          << "->0x" << v.first << "~" << v.second << std::dec << std::endl;
      if (v.second > 0) {
        released.insert(v.first, v.second);
      }
    }
    ASSERT_FALSE(released.empty());
    ASSERT_EQ(t.get_referenced_bytes(), 0);
    ASSERT_EQ(released.begin().get_start(), p2align(offset, alloc_unit));
    ASSERT_EQ(released.begin().get_end(), p2roundup(offset + length, alloc_unit));
  }
}

/*
  uint32_t alloc_unit = 4096;
  uint32_t test_size_range = 10000;
  uint32_t test_offset_range = 50000;
  uint32_t test_offset_length = 100000;
  uint32_t test_aligned_denom = 3;
  uint32_t test_aligned_nom = 2;
*/
INSTANTIATE_TEST_SUITE_P(
  BlueStore,
  bluestore_blob_use_tracker_t_test,
  ::testing::Values(
    std::vector<int>({4096, 10000, 50000, 100000, 2, 3}),
    std::vector<int>({4096, 10000, 40000, 80000, 1, 11}),
    std::vector<int>({8192, 30000, 80000, 160000, 2, 4}),
    std::vector<int>({32768, 40000, 80000, 160000, 5, 6})
    )
);


//---------------------------------------------------------------------------------
static int verify_extent(const extent_t &ext, const extent_t *ext_arr,
                         uint64_t ext_arr_size, uint64_t idx) {
  const extent_t &ext_ref = ext_arr[idx];
  if (ext.offset == ext_ref.offset && ext.length == ext_ref.length) {
    return 0;
  } else {
    std::cerr << "mismatch was found at index " << idx << std::endl;
    if (ext.length == 0) {
      std::cerr << "Null extent was returned at idx = " << idx << std::endl;
    }
    unsigned start = std::max(((int32_t)(idx)-3), 0);
    unsigned end = std::min(idx + 3, ext_arr_size);
    for (unsigned j = start; j < end; j++) {
      const extent_t &ext_ref = ext_arr[j];
      std::cerr << j << ") ref_ext = [" << ext_ref.offset << ", "
                << ext_ref.length << "]" << std::endl;
    }
    std::cerr << idx << ") ext     = [" << ext.offset << ", " << ext.length
              << "]" << std::endl;
    return -1;
  }
}

//---------------------------------------------------------------------------------
static int test_extents(uint64_t index, extent_t *ext_arr,
                        uint64_t ext_arr_size, SimpleBitmap &sbmap, bool set) {
  const uint64_t MAX_JUMP_BIG = 1523;
  const uint64_t MAX_JUMP_SMALL = 19;
  const uint64_t MAX_LEN_BIG = 523;
  const uint64_t MAX_LEN_SMALL = 23;

  uint64_t n = sbmap.get_size();
  uint64_t offset = 0;
  unsigned length, jump, i;
  for (i = 0; i < ext_arr_size; i++) {
    if (i & 3) {
      jump = std::rand() % MAX_JUMP_BIG;
    } else {
      jump = std::rand() % MAX_JUMP_SMALL;
    }
    offset += jump;
    if (i & 1) {
      length = std::rand() % MAX_LEN_BIG;
    } else {
      length = std::rand() % MAX_LEN_SMALL;
    }
    // make sure no zero length will be used
    length++;
    if (offset + length >= n) {
      break;
    }

    bool success;
    if (set) {
      success = sbmap.set(offset, length);
    } else {
      success = sbmap.clr(offset, length);
    }
    if (!success) {
      std::cerr << "Failed sbmap." << (set ? "set(" : "clr(") << offset << ", "
                << length << ")" << std::endl;
      return -1;
    }

    // if this is not the first entry and no jump -> merge extents
    if ((i == 0) || (jump > 0)) {
      ext_arr[i] = {offset, length};
    } else {
      // merge 2 extents
      i--;
      ext_arr[i].length += length;
    }
    offset += length;
  }
  unsigned arr_size = std::min((uint64_t)i, ext_arr_size);
  std::cout << std::hex << std::right;
  std::cout << "[" << index << "] " << (set ? "Set::" : "Clr::")
            << " extents count = 0x" << arr_size;
  std::cout << std::dec << std::endl;

  offset = 0;
  extent_t ext;
  for (unsigned i = 0; i < arr_size; i++) {
    if (set) {
      ext = sbmap.get_next_set_extent(offset);
    } else {
      ext = sbmap.get_next_clr_extent(offset);
    }

    if (verify_extent(ext, ext_arr, ext_arr_size, i) != 0) {
      return -1;
    }
    offset = ext.offset + ext.length;
  }

  if (set) {
    ext = sbmap.get_next_set_extent(offset);
  } else {
    ext = sbmap.get_next_clr_extent(offset);
  }
  if (ext.length == 0) {
    return 0;
  } else {
    std::cerr << "sbmap.get_next_" << (set ? "set" : "clr") << "_extent("
              << offset << ") return length = " << ext.length << std::endl;
    return -1;
  }
}

//---------------------------------------------------------------------------------
TEST(SimpleBitmap, basic) {
  const uint64_t MAX_EXTENTS_COUNT = 7131177;
  std::unique_ptr<extent_t[]> ext_arr =
      std::make_unique<extent_t[]>(MAX_EXTENTS_COUNT);
  ASSERT_TRUE(ext_arr != nullptr);
  const uint64_t BIT_COUNT = 4ULL << 30; // 4Gb = 512MB
  SimpleBitmap sbmap(g_ceph_context, BIT_COUNT);

  // use current time as seed for random generator
  std::srand(std::time(nullptr));
  for (unsigned i = 0; i < 3; i++) {
    memset(ext_arr.get(), 0, sizeof(extent_t) * MAX_EXTENTS_COUNT);
    sbmap.clear_all();
    ASSERT_TRUE(
        test_extents(i, ext_arr.get(), MAX_EXTENTS_COUNT, sbmap, true) == 0);

    memset(ext_arr.get(), 0, sizeof(extent_t) * MAX_EXTENTS_COUNT);
    sbmap.set_all();
    ASSERT_TRUE(
        test_extents(i, ext_arr.get(), MAX_EXTENTS_COUNT, sbmap, false) == 0);
  }
}

//---------------------------------------------------------------------------------
static int test_intersections(unsigned test_idx, SimpleBitmap &sbmap,
                              uint8_t map[], uint64_t map_size) {
  const uint64_t MAX_LEN_BIG = 523;
  const uint64_t MAX_LEN_SMALL = 23;

  bool success;
  uint64_t set_op_count = 0, clr_op_count = 0;
  unsigned length, i;
  for (i = 0; i < map_size / (MAX_LEN_BIG * 2); i++) {
    uint64_t offset = (std::rand() % (map_size - 1));
    if (i & 1) {
      length = std::rand() % MAX_LEN_BIG;
    } else {
      length = std::rand() % MAX_LEN_SMALL;
    }
    // make sure no zero length will be used
    length++;
    if (offset + length >= map_size) {
      continue;
    }
    // 2:1 set/clr
    bool set = (std::rand() % 3);
    if (set) {
      success = sbmap.set(offset, length);
      memset(map + offset, 0xFF, length);
      set_op_count++;
    } else {
      success = sbmap.clr(offset, length);
      memset(map + offset, 0x0, length);
      clr_op_count++;
    }
    if (!success) {
      std::cerr << "Failed sbmap." << (set ? "set(" : "clr(") << offset << ", "
                << length << ")" << std::endl;
      return -1;
    }
  }

  uint64_t set_bit_count = 0;
  uint64_t clr_bit_count = 0;
  for (uint64_t idx = 0; idx < map_size; idx++) {
    if (map[idx]) {
      set_bit_count++;
      success = sbmap.bit_is_set(idx);
    } else {
      clr_bit_count++;
      success = sbmap.bit_is_clr(idx);
    }
    if (!success) {
      std::cerr << "expected: sbmap.bit_is_" << (map[idx] ? "set(" : "clr(")
                << idx << ")" << std::endl;
      return -1;
    }
  }
  std::cout << std::hex << std::right << __func__;
  std::cout << " [" << test_idx << "] set_bit_count = 0x" << std::setfill('0')
            << std::setw(8) << set_bit_count << ", clr_bit_count = 0x"
            << std::setfill('0') << std::setw(8) << clr_bit_count
            << ", sum = 0x" << set_bit_count + clr_bit_count << std::endl;
  std::cout << std::dec;
  uint64_t offset = 0;
  for (uint64_t i = 0; i < (set_op_count + clr_op_count); i++) {
    extent_t ext = sbmap.get_next_set_extent(offset);
    // std::cout << "set_ext:: " << i << ") [" << ext.offset     << ", " <<
    // ext.length     << "]" << std::endl;
    for (uint64_t idx = ext.offset; idx < ext.offset + ext.length; idx++) {
      if (map[idx] != 0xFF) {
        std::cerr << "map[" << idx << "] is clear, but extent [" << ext.offset
                  << ", " << ext.length << "] is set" << std::endl;
        return -1;
      }
    }
    offset = ext.offset + ext.length;
  }

  offset = 0;
  for (uint64_t i = 0; i < (set_op_count + clr_op_count); i++) {
    extent_t ext = sbmap.get_next_clr_extent(offset);
    // std::cout << "clr_ext:: " << i << ") [" << ext.offset     << ", " <<
    // ext.length     << "]" << std::endl;
    for (uint64_t idx = ext.offset; idx < ext.offset + ext.length; idx++) {
      if (map[idx]) {
        std::cerr << "map[" << idx << "] is set, but extent [" << ext.offset
                  << ", " << ext.length << "] is free" << std::endl;
        return -1;
      }
    }
    offset = ext.offset + ext.length;
  }

  return 0;
}

//---------------------------------------------------------------------------------
TEST(SimpleBitmap, intersection) {
  const uint64_t MAP_SIZE = 1ULL << 30; // 1G
  SimpleBitmap sbmap(g_ceph_context, MAP_SIZE);

  // use current time as seed for random generator
  std::srand(std::time(nullptr));

  std::unique_ptr<uint8_t[]> map = std::make_unique<uint8_t[]>(MAP_SIZE);
  ASSERT_TRUE(map != nullptr);

  for (unsigned i = 0; i < 1; i++) {
    sbmap.clear_all();
    memset(map.get(), 0, MAP_SIZE);
    ASSERT_TRUE(test_intersections(i, sbmap, map.get(), MAP_SIZE) == 0);

    sbmap.set_all();
    memset(map.get(), 0xFF, MAP_SIZE);
    ASSERT_TRUE(test_intersections(i, sbmap, map.get(), MAP_SIZE) == 0);
  }
}

//---------------------------------------------------------------------------------
static int test_extents_boundaries(uint64_t index, extent_t *ext_arr,
                                   uint64_t ext_arr_size, SimpleBitmap &sbmap,
                                   bool set) {
  uint64_t n = sbmap.get_size();
  uint64_t offset = 0, k = 0;
  for (unsigned i = 0; i < 64; i++) {
    offset += i;
    if (offset >= n) {
      break;
    }

    for (unsigned length = 1; length <= 128; length++) {
      if (offset + length >= n) {
        break;
      }

      if (k >= ext_arr_size) {
        break;
      }
      bool success;
      if (set) {
        success = sbmap.set(offset, length);
      } else {
        success = sbmap.clr(offset, length);
      }
      if (!success) {
        std::cerr << "Failed sbmap." << (set ? "set(" : "clr(") << offset
                  << ", " << length << ")" << std::endl;
        return -1;
      }
      ext_arr[k++] = {offset, length};
      if (length < 64) {
        offset += 64;
      } else {
        offset += 128;
      }
    }
    if (k >= ext_arr_size) {
      break;
    }
  }

  unsigned arr_size = std::min((uint64_t)k, ext_arr_size);
  std::cout << std::hex << std::right << __func__;
  std::cout << " [" << index << "] " << (set ? "Set::" : "Clr::")
            << " extents count = 0x" << arr_size;
  std::cout << std::dec << std::endl;

  offset = 0;
  extent_t ext;
  for (unsigned i = 0; i < arr_size; i++) {
    if (set) {
      ext = sbmap.get_next_set_extent(offset);
    } else {
      ext = sbmap.get_next_clr_extent(offset);
    }

    if (verify_extent(ext, ext_arr, ext_arr_size, i) != 0) {
      return -1;
    }
    offset = ext.offset + ext.length;
  }

  if (set) {
    ext = sbmap.get_next_set_extent(offset);
  } else {
    ext = sbmap.get_next_clr_extent(offset);
  }
  if (ext.length == 0) {
    return 0;
  } else {
    std::cerr << "sbmap.get_next_" << (set ? "set" : "clr") << "_extent("
              << offset << ") return length = " << ext.length << std::endl;
    return -1;
  }
}

//---------------------------------------------------------------------------------
TEST(SimpleBitmap, boundaries) {
  const uint64_t MAX_EXTENTS_COUNT = 64 << 10;
  std::unique_ptr<extent_t[]> ext_arr =
      std::make_unique<extent_t[]>(MAX_EXTENTS_COUNT);
  ASSERT_TRUE(ext_arr != nullptr);

  // use current time as seed for random generator
  std::srand(std::time(nullptr));

  uint64_t bit_count = 32 << 20; // 32Mb = 4MB
  unsigned count = 0;
  for (unsigned i = 0; i < 64; i++) {
    SimpleBitmap sbmap(g_ceph_context, bit_count + i);
    memset(ext_arr.get(), 0, sizeof(extent_t) * MAX_EXTENTS_COUNT);
    sbmap.clear_all();
    ASSERT_TRUE(test_extents_boundaries(count, ext_arr.get(), MAX_EXTENTS_COUNT,
                                        sbmap, true) == 0);

    memset(ext_arr.get(), 0, sizeof(extent_t) * MAX_EXTENTS_COUNT);
    sbmap.set_all();
    ASSERT_TRUE(test_extents_boundaries(count++, ext_arr.get(),
                                        MAX_EXTENTS_COUNT, sbmap, false) == 0);
  }
}

//---------------------------------------------------------------------------------
TEST(SimpleBitmap, boundaries2) {
  const uint64_t bit_count_base = 64 << 10; // 64Kb = 8MB
  const extent_t null_extent = {0, 0};

  for (unsigned i = 0; i < 64; i++) {
    uint64_t bit_count = bit_count_base + i;
    extent_t full_extent = {0, bit_count};
    SimpleBitmap sbmap(g_ceph_context, bit_count);

    sbmap.set(0, bit_count);
    ASSERT_TRUE(sbmap.get_next_set_extent(0) == full_extent);
    ASSERT_TRUE(sbmap.get_next_clr_extent(0) == null_extent);

    for (uint64_t bit = 0; bit < bit_count; bit++) {
      sbmap.clr(bit, 1);
    }
    ASSERT_TRUE(sbmap.get_next_set_extent(0) == null_extent);
    ASSERT_TRUE(sbmap.get_next_clr_extent(0) == full_extent);

    for (uint64_t bit = 0; bit < bit_count; bit++) {
      sbmap.set(bit, 1);
    }
    ASSERT_TRUE(sbmap.get_next_set_extent(0) == full_extent);
    ASSERT_TRUE(sbmap.get_next_clr_extent(0) == null_extent);

    sbmap.clr(0, bit_count);
    ASSERT_TRUE(sbmap.get_next_set_extent(0) == null_extent);
    ASSERT_TRUE(sbmap.get_next_clr_extent(0) == full_extent);
  }
}

TEST(shared_blob_2hash_tracker_t, basic_test) {
  shared_blob_2hash_tracker_t t1(1024 * 1024, 4096);

  ASSERT_TRUE(t1.count_non_zero() == 0);

  t1.inc(0, 0, 1);
  ASSERT_TRUE(t1.count_non_zero() != 0);
  t1.inc(0, 0, -1);
  ASSERT_TRUE(t1.count_non_zero() == 0);

  t1.inc(3, 0x1000, 2);
  ASSERT_TRUE(t1.count_non_zero() != 0);
  t1.inc(3, 0x1000, -1);
  ASSERT_TRUE(t1.count_non_zero() != 0);
  t1.inc(3, 0x1000, -1);
  ASSERT_TRUE(t1.count_non_zero() == 0);

  t1.inc(2, 0x2000, 5);
  ASSERT_TRUE(t1.count_non_zero() != 0);
  t1.inc(18, 0x2000, -5);
  ASSERT_TRUE(t1.count_non_zero() != 0);
  t1.inc(18, 0x2000, 1);
  ASSERT_TRUE(t1.count_non_zero() != 0);
  t1.inc(2, 0x2000, -1);
  ASSERT_TRUE(t1.count_non_zero() != 0);
  t1.inc(18, 0x2000, 4);
  ASSERT_TRUE(t1.count_non_zero() != 0);
  t1.inc(2, 0x2000, -4);
  ASSERT_TRUE(t1.count_non_zero() == 0);

  t1.inc(3, 0x3000, 2);
  ASSERT_TRUE(t1.count_non_zero() != 0);
  t1.inc(4, 0x3000, -1);
  ASSERT_TRUE(t1.count_non_zero() != 0);
  t1.inc(4, 0x3000, -1);
  ASSERT_TRUE(t1.count_non_zero() != 0);
  t1.inc(3, 0x3000, -2);
  ASSERT_TRUE(t1.count_non_zero() != 0);
  t1.inc(4, 0x3000, 1);
  ASSERT_TRUE(t1.count_non_zero() != 0);
  t1.inc(4, 0x3000, 1);
  ASSERT_TRUE(t1.count_non_zero() == 0);

  t1.inc(5, 0x1000, 1);
  t1.inc(5, 0x2000, 3);
  t1.inc(5, 0x3000, 2);
  t1.inc(5, 0x8000, 1);

  ASSERT_TRUE(t1.count_non_zero() != 0);

  ASSERT_TRUE(!t1.test_all_zero(5, 0x1000));
  ASSERT_TRUE(!t1.test_all_zero(5, 0x2000));
  ASSERT_TRUE(!t1.test_all_zero(5, 0x3000));
  ASSERT_TRUE(t1.test_all_zero(5, 0x4000));
  ASSERT_TRUE(!t1.test_all_zero(5, 0x8000));

  ASSERT_TRUE(t1.test_all_zero_range(5, 0, 0x1000));
  ASSERT_TRUE(t1.test_all_zero_range(5, 0x500, 0x500));
  ASSERT_TRUE(!t1.test_all_zero_range(5, 0x500, 0x1500));
  ASSERT_TRUE(!t1.test_all_zero_range(5, 0x1500, 0x3200));
  ASSERT_TRUE(t1.test_all_zero_range(5, 0x4500, 0x1500));
  ASSERT_TRUE(t1.test_all_zero_range(5, 0x4500, 0x3b00));
  ASSERT_TRUE(!t1.test_all_zero_range(5, 0, 0x9000));
}

TEST(bluestore_blob_use_tracker_t, mempool_stats_test) {
  using mempool::bluestore_cache_other::allocated_bytes;
  using mempool::bluestore_cache_other::allocated_items;
  uint64_t other_items0 = allocated_items();
  uint64_t other_bytes0 = allocated_bytes();
  {
    bluestore_blob_use_tracker_t *t1 = new bluestore_blob_use_tracker_t;

    t1->init(1024 * 1024, 4096);
    ASSERT_EQ(256, allocated_items() - other_items0);  // = 1M / 4K
    ASSERT_EQ(1024, allocated_bytes() - other_bytes0); // = 1M / 4K * 4

    delete t1;
    ASSERT_EQ(allocated_items(), other_items0);
    ASSERT_EQ(allocated_bytes(), other_bytes0);
  }
  {
    bluestore_blob_use_tracker_t *t1 = new bluestore_blob_use_tracker_t;

    t1->init(1024 * 1024, 4096);
    t1->add_tail(2048 * 1024, 4096);
    // proper stats update after tail add
    ASSERT_EQ(512, allocated_items() - other_items0);  // = 2M / 4K
    ASSERT_EQ(2048, allocated_bytes() - other_bytes0); // = 2M / 4K * 4

    delete t1;
    ASSERT_EQ(allocated_items(), other_items0);
    ASSERT_EQ(allocated_bytes(), other_bytes0);
  }
  {
    bluestore_blob_use_tracker_t *t1 = new bluestore_blob_use_tracker_t;

    t1->init(1024 * 1024, 4096);
    t1->prune_tail(512 * 1024);
    // no changes in stats after pruning
    ASSERT_EQ(256, allocated_items() - other_items0);  // = 1M / 4K
    ASSERT_EQ(1024, allocated_bytes() - other_bytes0); // = 1M / 4K * 4

    delete t1;
    ASSERT_EQ(allocated_items(), other_items0);
    ASSERT_EQ(allocated_bytes(), other_bytes0);
  }
  {
    bluestore_blob_use_tracker_t *t1 = new bluestore_blob_use_tracker_t;
    bluestore_blob_use_tracker_t *t2 = new bluestore_blob_use_tracker_t;

    t1->init(1024 * 1024, 4096);

    // t1 keeps the same amount of entries + t2 has got half of them
    t1->split(512 * 1024, t2);
    ASSERT_EQ(256 + 128, allocated_items() - other_items0);  //= 1M / 4K*1.5
    ASSERT_EQ(1024 + 512, allocated_bytes() - other_bytes0); //= 1M / 4K*4*1.5

    // t1 & t2 release everything, then t2 get one less entry than t2 had had
    // before
    t1->split(4096, t2);
    ASSERT_EQ(127, allocated_items() - other_items0);     // = 512K / 4K - 1
    ASSERT_EQ(127 * 4, allocated_bytes() - other_bytes0); // = 512L / 4K * 4 - 4
    delete t1;
    delete t2;
    ASSERT_EQ(allocated_items(), other_items0);
    ASSERT_EQ(allocated_bytes(), other_bytes0);
  }
}

int main(int argc, char **argv) {
  auto args = argv_to_vec(argc, argv);
  auto cct =
      global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
                  CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
