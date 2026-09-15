// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include <gtest/gtest.h>
#include "osd/ECExtentCache.h"

using namespace std;
using namespace ECUtil;

shard_extent_map_t imap_from_vector(vector<vector<pair<uint64_t, uint64_t>>> &&in, stripe_info_t const *sinfo)
{
  shard_extent_map_t out(sinfo);
  for (int shard = 0; shard < (int)in.size(); shard++) {
    for (auto &&tup: in[shard]) {
      bufferlist bl;
      bl.append_zero(tup.second);
      out.insert_in_shard(shard_id_t(shard), tup.first, bl);
    }
  }
  return out;
}

shard_extent_map_t imap_from_iset(const shard_extent_set_t &sset, stripe_info_t *sinfo)
{
  shard_extent_map_t out(sinfo);

  for (auto &&[shard, set]: sset) {
    for (auto &&iter: set) {
      bufferlist bl;
      bl.append_zero(iter.second);
      out.insert_in_shard(shard, iter.first, bl);
    }
  }
  return out;
}

shard_extent_set_t iset_from_vector(vector<vector<pair<uint64_t, uint64_t>>> &&in, const stripe_info_t *sinfo)
{
  shard_extent_set_t out(sinfo->get_k_plus_m());
  for (int shard = 0; shard < (int)in.size(); shard++) {
    for (auto &&tup: in[shard]) {
      out[shard_id_t(shard)].insert(tup.first, tup.second);
    }
  }
  return out;
}

struct Client : public ECExtentCache::BackendReadListener
{
  hobject_t oid = hobject_t().make_temp_hobject("My first object");
  stripe_info_t sinfo;
  ECExtentCache::LRU lru;
  ECExtentCache cache;
  optional<shard_extent_set_t> active_reads;
  list<shard_extent_map_t> results;

  Client(uint64_t chunk_size, int k, int m, uint64_t cache_size) :
    sinfo(k, m, k*chunk_size, vector<shard_id_t>(0)),
    lru(cache_size), cache(*this, lru, sinfo, g_ceph_context) {};

  void backend_read(hobject_t _oid, const shard_extent_set_t& request,
    uint64_t object_size) override  {
    ceph_assert(oid == _oid);
    active_reads = request;
  }

  void cache_ready(const hobject_t& _oid, const shard_extent_map_t& _result)
  {
    ceph_assert(oid == _oid);
    results.emplace_back(_result);
  }

  void complete_read()
  {
    auto reads_done = imap_from_iset(*active_reads, &sinfo);
    active_reads.reset(); // set before done, as may be called back.
    cache.read_done(oid, std::move(reads_done));
  }

  void complete_write(ECExtentCache::OpRef &op)
  {
    shard_extent_map_t emap = imap_from_iset(op->get_writes(), &sinfo);
    //Fill in the parity. Parity correctness does not matter to the cache.
    emap.insert_parity_buffers();
    results.clear();
    cache.write_done(op, std::move(emap));
  }

  void cache_execute(ECExtentCache::OpRef &op)
  {
    list<ECExtentCache::OpRef> l;
    l.emplace_back(op);
    cache.execute(l);
  }

  const stripe_info_t *get_stripe_info() const { return &sinfo; }
};

TEST(ECExtentCache, double_write_done)
{
  Client cl(32, 2, 1, 64);

  auto to_write = iset_from_vector({{{0, 10}}, {{0, 10}}}, cl.get_stripe_info());

  optional op = cl.cache.prepare(cl.oid, nullopt, to_write, 10, 10, false,
  [&cl](ECExtentCache::OpRef &op)
  {
    cl.cache_ready(op->get_hoid(), op->get_result());
  });
  cl.cache_execute(*op);
  cl.complete_write(*op);
}

TEST(ECExtentCache, simple_write)
{
  Client cl(32, 2, 1, 64);
  {
    auto to_read = iset_from_vector( {{{0, 2}}, {{0, 2}}}, cl.get_stripe_info());
    auto to_write = iset_from_vector({{{0, 10}}, {{0, 10}}}, cl.get_stripe_info());

    /*    OpRef request(hobject_t const &oid,
      std::optional<std::shard_extent_set_t> const &to_read,
      std::shard_extent_set_t const &write,
      uint64_t orig_size,
      uint64_t projected_size,
      CacheReadyCb &&ready_cb)
      */

    optional op = cl.cache.prepare(cl.oid, to_read, to_write, 10, 10, false,
      [&cl](ECExtentCache::OpRef &op)
      {
        cl.cache_ready(op->get_hoid(), op->get_result());
      });
    cl.cache_execute(*op);
    ASSERT_EQ(to_read, cl.active_reads);
    ASSERT_TRUE(cl.results.empty());
    cl.complete_read();

    ASSERT_FALSE(cl.active_reads);
    ASSERT_EQ(1, cl.results.size());
    ASSERT_EQ(to_read, cl.results.front().get_extent_set());
    cl.complete_write(*op);

    ASSERT_FALSE(cl.active_reads);
    ASSERT_TRUE(cl.results.empty());
    op.reset();
  }

  // Repeating the same read should complete without a backend read..
  {
    auto to_read = iset_from_vector( {{{0, 2}}, {{0, 2}}}, cl.get_stripe_info());
    auto to_write = iset_from_vector({{{0, 10}}, {{0, 10}}}, cl.get_stripe_info());
    optional op = cl.cache.prepare(cl.oid, to_read, to_write, 10, 10, false,
      [&cl](ECExtentCache::OpRef &op)
      {
        cl.cache_ready(op->get_hoid(), op->get_result());
      });
    cl.cache_execute(*op);
    ASSERT_FALSE(cl.active_reads);
    ASSERT_FALSE(cl.results.empty());
    ASSERT_EQ(1, cl.results.size());
    ASSERT_EQ(to_read, cl.results.front().get_extent_set());
    cl.complete_write(*op);
    op.reset();
  }

  // Perform a read overlapping with the previous write, but not hte previous read.
  // This should not result in any backend reads, since the cache can be honoured
  // from the previous write.
  {
    auto to_read = iset_from_vector( {{{2, 2}}, {{2, 2}}}, cl.get_stripe_info());
    auto to_write = iset_from_vector({{{0, 10}}, {{0, 10}}}, cl.get_stripe_info());
    optional op = cl.cache.prepare(cl.oid, to_read, to_write, 10, 10, false,
      [&cl](ECExtentCache::OpRef &op)
      {
        cl.cache_ready(op->get_hoid(), op->get_result());
      });
    cl.cache_execute(*op);

    // SHould have remained in LRU!
    ASSERT_FALSE(cl.active_reads);
    ASSERT_EQ(1, cl.results.size());
    ASSERT_EQ(to_read, cl.results.front().get_extent_set());
    cl.complete_write(*op);
    op.reset();
  }
}

TEST(ECExtentCache, sequential_appends) {
  Client cl(32, 2, 1, 32);

  auto to_write1 = iset_from_vector({{{0, 10}}}, cl.get_stripe_info());

  // The first write...
  optional op1 = cl.cache.prepare(cl.oid, nullopt, to_write1, 0, 10, false,
   [&cl](ECExtentCache::OpRef &op)
   {
      cl.cache_ready(op->get_hoid(), op->get_result());
   });
  cl.cache_execute(*op1);

  // Write should have been honoured immediately.
  ASSERT_FALSE(cl.results.empty());
  auto to_write2 = iset_from_vector({{{10, 10}}}, cl.get_stripe_info());
  cl.complete_write(*op1);
  ASSERT_TRUE(cl.results.empty());

  // The first write...
  optional op2 = cl.cache.prepare(cl.oid, nullopt, to_write1, 10, 20, false,
   [&cl](ECExtentCache::OpRef &op)
   {
      cl.cache_ready(op->get_hoid(), op->get_result());
   });
  cl.cache_execute(*op2);

  ASSERT_FALSE(cl.results.empty());
  cl.complete_write(*op2);

}

TEST(ECExtentCache, multiple_writes)
{
  Client cl(32, 2, 1, 32);

  auto to_read1 = iset_from_vector( {{{0, 2}}}, cl.get_stripe_info());
  auto to_write1 = iset_from_vector({{{0, 10}}}, cl.get_stripe_info());

  // This should drive a request for this IO, which we do not yet honour.
  optional op1 = cl.cache.prepare(cl.oid, to_read1, to_write1, 10, 10, false,
   [&cl](ECExtentCache::OpRef &op)
   {
      cl.cache_ready(op->get_hoid(), op->get_result());
   });
  cl.cache_execute(*op1);
  ASSERT_EQ(to_read1, cl.active_reads);
  ASSERT_TRUE(cl.results.empty());

  // Perform another request. We should not see any change in the read requests.
  auto to_read2 = iset_from_vector( {{{8, 4}}}, cl.get_stripe_info());
  auto to_write2 = iset_from_vector({{{10, 10}}}, cl.get_stripe_info());
  optional op2 = cl.cache.prepare(cl.oid, to_read2, to_write2, 10, 10, false,
   [&cl](ECExtentCache::OpRef &op)
   {
      cl.cache_ready(op->get_hoid(), op->get_result());
   });
  cl.cache_execute(*op2);
  ASSERT_EQ(to_read1, cl.active_reads);
  ASSERT_TRUE(cl.results.empty());

  // Perform another request, this to check that reads are coalesced.
  auto to_read3 = iset_from_vector( {{{32, 6}}}, cl.get_stripe_info());
  auto to_write3 = iset_from_vector({}, cl.get_stripe_info());
  optional op3 = cl.cache.prepare(cl.oid, to_read3, to_write3, 10, 10, false,
   [&cl](ECExtentCache::OpRef &op)
   {
      cl.cache_ready(op->get_hoid(), op->get_result());
   });
  cl.cache_execute(*op3);
  ASSERT_EQ(to_read1, cl.active_reads);
  ASSERT_TRUE(cl.results.empty());

  // Finally op4, with no reads.
  auto to_write4 = iset_from_vector({{{20, 10}}}, cl.get_stripe_info());
  optional op4 = cl.cache.prepare(cl.oid, nullopt, to_write4, 10, 10, false,
   [&cl](ECExtentCache::OpRef &op)
   {
      cl.cache_ready(op->get_hoid(), op->get_result());
   });
  cl.cache_execute(*op4);
  ASSERT_EQ(to_read1, cl.active_reads);
  ASSERT_TRUE(cl.results.empty());

  // Completing the first read will allow the first write and start a batched read.
  // Note that the cache must not read what was written in op 1.
  cl.complete_read();
  auto expected_read = iset_from_vector({{{10,2}, {32,6}}}, cl.get_stripe_info());
  ASSERT_EQ(expected_read, cl.active_reads);
  ASSERT_EQ(1, cl.results.size());
  ASSERT_EQ(to_read1, cl.results.front().get_extent_set());
  cl.complete_write(*op1);

  // The next write requires some more reads, so should not occur.
  ASSERT_TRUE(cl.results.empty());

  // All reads complete, this should allow for op2 to be ready.
  cl.complete_read();
  ASSERT_FALSE(cl.active_reads);
  ASSERT_EQ(3, cl.results.size());
  auto result = cl.results.begin();
  ASSERT_EQ(to_read2, result++->get_extent_set());
  ASSERT_EQ(to_read3, result++->get_extent_set());
  ASSERT_TRUE(result++->empty());

  cl.complete_write(*op2);
  cl.complete_write(*op3);
  cl.complete_write(*op4);

  op1.reset();
  op2.reset();
  op3.reset();
  op4.reset();
}

int dummies;
struct Dummy
{
  Dummy() {dummies++;}
  ~Dummy() {dummies--;}
};

TEST(ECExtentCache, on_change)
{
  Client cl(32, 2, 1, 64);
  auto to_read1 = iset_from_vector( {{{0, 2}}}, cl.get_stripe_info());
  auto to_write1 = iset_from_vector({{{0, 10}}}, cl.get_stripe_info());

  optional<ECExtentCache::OpRef> op;
  optional<shared_ptr<Dummy>> dummy;

  dummy.emplace(make_shared<Dummy>());
  ceph_assert(dummies == 1);
  {
    shared_ptr<Dummy> d = *dummy;
    /* Here we generate an op that we never expect to be completed. Note that
     * some static code analysis tools suggest deleting d here. DO NOT DO THIS
     * as we are relying on side effects from the destruction of d in this test.
     */
    op.emplace(cl.cache.prepare(cl.oid, to_read1, to_write1, 10, 10, false,
      [d](ECExtentCache::OpRef &ignored)
      {
        ceph_abort("Should be cancelled");
      }));
  }
  cl.cache_execute(*op);

  /* We now have the following graph of objects:
   * cache -- op -- lambda -- d
   *                 dummy --/
   */
  ASSERT_EQ(1, dummies);

  /* Executing the on_change will "cancel" this cache op.  This will cause it
   * to release the lambda, reducing us down to dummy -- d
   */
  cl.cache.on_change();
  ASSERT_EQ(1, dummies);

  /* This emulates the rmw pipeline clearing outstanding IO.  We now have no
   * references to d, so we should have destructed the object.
   * */
  dummy.reset();
  ASSERT_EQ(0, dummies);

  /* Keeping the op alive here is emulating the dummy keeping a record of the
   * cache op. It will also be destroyed at this point by rmw pipeline.
   */
  ASSERT_FALSE(cl.cache.idle());
  op.reset();
  ASSERT_TRUE(cl.cache.idle());

  // The cache has its own asserts, which we should honour.
  cl.cache.on_change2();
}

TEST(ECExtentCache, multiple_misaligned_writes)
{
  Client cl(256*1024, 2, 1, 1024*1024);

  // IO 1 is really a 6k write. The write is inflated to 8k, but the second 4k is
  // partial, so we read the second 4k to RMW
  auto to_read1 = iset_from_vector( {{{4*1024, 4*1024}}}, cl.get_stripe_info());
  auto to_write1 = iset_from_vector({{{0, 8*1024}}}, cl.get_stripe_info());

  // IO 2 is the next 8k write, starting at 6k. So we have a 12k write, reading the
  // first and last pages. The first part of this read should be in the cache.
  auto to_read2 = iset_from_vector( {{{4*1024, 4*1024}, {12*4096, 4*4096}}}, cl.get_stripe_info());
  auto to_read2_exec = iset_from_vector( {{{12*4096, 4*4096}}}, cl.get_stripe_info());
  auto to_write2 = iset_from_vector({{{4*1024, 12*1024}}}, cl.get_stripe_info());

  // IO 3 is the next misaligned 4k, very similar to IO 3.
  auto to_read3 = iset_from_vector( {{{12*1024, 4*1024}, {20*4096, 4*4096}}}, cl.get_stripe_info());
  auto to_read3_exec = iset_from_vector( {{{20*4096, 4*4096}}}, cl.get_stripe_info());
  auto to_write3 = iset_from_vector({{{12*1024, 12*1024}}}, cl.get_stripe_info());

  //Perform the first write, which should result in a read.
  optional op1 = cl.cache.prepare(cl.oid, to_read1, to_write1, 22*1024, 22*1024, false,
   [&cl](ECExtentCache::OpRef &op)
   {
     cl.cache_ready(op->get_hoid(), op->get_result());
   });
  cl.cache_execute(*op1);
  ASSERT_EQ(to_read1, cl.active_reads);
  ASSERT_TRUE(cl.results.empty());

  // Submit the second IO.
  optional op2 = cl.cache.prepare(cl.oid, to_read2, to_write2, 22*1024, 22*1024, false,
   [&cl](ECExtentCache::OpRef &op)
   {
     cl.cache_ready(op->get_hoid(), op->get_result());
   });
  cl.cache_execute(*op2);
  // We should still be executing read 1.
  ASSERT_EQ(to_read1, cl.active_reads);
  ASSERT_TRUE(cl.results.empty());

  // Allow the read to complete. We should now have op1 done...
  cl.complete_read();
  ASSERT_EQ(to_read2_exec, cl.active_reads);
  ASSERT_FALSE(cl.results.empty());
  cl.complete_write(*op1);

  // And move on to op3
  optional op3 = cl.cache.prepare(cl.oid, to_read3, to_write3, 22*1024, 22*1024, false,
   [&cl](ECExtentCache::OpRef &op)
   {
     cl.cache_ready(op->get_hoid(), op->get_result());
   });
  cl.cache_execute(*op3);
  // We should still be executing read 1.
  ASSERT_EQ(to_read2_exec, cl.active_reads);
  ASSERT_TRUE(cl.results.empty());

  // Allow the read to complete. We should now have op2 done...
  cl.complete_read();
  ASSERT_EQ(to_read3_exec, cl.active_reads);
  ASSERT_FALSE(cl.results.empty());
  cl.complete_write(*op2);
  ASSERT_EQ(to_read3_exec, cl.active_reads);
  ASSERT_TRUE(cl.results.empty());
  cl.complete_read();
  ASSERT_FALSE(cl.results.empty());
  cl.complete_write(*op3);

}

TEST(ECExtentCache, multiple_misaligned_writes2)
{
  Client cl(256*1024, 2, 1, 1024*1024);

  // IO 1 is really a 6k write. The write is inflated to 8k, but the second 4k is
  // partial, so we read the second 4k to RMW
  auto to_read1 = iset_from_vector( {{{4*1024, 4*1024}}}, cl.get_stripe_info());
  auto to_write1 = iset_from_vector({{{0, 8*1024}}}, cl.get_stripe_info());

  // IO 2 is the next 8k write, starting at 6k. So we have a 12k write, reading the
  // first and last pages. The first part of this read should be in the cache.
  auto to_read2 = iset_from_vector( {{{4*1024, 4*1024}, {12*1024, 4*1024}}}, cl.get_stripe_info());
  auto to_read2_exec = iset_from_vector( {{{12*1024, 4*1024}}}, cl.get_stripe_info());
  auto to_write2 = iset_from_vector({{{4*1024, 12*1024}}}, cl.get_stripe_info());

  // IO 3 is the next misaligned 4k, very similar to IO 3.
  auto to_read3 = iset_from_vector( {{{12*1024, 4*1024}, {20*1024, 4*1024}}}, cl.get_stripe_info());
  auto to_read3_exec = iset_from_vector( {{{20*1024, 4*1024}}}, cl.get_stripe_info());
  auto to_write3 = iset_from_vector({{{12*1024, 12*1024}}}, cl.get_stripe_info());

  //Perform the first write, which should result in a read.
  optional op1 = cl.cache.prepare(cl.oid, to_read1, to_write1, 22*1024, 22*1024, false,
   [&cl](ECExtentCache::OpRef &op)
   {
     cl.cache_ready(op->get_hoid(), op->get_result());
   });
  cl.cache_execute(*op1);
  ASSERT_EQ(to_read1, cl.active_reads);
  ASSERT_TRUE(cl.results.empty());

  // Submit the second IO.
  optional op2 = cl.cache.prepare(cl.oid, to_read2, to_write2, 22*1024, 22*1024, false,
   [&cl](ECExtentCache::OpRef &op)
   {
     cl.cache_ready(op->get_hoid(), op->get_result());
   });
  cl.cache_execute(*op2);
  // We should still be executing read 1.
  ASSERT_EQ(to_read1, cl.active_reads);
  ASSERT_TRUE(cl.results.empty());

  // Allow the read to complete. We should now have op1 done...
  cl.complete_read();
  ASSERT_EQ(to_read2_exec, cl.active_reads);
  ASSERT_FALSE(cl.results.empty());
  cl.complete_write(*op1);

  // And move on to op3
  optional op3 = cl.cache.prepare(cl.oid, to_read3, to_write3, 22*1024, 22*1024, false,
   [&cl](ECExtentCache::OpRef &op)
   {
     cl.cache_ready(op->get_hoid(), op->get_result());
   });
  cl.cache_execute(*op3);
  // We should still be executing read 1.
  ASSERT_EQ(to_read2_exec, cl.active_reads);
  ASSERT_TRUE(cl.results.empty());

  // Allow the read to complete. We should now have op2 done...
  cl.complete_read();
  ASSERT_EQ(to_read3_exec, cl.active_reads);
  ASSERT_FALSE(cl.results.empty());
  cl.complete_write(*op2);
  ASSERT_EQ(to_read3_exec, cl.active_reads);
  ASSERT_TRUE(cl.results.empty());
  cl.complete_read();
  ASSERT_FALSE(cl.results.empty());
  cl.complete_write(*op3);

}

TEST(ECExtentCache, test_invalidate)
{
  Client cl(256*1024, 2, 1, 1024*1024);

  /* First attempt a write which does not do any reads */
  {
    auto to_read1 = iset_from_vector( {{{0, 4096}}}, cl.get_stripe_info());
    auto to_write1 = iset_from_vector({{{0, 4096}}}, cl.get_stripe_info());
    optional op1 = cl.cache.prepare(cl.oid, to_read1, to_write1, 4096, 4096, false,
      [&cl](ECExtentCache::OpRef &op)
      {
        cl.cache_ready(op->get_hoid(), op->get_result());
      });
    cl.cache_execute(*op1);
    ASSERT_EQ(to_read1, cl.active_reads);
    ASSERT_TRUE(cl.results.empty());

    /* Now perform an invalidating cache write */
    optional op2 = cl.cache.prepare(cl.oid, nullopt, shard_extent_set_t(cl.sinfo.get_k_plus_m()), 4*1024, 0, false,
      [&cl](ECExtentCache::OpRef &op)
      {
        cl.cache_ready(op->get_hoid(), op->get_result());
      });
    cl.cache_execute(*op2);

    cl.complete_read();
    ASSERT_EQ(2, cl.results.size());
    auto result = cl.results.begin();
    ASSERT_FALSE(result++->empty());
    ASSERT_TRUE(result++->empty());

    cl.complete_write(*op1);
    ASSERT_FALSE(cl.active_reads);
    cl.complete_write(*op2);

    cl.cache.on_change();
    op1.reset();
    op2.reset();
    cl.cache.on_change2();
    ASSERT_TRUE(cl.cache.idle());
  }

  /* Second test, modifies, deletes, creates, then modifies.  */
  {
    auto to_read1 = iset_from_vector( {{{0, 8192}}}, cl.get_stripe_info());
    auto to_write1 = iset_from_vector({{{0, 8192}}}, cl.get_stripe_info());
    auto to_write2 = iset_from_vector({{{4096, 4096}}}, cl.get_stripe_info());
    auto to_read3 = iset_from_vector( {{{0, 4096}}}, cl.get_stripe_info());
    auto to_write3 = iset_from_vector({{{0, 4096}}}, cl.get_stripe_info());
    optional op1 = cl.cache.prepare(cl.oid, to_read1, to_write1, 8192, 8192, false,
      [&cl](ECExtentCache::OpRef &op)
      {
        cl.cache_ready(op->get_hoid(), op->get_result());
      });
    optional op2 = cl.cache.prepare(cl.oid, nullopt, shard_extent_set_t(cl.sinfo.get_k_plus_m()), 4*1024, 0, false,
      [&cl](ECExtentCache::OpRef &op)
      {
        cl.cache_ready(op->get_hoid(), op->get_result());
      });
    optional op3 = cl.cache.prepare(cl.oid, nullopt, to_write2, 0, 8192, false,
      [&cl](ECExtentCache::OpRef &op)
      {
        cl.cache_ready(op->get_hoid(), op->get_result());
      });
    optional op4 = cl.cache.prepare(cl.oid, to_read3, to_write3, 8192, 8192, false,
      [&cl](ECExtentCache::OpRef &op)
      {
        cl.cache_ready(op->get_hoid(), op->get_result());
      });
    cl.cache_execute(*op1);
    cl.cache_execute(*op2);
    cl.cache_execute(*op3);
    cl.cache_execute(*op4);

    /* The first result must actually read. */
    cl.complete_read();
    ASSERT_EQ(4, cl.results.size());
    auto result = cl.results.begin();
    ASSERT_FALSE(result++->empty());
    ASSERT_TRUE(result++->empty());
    ASSERT_TRUE(result++->empty());
    ASSERT_FALSE(result++->empty());
    cl.complete_write(*op1);
    cl.complete_write(*op2);
    cl.complete_write(*op3);
    cl.complete_write(*op4);

    cl.cache.on_change();
    op1.reset();
    op2.reset();
    op3.reset();
    op4.reset();
    cl.cache.on_change2();
    ASSERT_TRUE(cl.cache.idle());
  }
}

TEST(ECExtentCache, test_invalidate_lru)
{
  uint64_t c = 4096;
  int k = 4;
  int m = 2;
  Client cl(c, k, m, 1024*c);

  /* Populate the cache LRU and then invalidate the cache. */
  {
    uint64_t bs = 3767;
    auto io1 = iset_from_vector({{{align_prev(35*bs), align_next(36*bs) - align_prev(35*bs)}}}, cl.get_stripe_info());
    io1[shard_id_t(k)].insert(io1.get_extent_superset());
    io1[shard_id_t(k+1)].insert(io1.get_extent_superset());
    auto io2 = iset_from_vector({{{align_prev(18*bs), align_next(19*bs) - align_prev(18*bs)}}}, cl.get_stripe_info());
    io2[shard_id_t(k)].insert(io1.get_extent_superset());
    io2[shard_id_t(k+1)].insert(io1.get_extent_superset());
    // io 3 is the truncate (This does the invalidate)
    auto io3 = shard_extent_set_t(cl.sinfo.get_k_plus_m());
    auto io4 = iset_from_vector({{{align_prev(30*bs), align_next(31*bs) - align_prev(30*bs)}}}, cl.get_stripe_info());
    io3[shard_id_t(k)].insert(io1.get_extent_superset());
    io3[shard_id_t(k+1)].insert(io1.get_extent_superset());
    auto io5 = iset_from_vector({{{align_prev(18*bs), align_next(19*bs) - align_prev(18*bs)}}}, cl.get_stripe_info());
    io4[shard_id_t(k)].insert(io1.get_extent_superset());
    io4[shard_id_t(k+1)].insert(io1.get_extent_superset());

    optional op1 = cl.cache.prepare(cl.oid, nullopt, io1, 0, align_next(36*bs), false,
      [&cl](ECExtentCache::OpRef &op)
      {
        cl.cache_ready(op->get_hoid(), op->get_result());
      });

    cl.cache_execute(*op1);
    ASSERT_FALSE(cl.active_reads);
    cl.complete_write(*op1);
    op1.reset();

    optional op2 = cl.cache.prepare(cl.oid, io2, io2, align_next(36*bs), align_next(36*bs), false,
      [&cl](ECExtentCache::OpRef &op)
      {
        cl.cache_ready(op->get_hoid(), op->get_result());
      });
    cl.cache_execute(*op2);
    // We have active reads because the object was discarded fro the cache
    // and has forgotten about all the zero reads.
    ASSERT_TRUE(cl.active_reads);
    cl.complete_read();
    cl.complete_write(*op2);
    op2.reset();

    optional op3 = cl.cache.prepare(cl.oid, nullopt, io3, align_next(36*bs), 0, true,
      [&cl](ECExtentCache::OpRef &op)
      {
        cl.cache_ready(op->get_hoid(), op->get_result());
      });
    cl.cache_execute(*op3);
    ASSERT_FALSE(cl.active_reads);
    cl.complete_write(*op3);
    op3.reset();

    optional op4 = cl.cache.prepare(cl.oid, nullopt, io4, 0, align_next(30*bs), false,
      [&cl](ECExtentCache::OpRef &op)
      {
        cl.cache_ready(op->get_hoid(), op->get_result());
      });
    cl.cache_execute(*op4);
    ASSERT_FALSE(cl.active_reads);
    cl.complete_write(*op4);
    op4.reset();

    optional op5 = cl.cache.prepare(cl.oid, io5, io5, align_next(30*bs), align_next(30*bs), false,
      [&cl](ECExtentCache::OpRef &op)
      {
        cl.cache_ready(op->get_hoid(), op->get_result());
      });
    cl.cache_execute(*op5);
    ASSERT_TRUE(cl.active_reads);
    cl.complete_write(*op5);
    op5.reset();
  }
}

struct MultiClient : public ECExtentCache::BackendReadListener
{
  hobject_t oid_x = hobject_t().make_temp_hobject("Object X");
  hobject_t oid_y = hobject_t().make_temp_hobject("Object Y");
  stripe_info_t sinfo;
  ECExtentCache::LRU lru;
  ECExtentCache cache;

  map<hobject_t, optional<shard_extent_set_t>> active_reads;
  map<hobject_t, uint64_t> last_read_object_size;
  list<shard_extent_map_t> results;

  MultiClient(uint64_t chunk_size, int k, int m, uint64_t cache_size) :
    sinfo(k, m, k*chunk_size, vector<shard_id_t>(0)),
    lru(cache_size), cache(*this, lru, sinfo, g_ceph_context) {};

  void backend_read(hobject_t _oid, const shard_extent_set_t& request,
    uint64_t object_size) override  {
    active_reads[_oid].emplace(request);
    last_read_object_size[_oid] = object_size;
  }

  void cache_ready(const hobject_t& _oid, const shard_extent_map_t& _result)
  {
    results.emplace_back(_result);
  }

  void complete_read(const hobject_t &oid)
  {
    auto it = active_reads.find(oid);
    ceph_assert(it != active_reads.end() && it->second);
    auto reads_done = imap_from_iset(*it->second, &sinfo);
    it->second.reset();
    cache.read_done(oid, std::move(reads_done));
  }

  void complete_write(ECExtentCache::OpRef &op)
  {
    shard_extent_map_t emap = imap_from_iset(op->get_writes(), &sinfo);
    emap.insert_parity_buffers();
    results.clear();
    cache.write_done(op, std::move(emap));
  }

  void cache_execute(ECExtentCache::OpRef &op)
  {
    list<ECExtentCache::OpRef> l;
    l.emplace_back(op);
    cache.execute(l);
  }

  const stripe_info_t *get_stripe_info() const { return &sinfo; }
};

// Reproduces the production crash: ceph_assert(reads_sent) in do_read_op.
//
// The crash path goes through submit_transaction → execute → request →
// send_reads and involves a clone (snap write) creating TWO cache ops in
// one execute() call:
//
//  (a) Clone target (snap oid): invalidates_cache=true, current_size=0,
//      no reads, no writes.  Simulates a clone of HEAD → snap.
//  (b) HEAD: a write needing RMW reads, which keeps pending_cache_ops > 0
//      so write_done never fires for the clone target.
//
// cache_maybe_ready pops the clone target (read_done=true, invalidation
// fires).  invalidate() clears do_not_read and pre-sets projected_size =
// op->projected_size BEFORE replaying — so the growth hole from 0 →
// projected_size is never re-added.  The clone target Object now has
// current_size=0 and empty do_not_read.
//
// A new op on the clone oid (via submit_transaction → execute) computes
// reads with orig_size = projected_size (large), which survive the empty
// do_not_read.  send_reads fires with current_size=0.  In production,
// get_min_avail_to_read_shards returns 0 without populating shard_reads,
// and do_read_op hits ceph_assert(reads_sent).
TEST(ECExtentCache, CloneInvalidateStaleSize)
{
  MultiClient cl(4096, 2, 1, 1024*1024);
  const auto *si = cl.get_stripe_info();
  uint64_t head_size = 8192;

  // Step 1: Object X blocks the front of waiting_ops with reads.
  auto x_read = iset_from_vector({{{0, 4096}}, {{0, 4096}}}, si);
  auto x_write = iset_from_vector({{{0, 4096}}, {{0, 4096}}}, si);

  optional op_x = cl.cache.prepare(cl.oid_x, x_read, x_write, 4096, 4096,
    false,
    [&cl](ECExtentCache::OpRef &op) {
      cl.cache_ready(op->get_hoid(), op->get_result());
    });
  cl.cache_execute(*op_x);
  ASSERT_TRUE(cl.active_reads[cl.oid_x].has_value());

  // Step 2: Simulate a clone+write in a single execute() call, exactly as
  // start_rmw would do.  Two ops: clone target Y (invalidates, no reads/
  // writes, current_size=0) and HEAD X2 (needs reads, keeps things pending).
  //
  // Clone target: orig_size=0 (snap doesn't exist), projected_size=head_size,
  // invalidates_cache=true.  No reads, no writes.
  optional op_clone = cl.cache.prepare(cl.oid_y, nullopt,
    iset_from_vector({{}, {}}, si),
    0, head_size, true,
    [&cl](ECExtentCache::OpRef &op) {
      cl.cache_ready(op->get_hoid(), op->get_result());
    });

  // HEAD write: needs reads (keeps pending_cache_ops > 0 for the RMW Op
  // so write_done never fires for the clone target).
  // We use a SECOND op on X to simulate the HEAD write in the same
  // transaction.  The reads keep this op pending.
  auto x2_read = iset_from_vector({{{0, 4096}}, {}}, si);
  auto x2_write = iset_from_vector({{{0, 4096}}, {}}, si);
  optional op_x2 = cl.cache.prepare(cl.oid_x, x2_read, x2_write,
    4096, 4096, false,
    [&cl](ECExtentCache::OpRef &op) {
      cl.cache_ready(op->get_hoid(), op->get_result());
    });

  // Execute both together, as start_rmw does.
  {
    list<ECExtentCache::OpRef> l;
    l.emplace_back(*op_clone);
    l.emplace_back(*op_x2);
    cl.cache.execute(l);
  }

  // cache_maybe_ready processes:
  //  - X is at front, reads outstanding → stops (from step 1)
  // But the clone target was added AFTER X in waiting_ops, so X blocks
  // everything.  The clone target's invalidation has NOT fired yet.
  //
  // Complete X's reads so cache_maybe_ready can advance.
  cl.complete_read(cl.oid_x);

  // Now cache_maybe_ready:
  //  - X completes → write_done, popped
  //  - Clone target: invalidates_cache=true, reading=false → invalidate()!
  //    invalidate clears do_not_read, sets projected_size=head_size,
  //    replays clone target (no reads/writes, growth hole NOT re-added
  //    because projected_size already == op->projected_size)
  //  - Clone target: complete_if_reads_cached → callback fires
  //  - X2: reads outstanding (from execute) → cache_maybe_ready stops
  //
  // Clone target Object now has:
  //   current_size = 0  (write_done never called — pending_cache_ops > 0)
  //   projected_size = head_size
  //   do_not_read = EMPTY (invalidation cleared it, growth hole not re-added)

  // Step 3: A new op on the clone oid arrives (e.g. snap trim or CLS op).
  // submit_transaction → execute → request → send_reads.
  auto y_read = iset_from_vector({{{0, 4096}}, {}}, si);
  auto y_write = iset_from_vector({{{0, 4096}}, {}}, si);

  optional op_y = cl.cache.prepare(cl.oid_y, y_read, y_write,
    head_size, head_size, false,
    [&cl](ECExtentCache::OpRef &op) {
      cl.cache_ready(op->get_hoid(), op->get_result());
    });
  cl.cache_execute(*op_y);

  // With the fix, invalidate() sets projected_size = current_size (0
  // for a clone target).  During replay, the growth hole from 0 ->
  // projected_size is re-added to do_not_read, so the reads are subtracted
  // and no backend_read is issued.
  //
  // Without the fix, the growth hole is NOT re-added, reads survive
  // do_not_read, and send_reads fires with current_size=0 — which in
  // production causes ceph_assert(reads_sent).
  ASSERT_FALSE(cl.active_reads[cl.oid_y].has_value());

  // Clean up: op_x2's reads may have been covered by op_x's do_not_read,
  // so only complete the read if it was actually issued.
  if (cl.active_reads[cl.oid_x].has_value()) {
    cl.complete_read(cl.oid_x);
  }
  cl.complete_write(*op_x);
  cl.complete_write(*op_clone);
  cl.complete_write(*op_x2);
  cl.complete_write(*op_y);
}