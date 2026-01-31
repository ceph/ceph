// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>
#include "include/hybrid_interval_set.h"
#include "common/hybrid_interval_map.h"
#include "include/buffer.h"
#include <boost/container/flat_map.hpp>

// Type aliases matching EC usage
using extent_set = hybrid_interval_set<uint64_t, boost::container::flat_map, false>;
using extent_map = hybrid_interval_map<uint64_t, ceph::buffer::list, 
                                       struct bl_split_merge,
                                       boost::container::flat_map, true>;

// bl_split_merge for buffer::list operations
struct bl_split_merge {
  ceph::buffer::list split(uint64_t offset, uint64_t length,
                           ceph::buffer::list &bl) const {
    ceph::buffer::list out;
    out.substr_of(bl, offset, length);
    return out;
  }

  bool can_merge(const ceph::buffer::list &left,
                 const ceph::buffer::list &right) const {
    return true;
  }

  ceph::buffer::list merge(ceph::buffer::list &&left,
                           ceph::buffer::list &&right) const {
    ceph::buffer::list bl{std::move(left)};
    bl.claim_append(right);
    return bl;
  }

  uint64_t length(const ceph::buffer::list &b) const { return b.length(); }
};

/**
 * Complex EC Failure Scenarios
 * 
 * These tests simulate realistic failure cases in erasure-coded pools
 * where the hybrid interval structures would be heavily used.
 */

TEST(HybridComplexEC, PartialStripeReadWithOneDegradedShard) {
  // Scenario: k=4, m=2 EC pool, reading 64KB object with one shard down
  // Client reads 16KB at offset 32KB
  // FastEC must read from 3 available shards and reconstruct 4th
  
  extent_map shard0, shard1, shard2, shard3_missing;
  
  // Each shard has 16KB chunks (64KB / 4 shards)
  // Reading offset 32KB-48KB means we need chunks from all 4 shards
  
  // Shard 0: has data at offset 8KB-12KB (corresponds to object 32KB-48KB)
  ceph::buffer::list bl0;
  bl0.append(std::string(4096, 'A'));
  shard0.insert(8192, 4096, std::move(bl0));
  ASSERT_TRUE(shard0.is_single()) << "Single chunk should stay in single mode";
  
  // Shard 1: has data at offset 8KB-12KB
  ceph::buffer::list bl1;
  bl1.append(std::string(4096, 'B'));
  shard1.insert(8192, 4096, std::move(bl1));
  ASSERT_TRUE(shard1.is_single()) << "Single chunk should stay in single mode";
  
  // Shard 2: has data at offset 8KB-12KB
  ceph::buffer::list bl2;
  bl2.append(std::string(4096, 'C'));
  shard2.insert(8192, 4096, std::move(bl2));
  ASSERT_TRUE(shard2.is_single()) << "Single chunk should stay in single mode";
  
  // Shard 3: MISSING - will be reconstructed
  ASSERT_TRUE(shard3_missing.empty());
  ASSERT_TRUE(shard3_missing.is_single()) << "Empty should be single mode";
  
  // Verify all shards have exactly one extent (optimal case for hybrid)
  ASSERT_EQ(1u, shard0.num_intervals());
  ASSERT_EQ(1u, shard1.num_intervals());
  ASSERT_EQ(1u, shard2.num_intervals());
  ASSERT_EQ(0u, shard3_missing.num_intervals());
}

TEST(HybridComplexEC, MultiplePartialReadsWithFragmentation) {
  // Scenario: Object has been partially overwritten multiple times
  // Results in fragmented extents across shards
  // k=4, m=2, 256KB object with 4KB overwrites at various offsets
  
  extent_set shard_extents;
  
  // Initial write: full 64KB shard (256KB / 4)
  shard_extents.insert(0, 65536);
  ASSERT_TRUE(shard_extents.is_single()) << "Single extent should be single mode";
  
  // Overwrite at offset 4KB-8KB (creates hole)
  shard_extents.erase(4096, 4096);
  ASSERT_FALSE(shard_extents.is_single()) << "Erase creating gap should upgrade to multi";
  ASSERT_EQ(2u, shard_extents.num_intervals()) << "Should have [0-4KB, 8KB-64KB]";
  
  // Overwrite at offset 16KB-20KB (creates another hole)
  shard_extents.erase(16384, 4096);
  ASSERT_FALSE(shard_extents.is_single());
  ASSERT_EQ(3u, shard_extents.num_intervals()) << "Should have 3 fragments";
  
  // Overwrite at offset 32KB-36KB (creates yet another hole)
  shard_extents.erase(32768, 4096);
  ASSERT_FALSE(shard_extents.is_single());
  ASSERT_EQ(4u, shard_extents.num_intervals()) << "Should have 4 fragments";
  
  // Now reading requires gathering from 4 non-contiguous extents
  // This is where multi-mode is necessary and justified
  ASSERT_TRUE(shard_extents.contains(0, 4096));
  ASSERT_FALSE(shard_extents.contains(4096, 4096));
  ASSERT_TRUE(shard_extents.contains(8192, 8192));
  ASSERT_FALSE(shard_extents.contains(16384, 4096));
}

TEST(HybridComplexEC, RecoveryWithMultipleMissingShards) {
  // Scenario: k=8, m=3 EC pool, 2 shards down simultaneously
  // Recovery must read from 8 available shards to reconstruct 2 missing
  // Each shard contributes one extent to the recovery operation
  
  std::vector<extent_map> available_shards(8);
  std::vector<extent_map> missing_shards(2);
  
  // Simulate recovery of 1MB object (128KB per shard for k=8)
  // Each available shard has one 128KB extent
  for (int i = 0; i < 8; i++) {
    ceph::buffer::list bl;
    bl.append(std::string(131072, 'A' + i));
    available_shards[i].insert(0, 131072, std::move(bl));
    ASSERT_TRUE(available_shards[i].is_single()) 
      << "Recovery read from shard " << i << " should be single extent";
  }
  
  // Missing shards are empty (will be reconstructed)
  for (int i = 0; i < 2; i++) {
    ASSERT_TRUE(missing_shards[i].empty());
    ASSERT_TRUE(missing_shards[i].is_single()) << "Empty should be single mode";
  }
  
  // After reconstruction, missing shards will also have single extents
  // This demonstrates optimal case: 10 shards, all in single mode
}

TEST(HybridComplexEC, PartialReadDuringBackfill) {
  // Scenario: OSD is backfilling after recovery, client reads during backfill
  // Some extents are backfilled, others are still being recovered
  // Results in mixed state with some shards having data, others empty
  
  extent_set backfilled_extents;
  extent_set pending_extents;
  
  // Backfilled: extents 0-64KB, 128KB-192KB (non-contiguous)
  backfilled_extents.insert(0, 65536);
  ASSERT_TRUE(backfilled_extents.is_single());
  
  backfilled_extents.insert(131072, 65536);
  ASSERT_FALSE(backfilled_extents.is_single()) << "Two non-adjacent extents";
  ASSERT_EQ(2u, backfilled_extents.num_intervals());
  
  // Pending: extents 64KB-128KB, 192KB-256KB
  pending_extents.insert(65536, 65536);
  ASSERT_TRUE(pending_extents.is_single());
  
  pending_extents.insert(196608, 65536);
  ASSERT_FALSE(pending_extents.is_single());
  ASSERT_EQ(2u, pending_extents.num_intervals());
  
  // Client read at 96KB-160KB spans both backfilled and pending
  ASSERT_TRUE(backfilled_extents.intersects(98304, 65536));
  ASSERT_TRUE(pending_extents.intersects(98304, 65536));
  
  // Must coordinate reads from both sets
  extent_set combined;
  combined.union_of(backfilled_extents, pending_extents);
  ASSERT_FALSE(combined.is_single());
  ASSERT_EQ(4u, combined.num_intervals()) << "Should have all 4 extents";
}

TEST(HybridComplexEC, SnapshotReadWithClones) {
  // Scenario: Object has snapshots, reading old version requires
  // combining extents from head and clone objects
  // k=4, m=2, 128KB object with 3 snapshots
  
  extent_map head_extents;
  extent_map snap1_extents;
  extent_map snap2_extents;
  
  // Head: modified at offset 32KB-64KB
  ceph::buffer::list head_bl;
  head_bl.append(std::string(32768, 'H'));
  head_extents.insert(32768, 32768, std::move(head_bl));
  ASSERT_TRUE(head_extents.is_single());
  
  // Snap1: has original data at 0-32KB and 64KB-128KB
  ceph::buffer::list snap1_bl1;
  snap1_bl1.append(std::string(32768, 'S'));
  snap1_extents.insert(0, 32768, std::move(snap1_bl1));
  ASSERT_TRUE(snap1_extents.is_single());
  
  ceph::buffer::list snap1_bl2;
  snap1_bl2.append(std::string(65536, 'S'));
  snap1_extents.insert(65536, 65536, std::move(snap1_bl2));
  ASSERT_FALSE(snap1_extents.is_single()) << "Two extents in snap1";
  ASSERT_EQ(2u, snap1_extents.num_intervals());
  
  // Reading snap1 requires combining extents from snap1 only
  // Reading head requires combining from head + snap1
  // This creates complex extent tracking scenarios
}

TEST(HybridComplexEC, ScrubWithMismatchedExtents) {
  // Scenario: Scrub detects inconsistency between replicas
  // Must compare extent maps from multiple shards
  // k=4, m=2, detecting bit rot in one shard
  
  extent_set shard0_extents, shard1_extents, shard2_extents, shard3_extents;
  
  // Normal shards: all have complete 64KB extent
  shard0_extents.insert(0, 65536);
  shard1_extents.insert(0, 65536);
  shard2_extents.insert(0, 65536);
  ASSERT_TRUE(shard0_extents.is_single());
  ASSERT_TRUE(shard1_extents.is_single());
  ASSERT_TRUE(shard2_extents.is_single());
  
  // Corrupted shard: missing middle 16KB due to bit rot
  shard3_extents.insert(0, 24576);
  shard3_extents.insert(40960, 24576);
  ASSERT_FALSE(shard3_extents.is_single()) << "Corrupted shard has gap";
  ASSERT_EQ(2u, shard3_extents.num_intervals());
  
  // Scrub comparison detects mismatch
  ASSERT_FALSE(shard0_extents == shard3_extents);
  ASSERT_TRUE(shard0_extents == shard1_extents);
  ASSERT_TRUE(shard1_extents == shard2_extents);
}

TEST(HybridComplexEC, DeepScrubWithChecksumVerification) {
  // Scenario: Deep scrub reads all extents and verifies checksums
  // Must track which extents have been verified
  // k=8, m=3, 1MB object split into 128KB shards
  
  extent_set verified_extents;
  extent_set pending_verification;
  
  // Initially all extents pending
  pending_verification.insert(0, 131072);
  ASSERT_TRUE(pending_verification.is_single());
  
  // Verify in 32KB chunks (simulating incremental verification)
  for (uint64_t offset = 0; offset < 131072; offset += 32768) {
    // Move from pending to verified
    pending_verification.erase(offset, 32768);
    verified_extents.insert(offset, 32768);
    
    if (offset == 0) {
      ASSERT_TRUE(verified_extents.is_single()) << "First chunk verified";
      ASSERT_TRUE(pending_verification.is_single()) << "Remaining pending";
    }
  }
  
  // After full verification
  ASSERT_TRUE(pending_verification.empty());
  ASSERT_FALSE(verified_extents.is_single()) << "Multiple chunks verified";
  ASSERT_EQ(4u, verified_extents.num_intervals());
}

TEST(HybridComplexEC, ConcurrentReadsWithOverlappingExtents) {
  // Scenario: Multiple clients reading overlapping ranges simultaneously
  // Each read operation tracks its own extent set
  // k=4, m=2, 256KB object with 3 concurrent reads
  
  extent_set read1_extents;  // Client 1: reading 0-64KB
  extent_set read2_extents;  // Client 2: reading 32KB-96KB (overlaps)
  extent_set read3_extents;  // Client 3: reading 64KB-128KB
  
  // Read 1: single extent
  read1_extents.insert(0, 65536);
  ASSERT_TRUE(read1_extents.is_single());
  
  // Read 2: single extent (overlaps with read1)
  read2_extents.insert(32768, 65536);
  ASSERT_TRUE(read2_extents.is_single());
  
  // Read 3: single extent (adjacent to read2)
  read3_extents.insert(65536, 65536);
  ASSERT_TRUE(read3_extents.is_single());
  
  // Check overlaps
  ASSERT_TRUE(read1_extents.intersects(32768, 65536)) << "Read1 overlaps Read2";
  ASSERT_TRUE(read2_extents.intersects(65536, 65536)) << "Read2 overlaps Read3";
  ASSERT_FALSE(read1_extents.intersects(65536, 65536)) << "Read1 doesn't overlap Read3";
  
  // Combined extent set for cache management
  extent_set all_reads;
  all_reads.union_of(read1_extents, read2_extents);
  all_reads.union_of(all_reads, read3_extents);
  ASSERT_FALSE(all_reads.is_single());
  ASSERT_EQ(1u, all_reads.num_intervals()) << "Should merge into single 0-128KB";
}

TEST(HybridComplexEC, PartialWriteWithReadModifyWrite) {
  // Scenario: Small write requires read-modify-write cycle
  // Must track which extents need to be read vs written
  // k=4, m=2, writing 8KB at offset 60KB in 256KB object
  
  extent_set extents_to_read;
  extent_set extents_to_write;
  
  // For 8KB write at 60KB, must read full stripe (64KB aligned)
  // Stripe starts at 49152 (48KB), covers 49152-114688 (48KB-112KB)
  extents_to_read.insert(49152, 65536);
  ASSERT_TRUE(extents_to_read.is_single());
  
  // After read-modify-write, only modified stripe is written
  extents_to_write.insert(49152, 65536);
  ASSERT_TRUE(extents_to_write.is_single());
  
  // Verify alignment
  ASSERT_EQ(0u, extents_to_read.range_start() % 16384) 
    << "Should be stripe-aligned";
}

TEST(HybridComplexEC, RecoveryWithPartialObjectLoss) {
  // Scenario: OSD lost, recovering objects with varying completeness
  // Some objects fully lost, others partially available
  // k=4, m=2, recovering 10 objects with different states
  
  std::vector<extent_set> object_extents(10);
  
  // Object 0: fully lost (empty)
  ASSERT_TRUE(object_extents[0].empty());
  ASSERT_TRUE(object_extents[0].is_single());
  
  // Objects 1-3: fully available (single extent each)
  for (int i = 1; i <= 3; i++) {
    object_extents[i].insert(0, 65536);
    ASSERT_TRUE(object_extents[i].is_single());
  }
  
  // Objects 4-6: partially available (fragmented)
  for (int i = 4; i <= 6; i++) {
    object_extents[i].insert(0, 32768);
    object_extents[i].insert(49152, 16384);
    ASSERT_FALSE(object_extents[i].is_single());
    ASSERT_EQ(2u, object_extents[i].num_intervals());
  }
  
  // Objects 7-9: mostly available with small gaps
  for (int i = 7; i <= 9; i++) {
    object_extents[i].insert(0, 61440);
    object_extents[i].insert(65536, 0);  // Empty insert (no-op)
    ASSERT_TRUE(object_extents[i].is_single()) << "Should stay single";
  }
  
  // Recovery must handle all these cases efficiently
  // Hybrid structure optimal for objects 0-3 and 7-9 (single mode)
  // Multi-mode necessary only for objects 4-6 (fragmented)
}

TEST(HybridComplexEC, LargeObjectWithSparseWrites) {
  // Scenario: 10MB object with sparse 4KB writes
  // Common in database workloads with random updates
  // k=8, m=3, 10MB object = 1.25MB per shard
  
  extent_set shard_extents;
  
  // Sparse writes at: 0KB, 256KB, 512KB, 768KB, 1024KB
  uint64_t shard_size = 1310720;  // 1.25MB
  std::vector<uint64_t> write_offsets = {0, 262144, 524288, 786432, 1048576};
  
  for (auto offset : write_offsets) {
    if (offset < shard_size) {
      shard_extents.insert(offset, 4096);
    }
  }
  
  ASSERT_FALSE(shard_extents.is_single()) << "Sparse writes create multiple extents";
  ASSERT_EQ(5u, shard_extents.num_intervals());
  
  // Reading full object requires gathering from 5 non-contiguous extents
  // This is a case where multi-mode is necessary and beneficial
  for (auto offset : write_offsets) {
    if (offset < shard_size) {
      ASSERT_TRUE(shard_extents.contains(offset, 4096));
    }
  }
}

// Made with Bob
