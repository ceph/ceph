// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 Contributors
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <gtest/gtest.h>
#include "osd/ObjectPack.h"
#include "include/buffer.h"
#include <sstream>

using namespace ceph::osd;
using namespace std;

// Helper functions for creating test objects
static hobject_t mk_obj(unsigned id, int64_t pool = 1) {
  hobject_t hoid;
  stringstream ss;
  ss << "obj_" << id;
  hoid.oid = ss.str();
  hoid.set_hash(id);
  hoid.pool = pool;
  hoid.snap = CEPH_NOSNAP;
  return hoid;
}

static ceph::buffer::list mk_data(size_t size, char fill = 'x') {
  ceph::buffer::list bl;
  string data(size, fill);
  bl.append(data);
  return bl;
}

// ============================================================================
// PackedObjectInfo Tests
// ============================================================================

TEST(ObjectPack, PackedObjectInfo_Basic) {
  hobject_t container = mk_obj(100);
  PackedObjectInfo info(container, 1024, 512);
  
  ASSERT_TRUE(info.is_packed());
  ASSERT_EQ(container, info.container_object_id);
  ASSERT_EQ(1024u, info.offset);
  ASSERT_EQ(512u, info.length);
}

TEST(ObjectPack, PackedObjectInfo_NotPacked) {
  PackedObjectInfo info;
  ASSERT_FALSE(info.is_packed());
}

TEST(ObjectPack, PackedObjectInfo_Encoding) {
  hobject_t container = mk_obj(100);
  PackedObjectInfo info1(container, 2048, 1024);
  
  ceph::buffer::list bl;
  info1.encode(bl);
  
  auto p = bl.cbegin();
  PackedObjectInfo info2;
  info2.decode(p);
  
  ASSERT_EQ(info1.container_object_id, info2.container_object_id);
  ASSERT_EQ(info1.offset, info2.offset);
  ASSERT_EQ(info1.length, info2.length);
}

// ============================================================================
// ContainerReverseMapEntry Tests
// ============================================================================

TEST(ObjectPack, ContainerReverseMapEntry_Basic) {
  hobject_t logical = mk_obj(1);
  ContainerReverseMapEntry entry(logical, 512, false);
  
  ASSERT_EQ(logical, entry.logical_object_id);
  ASSERT_EQ(512u, entry.length);
  ASSERT_FALSE(entry.is_garbage);
}

TEST(ObjectPack, ContainerReverseMapEntry_Garbage) {
  hobject_t logical = mk_obj(2);
  ContainerReverseMapEntry entry(logical, 1024, true);
  
  ASSERT_TRUE(entry.is_garbage);
}

TEST(ObjectPack, ContainerReverseMapEntry_Encoding) {
  hobject_t logical = mk_obj(3);
  ContainerReverseMapEntry entry1(logical, 2048, false);
  
  ceph::buffer::list bl;
  entry1.encode(bl);
  
  auto p = bl.cbegin();
  ContainerReverseMapEntry entry2;
  entry2.decode(p);
  
  ASSERT_EQ(entry1.logical_object_id, entry2.logical_object_id);
  ASSERT_EQ(entry1.length, entry2.length);
  ASSERT_EQ(entry1.is_garbage, entry2.is_garbage);
}

// ============================================================================
// ContainerInfo Tests
// ============================================================================

TEST(ObjectPack, ContainerInfo_Basic) {
  hobject_t container = mk_obj(100);
  ContainerInfo info(container, 4 * 1024 * 1024);
  
  ASSERT_EQ(container, info.container_id);
  ASSERT_EQ(4 * 1024 * 1024u, info.total_size);
  ASSERT_EQ(0u, info.used_bytes);
  ASSERT_EQ(0u, info.garbage_bytes);
  ASSERT_EQ(0u, info.next_offset);
  ASSERT_FALSE(info.is_sealed);
  ASSERT_EQ(0.0, info.fragmentation_ratio());
  ASSERT_EQ(4 * 1024 * 1024u, info.available_space());
}

TEST(ObjectPack, ContainerInfo_Fragmentation) {
  hobject_t container = mk_obj(100);
  ContainerInfo info(container, 4 * 1024 * 1024);
  
  info.used_bytes = 1024 * 1024;
  info.garbage_bytes = 256 * 1024;
  
  // Fragmentation = garbage / (used + garbage)
  double expected = 256.0 * 1024 / (1024 * 1024 + 256 * 1024);
  ASSERT_NEAR(expected, info.fragmentation_ratio(), 0.001);
}

TEST(ObjectPack, ContainerInfo_AvailableSpace) {
  hobject_t container = mk_obj(100);
  ContainerInfo info(container, 1024 * 1024);
  
  info.next_offset = 512 * 1024;
  ASSERT_EQ(512 * 1024u, info.available_space());
  
  info.next_offset = 1024 * 1024;
  ASSERT_EQ(0u, info.available_space());
  
  info.next_offset = 2 * 1024 * 1024; // Overflow case
  ASSERT_EQ(0u, info.available_space());
}

TEST(ObjectPack, ContainerInfo_ReverseMap) {
  hobject_t container = mk_obj(100);
  ContainerInfo info(container, 4 * 1024 * 1024);
  
  hobject_t obj1 = mk_obj(1);
  hobject_t obj2 = mk_obj(2);
  
  info.reverse_map[0] = ContainerReverseMapEntry(obj1, 1024, false);
  info.reverse_map[2048] = ContainerReverseMapEntry(obj2, 512, false);
  
  ASSERT_EQ(2u, info.reverse_map.size());
  ASSERT_EQ(obj1, info.reverse_map[0].logical_object_id);
  ASSERT_EQ(obj2, info.reverse_map[2048].logical_object_id);
}

TEST(ObjectPack, ContainerInfo_Encoding) {
  hobject_t container = mk_obj(100);
  ContainerInfo info1(container, 4 * 1024 * 1024);
  info1.used_bytes = 1024;
  info1.garbage_bytes = 512;
  info1.next_offset = 2048;
  info1.is_sealed = true;
  
  hobject_t obj1 = mk_obj(1);
  info1.reverse_map[0] = ContainerReverseMapEntry(obj1, 1024, false);
  
  ceph::buffer::list bl;
  info1.encode(bl);
  
  auto p = bl.cbegin();
  ContainerInfo info2;
  info2.decode(p);
  
  ASSERT_EQ(info1.container_id, info2.container_id);
  ASSERT_EQ(info1.total_size, info2.total_size);
  ASSERT_EQ(info1.used_bytes, info2.used_bytes);
  ASSERT_EQ(info1.garbage_bytes, info2.garbage_bytes);
  ASSERT_EQ(info1.next_offset, info2.next_offset);
  ASSERT_EQ(info1.is_sealed, info2.is_sealed);
  ASSERT_EQ(info1.reverse_map.size(), info2.reverse_map.size());
}

// ============================================================================
// PackingConfig Tests
// ============================================================================

TEST(ObjectPack, PackingConfig_Defaults) {
  PackingConfig config;
  
  ASSERT_EQ(64 * 1024u, config.small_object_threshold);
  ASSERT_EQ(4 * 1024 * 1024u, config.container_size);
  ASSERT_EQ(64u, config.alignment_small);
  ASSERT_EQ(4096u, config.alignment_large);
  ASSERT_NEAR(0.20, config.gc_fragmentation_threshold, 0.001);
  ASSERT_EQ(256 * 1024u, config.gc_min_garbage_bytes);
}

TEST(ObjectPack, PackingConfig_Alignment) {
  PackingConfig config;
  
  // Small objects use 64-byte alignment
  ASSERT_EQ(64u, config.get_alignment(1024));
  ASSERT_EQ(64u, config.get_alignment(2047));
  
  // Large objects use 4KB alignment
  ASSERT_EQ(4096u, config.get_alignment(2048));
  ASSERT_EQ(4096u, config.get_alignment(32 * 1024));
}

TEST(ObjectPack, PackingConfig_AlignOffset) {
  PackingConfig config;
  
  // 64-byte alignment
  ASSERT_EQ(0u, config.align_offset(0, 64));
  ASSERT_EQ(64u, config.align_offset(1, 64));
  ASSERT_EQ(64u, config.align_offset(63, 64));
  ASSERT_EQ(64u, config.align_offset(64, 64));
  ASSERT_EQ(128u, config.align_offset(65, 64));
  
  // 4KB alignment
  ASSERT_EQ(0u, config.align_offset(0, 4096));
  ASSERT_EQ(4096u, config.align_offset(1, 4096));
  ASSERT_EQ(4096u, config.align_offset(4095, 4096));
  ASSERT_EQ(4096u, config.align_offset(4096, 4096));
  ASSERT_EQ(8192u, config.align_offset(4097, 4096));
}

// ============================================================================
// ObjectPackEngine Tests
// ============================================================================

TEST(ObjectPack, Engine_ShouldPackObject) {
  PackingConfig config;
  config.small_object_threshold = 64 * 1024;
  ObjectPackEngine engine(config);
  
  ASSERT_FALSE(engine.should_pack_object(0));
  ASSERT_TRUE(engine.should_pack_object(1));
  ASSERT_TRUE(engine.should_pack_object(1024));
  ASSERT_TRUE(engine.should_pack_object(64 * 1024));
  ASSERT_FALSE(engine.should_pack_object(64 * 1024 + 1));
  ASSERT_FALSE(engine.should_pack_object(1024 * 1024));
}

TEST(ObjectPack, Engine_PlanWrite_NewContainer) {
  ObjectPackEngine engine;
  
  hobject_t obj = mk_obj(1);
  ceph::buffer::list data = mk_data(1024);
  
  PackResult result = engine.plan_write(obj, data, std::nullopt);
  
  ASSERT_TRUE(result.success);
  ASSERT_EQ(3u, result.operations.size());
  
  // Operation 1: Write to container
  ASSERT_EQ(PackOpType::WRITE_TO_CONTAINER, result.operations[0].type);
  ASSERT_EQ(obj, result.operations[0].logical_object);
  ASSERT_EQ(0u, result.operations[0].offset);
  ASSERT_EQ(1024u, result.operations[0].length);
  
  // Operation 2: Update object OI
  ASSERT_EQ(PackOpType::UPDATE_OBJECT_OI, result.operations[1].type);
  ASSERT_EQ(obj, result.operations[1].logical_object);
  ASSERT_TRUE(result.operations[1].packed_info.has_value());
  ASSERT_EQ(0u, result.operations[1].packed_info->offset);
  ASSERT_EQ(1024u, result.operations[1].packed_info->length);
  
  // Operation 3: Update container OMAP
  ASSERT_EQ(PackOpType::UPDATE_CONTAINER_OMAP, result.operations[2].type);
  ASSERT_EQ(0u, result.operations[2].offset);
  ASSERT_TRUE(result.operations[2].reverse_entry.has_value());
  ASSERT_EQ(obj, result.operations[2].reverse_entry->logical_object_id);
  ASSERT_EQ(1024u, result.operations[2].reverse_entry->length);
  ASSERT_FALSE(result.operations[2].reverse_entry->is_garbage);
}

TEST(ObjectPack, Engine_PlanWrite_ExistingContainer) {
  ObjectPackEngine engine;
  
  hobject_t container = mk_obj(100);
  ContainerInfo container_info(container, 4 * 1024 * 1024);
  container_info.next_offset = 1024;
  
  hobject_t obj = mk_obj(1);
  ceph::buffer::list data = mk_data(512);
  
  PackResult result = engine.plan_write(obj, data, container_info);
  
  ASSERT_TRUE(result.success);
  ASSERT_EQ(3u, result.operations.size());
  
  // Should write at aligned offset after existing data
  ASSERT_EQ(PackOpType::WRITE_TO_CONTAINER, result.operations[0].type);
  ASSERT_EQ(container, result.operations[0].container_object);
  // 512 bytes needs 64-byte alignment, 1024 is already aligned
  ASSERT_EQ(1024u, result.operations[0].offset);
}

TEST(ObjectPack, Engine_PlanWrite_TooLarge) {
  PackingConfig config;
  config.small_object_threshold = 1024;
  ObjectPackEngine engine(config);
  
  hobject_t obj = mk_obj(1);
  ceph::buffer::list data = mk_data(2048);
  
  PackResult result = engine.plan_write(obj, data, std::nullopt);
  
  ASSERT_FALSE(result.success);
  ASSERT_FALSE(result.error_message.empty());
}

TEST(ObjectPack, Engine_PlanRead) {
  ObjectPackEngine engine;
  
  hobject_t obj = mk_obj(1);
  hobject_t container = mk_obj(100);
  PackedObjectInfo packed_info(container, 2048, 1024);
  
  PackResult result = engine.plan_read(obj, packed_info);
  
  ASSERT_TRUE(result.success);
  ASSERT_EQ(1u, result.operations.size());
  
  ASSERT_EQ(PackOpType::READ_FROM_CONTAINER, result.operations[0].type);
  ASSERT_EQ(obj, result.operations[0].logical_object);
  ASSERT_EQ(container, result.operations[0].container_object);
  ASSERT_EQ(2048u, result.operations[0].offset);
  ASSERT_EQ(1024u, result.operations[0].length);
}

TEST(ObjectPack, Engine_PlanRead_NotPacked) {
  ObjectPackEngine engine;
  
  hobject_t obj = mk_obj(1);
  PackedObjectInfo packed_info; // Not packed
  
  PackResult result = engine.plan_read(obj, packed_info);
  
  ASSERT_FALSE(result.success);
}

TEST(ObjectPack, Engine_PlanModify_StaysSmall) {
  ObjectPackEngine engine;
  
  hobject_t obj = mk_obj(1);
  hobject_t old_container = mk_obj(100);
  hobject_t new_container = mk_obj(101);
  
  PackedObjectInfo packed_info(old_container, 1024, 512);
  ContainerInfo container_info(new_container, 4 * 1024 * 1024);
  
  ceph::buffer::list existing_data = mk_data(512, 'a');
  ceph::buffer::list new_data = mk_data(256, 'b');
  
  PackResult result = engine.plan_modify(
    obj, existing_data, new_data, packed_info, container_info);
  
  ASSERT_TRUE(result.success);
  ASSERT_GT(result.operations.size(), 1u);
  
  // First operation should mark old location as garbage
  ASSERT_EQ(PackOpType::MARK_GARBAGE, result.operations[0].type);
  ASSERT_EQ(old_container, result.operations[0].container_object);
  ASSERT_EQ(1024u, result.operations[0].offset);
  ASSERT_EQ(512u, result.operations[0].length);
  
  // Subsequent operations should write to new location
  bool found_write = false;
  for (const auto& op : result.operations) {
    if (op.type == PackOpType::WRITE_TO_CONTAINER) {
      found_write = true;
      ASSERT_EQ(768u, op.length); // 512 + 256
    }
  }
  ASSERT_TRUE(found_write);
}

TEST(ObjectPack, Engine_PlanModify_GrowsTooBig) {
  PackingConfig config;
  config.small_object_threshold = 1024;
  ObjectPackEngine engine(config);
  
  hobject_t obj = mk_obj(1);
  hobject_t container = mk_obj(100);
  PackedObjectInfo packed_info(container, 1024, 512);
  
  ceph::buffer::list existing_data = mk_data(512);
  ceph::buffer::list new_data = mk_data(1024); // Total = 1536, exceeds threshold
  
  PackResult result = engine.plan_modify(
    obj, existing_data, new_data, packed_info, std::nullopt);
  
  ASSERT_TRUE(result.success);
  ASSERT_GE(result.operations.size(), 2u);
  
  // Should promote to standalone
  ASSERT_EQ(PackOpType::PROMOTE_TO_STANDALONE, result.operations[0].type);
  ASSERT_EQ(obj, result.operations[0].logical_object);
  
  // Should mark old location as garbage
  ASSERT_EQ(PackOpType::MARK_GARBAGE, result.operations[1].type);
}

TEST(ObjectPack, Engine_PlanDelete) {
  ObjectPackEngine engine;
  
  hobject_t obj = mk_obj(1);
  hobject_t container = mk_obj(100);
  PackedObjectInfo packed_info(container, 2048, 1024);
  
  ContainerInfo container_info(container, 4 * 1024 * 1024);
  container_info.used_bytes = 10 * 1024;
  container_info.garbage_bytes = 1024;
  
  PackResult result = engine.plan_delete(obj, packed_info, container_info);
  
  ASSERT_TRUE(result.success);
  ASSERT_GE(result.operations.size(), 1u);
  
  // Should mark as garbage
  ASSERT_EQ(PackOpType::MARK_GARBAGE, result.operations[0].type);
  ASSERT_EQ(container, result.operations[0].container_object);
  ASSERT_EQ(2048u, result.operations[0].offset);
  ASSERT_EQ(1024u, result.operations[0].length);
}

TEST(ObjectPack, Engine_PlanDelete_TriggersGC) {
  PackingConfig config;
  config.gc_fragmentation_threshold = 0.20;
  config.gc_min_garbage_bytes = 1024;
  ObjectPackEngine engine(config);
  
  hobject_t obj = mk_obj(1);
  hobject_t container = mk_obj(100);
  PackedObjectInfo packed_info(container, 0, 2048);
  
  ContainerInfo container_info(container, 4 * 1024 * 1024);
  container_info.used_bytes = 8 * 1024;
  container_info.garbage_bytes = 1024;
  // After delete: used=6K, garbage=3K, ratio=3/9=0.33 > 0.20
  
  PackResult result = engine.plan_delete(obj, packed_info, container_info);
  
  ASSERT_TRUE(result.success);
  ASSERT_EQ(2u, result.operations.size());
  
  // Should include GC hint
  ASSERT_EQ(PackOpType::COMPACT_CONTAINER, result.operations[1].type);
}

TEST(ObjectPack, Engine_NeedsGC) {
  PackingConfig config;
  config.gc_fragmentation_threshold = 0.25;
  config.gc_min_garbage_bytes = 1024;
  ObjectPackEngine engine(config);
  
  hobject_t container = mk_obj(100);
  ContainerInfo info(container, 4 * 1024 * 1024);
  
  // No garbage
  info.used_bytes = 10 * 1024;
  info.garbage_bytes = 0;
  ASSERT_FALSE(engine.needs_gc(info));
  
  // High fragmentation but not enough garbage
  info.used_bytes = 1024;
  info.garbage_bytes = 512; // 33% fragmentation but < 1024 bytes
  ASSERT_FALSE(engine.needs_gc(info));
  
  // Enough garbage but low fragmentation
  info.used_bytes = 100 * 1024;
  info.garbage_bytes = 2 * 1024; // Only 2% fragmentation
  ASSERT_FALSE(engine.needs_gc(info));
  
  // Both conditions met
  info.used_bytes = 3 * 1024;
  info.garbage_bytes = 2 * 1024; // 40% fragmentation and > 1024 bytes
  ASSERT_TRUE(engine.needs_gc(info));
}

TEST(ObjectPack, Engine_PlanGC) {
  PackingConfig config;
  config.gc_fragmentation_threshold = 0.20;  // 20%
  config.gc_min_garbage_bytes = 1024;        // 1KB minimum
  ObjectPackEngine engine(config);
  
  hobject_t container = mk_obj(100);
  ContainerInfo container_info(container, 4 * 1024 * 1024);
  container_info.used_bytes = 3 * 1024;
  container_info.garbage_bytes = 2 * 1024;  // 40% fragmentation, 2KB garbage
  
  // Add some live objects to reverse map
  hobject_t obj1 = mk_obj(1);
  hobject_t obj2 = mk_obj(2);
  container_info.reverse_map[0] = ContainerReverseMapEntry(obj1, 1024, false);
  container_info.reverse_map[2048] = ContainerReverseMapEntry(obj2, 2048, false);
  
  std::map<hobject_t, PackedObjectInfo> live_objects;
  live_objects[obj1] = PackedObjectInfo(container, 0, 1024);
  live_objects[obj2] = PackedObjectInfo(container, 2048, 2048);
  
  PackResult result = engine.plan_gc(container_info, live_objects);
  
  ASSERT_TRUE(result.success);
  ASSERT_GT(result.operations.size(), 0u);
  
  // Should end with delete of old container
  ASSERT_EQ(PackOpType::DELETE_CONTAINER, result.operations.back().type);
  ASSERT_EQ(container, result.operations.back().container_object);
}

TEST(ObjectPack, Engine_PlanGC_NotNeeded) {
  ObjectPackEngine engine;
  
  hobject_t container = mk_obj(100);
  ContainerInfo container_info(container, 4 * 1024 * 1024);
  container_info.used_bytes = 10 * 1024;
  container_info.garbage_bytes = 100; // Low fragmentation
  
  std::map<hobject_t, PackedObjectInfo> live_objects;
  
  PackResult result = engine.plan_gc(container_info, live_objects);
  
  ASSERT_FALSE(result.success);
}

TEST(ObjectPack, Engine_PlanPromote) {
  PackingConfig config;
  config.small_object_threshold = 1024;
  ObjectPackEngine engine(config);
  
  hobject_t obj = mk_obj(1);
  hobject_t container = mk_obj(100);
  PackedObjectInfo packed_info(container, 2048, 512);
  
  PackResult result = engine.plan_promote(obj, packed_info, 2048);
  
  ASSERT_TRUE(result.success);
  ASSERT_EQ(2u, result.operations.size());
  
  // Should promote
  ASSERT_EQ(PackOpType::PROMOTE_TO_STANDALONE, result.operations[0].type);
  ASSERT_EQ(obj, result.operations[0].logical_object);
  
  // Should mark old location as garbage
  ASSERT_EQ(PackOpType::MARK_GARBAGE, result.operations[1].type);
  ASSERT_EQ(container, result.operations[1].container_object);
  ASSERT_EQ(2048u, result.operations[1].offset);
  ASSERT_EQ(512u, result.operations[1].length);
}

TEST(ObjectPack, Engine_PlanPromote_StillSmall) {
  PackingConfig config;
  config.small_object_threshold = 2048;
  ObjectPackEngine engine(config);
  
  hobject_t obj = mk_obj(1);
  hobject_t container = mk_obj(100);
  PackedObjectInfo packed_info(container, 0, 512);
  
  PackResult result = engine.plan_promote(obj, packed_info, 1024);
  
  ASSERT_FALSE(result.success);
}

// ============================================================================
// Integration Tests
// ============================================================================

TEST(ObjectPack, Integration_WriteReadCycle) {
  ObjectPackEngine engine;
  
  // Write object
  hobject_t obj = mk_obj(1);
  ceph::buffer::list write_data = mk_data(1024, 'x');
  
  PackResult write_result = engine.plan_write(obj, write_data, std::nullopt);
  ASSERT_TRUE(write_result.success);
  
  // Extract packing info from write operations
  ASSERT_TRUE(write_result.operations[1].packed_info.has_value());
  PackedObjectInfo packed_info = *write_result.operations[1].packed_info;
  
  // Read object
  PackResult read_result = engine.plan_read(obj, packed_info);
  ASSERT_TRUE(read_result.success);
  ASSERT_EQ(1u, read_result.operations.size());
  
  // Verify read targets correct location
  ASSERT_EQ(packed_info.container_object_id, 
            read_result.operations[0].container_object);
  ASSERT_EQ(packed_info.offset, read_result.operations[0].offset);
  ASSERT_EQ(packed_info.length, read_result.operations[0].length);
}

TEST(ObjectPack, Integration_MultipleObjectsInContainer) {
  ObjectPackEngine engine;
  
  hobject_t container = mk_obj(100);
  ContainerInfo container_info(container, 4 * 1024 * 1024);
  
  // Write first object
  hobject_t obj1 = mk_obj(1);
  ceph::buffer::list data1 = mk_data(1024);
  PackResult result1 = engine.plan_write(obj1, data1, container_info);
  ASSERT_TRUE(result1.success);
  
  // Update container info
  container_info.next_offset = 1024;
  container_info.used_bytes = 1024;
  
  // Write second object
  hobject_t obj2 = mk_obj(2);
  ceph::buffer::list data2 = mk_data(512);
  PackResult result2 = engine.plan_write(obj2, data2, container_info);
  ASSERT_TRUE(result2.success);
  
  // Second object should be at aligned offset after first
  ASSERT_EQ(1024u, result2.operations[0].offset);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

// Made with Bob
