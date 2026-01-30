// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Contributors
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "ObjectPack.h"
#include "include/encoding.h"
#include "common/Formatter.h"
#include <sstream>

namespace ceph {
namespace osd {

// ============================================================================
// PackedObjectInfo Implementation
// ============================================================================

void PackedObjectInfo::encode(ceph::buffer::list& bl) const {
  ENCODE_START(1, 1, bl);
  encode(container_object_id, bl);
  encode(offset, bl);
  encode(length, bl);
  encode(packed, bl);
  ENCODE_FINISH(bl);
}

void PackedObjectInfo::decode(ceph::buffer::list::const_iterator& p) {
  DECODE_START(1, p);
  decode(container_object_id, p);
  decode(offset, p);
  decode(length, p);
  decode(packed, p);
  DECODE_FINISH(p);
}

void PackedObjectInfo::dump(ceph::Formatter *f) const {
  f->dump_stream("container_object_id") << container_object_id;
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("length", length);
}

// ============================================================================
// ContainerReverseMapEntry Implementation
// ============================================================================

void ContainerReverseMapEntry::encode(ceph::buffer::list& bl) const {
  ENCODE_START(1, 1, bl);
  encode(logical_object_id, bl);
  encode(length, bl);
  encode(is_garbage, bl);
  ENCODE_FINISH(bl);
}

void ContainerReverseMapEntry::decode(ceph::buffer::list::const_iterator& p) {
  DECODE_START(1, p);
  decode(logical_object_id, p);
  decode(length, p);
  decode(is_garbage, p);
  DECODE_FINISH(p);
}

void ContainerReverseMapEntry::dump(ceph::Formatter *f) const {
  f->dump_stream("logical_object_id") << logical_object_id;
  f->dump_unsigned("length", length);
  f->dump_bool("is_garbage", is_garbage);
}

// ============================================================================
// ContainerInfo Implementation
// ============================================================================

void ContainerInfo::encode(ceph::buffer::list& bl) const {
  ENCODE_START(1, 1, bl);
  encode(container_id, bl);
  encode(total_size, bl);
  encode(used_bytes, bl);
  encode(garbage_bytes, bl);
  encode(next_offset, bl);
  encode(is_sealed, bl);
  encode(reverse_map, bl);
  ENCODE_FINISH(bl);
}

void ContainerInfo::decode(ceph::buffer::list::const_iterator& p) {
  DECODE_START(1, p);
  decode(container_id, p);
  decode(total_size, p);
  decode(used_bytes, p);
  decode(garbage_bytes, p);
  decode(next_offset, p);
  decode(is_sealed, p);
  decode(reverse_map, p);
  DECODE_FINISH(p);
}

void ContainerInfo::dump(ceph::Formatter *f) const {
  f->dump_stream("container_id") << container_id;
  f->dump_unsigned("total_size", total_size);
  f->dump_unsigned("used_bytes", used_bytes);
  f->dump_unsigned("garbage_bytes", garbage_bytes);
  f->dump_unsigned("next_offset", next_offset);
  f->dump_bool("is_sealed", is_sealed);
  f->dump_float("fragmentation_ratio", fragmentation_ratio());
  f->dump_unsigned("available_space", available_space());
  f->open_array_section("reverse_map");
  for (const auto& [offset, entry] : reverse_map) {
    f->open_object_section("entry");
    f->dump_unsigned("offset", offset);
    entry.dump(f);
    f->close_section();
  }
  f->close_section();
}

// ============================================================================
// PackOperation Static Factory Methods
// ============================================================================

PackOperation PackOperation::write_to_container(
  const hobject_t& logical_obj,
  const hobject_t& container_obj,
  uint64_t off,
  const ceph::buffer::list& bl)
{
  PackOperation op(PackOpType::WRITE_TO_CONTAINER);
  op.logical_object = logical_obj;
  op.container_object = container_obj;
  op.offset = off;
  op.length = bl.length();
  op.data = bl;
  return op;
}

PackOperation PackOperation::update_object_oi(
  const hobject_t& logical_obj,
  const PackedObjectInfo& info)
{
  PackOperation op(PackOpType::UPDATE_OBJECT_OI);
  op.logical_object = logical_obj;
  op.packed_info = info;
  return op;
}

PackOperation PackOperation::update_container_omap(
  const hobject_t& container_obj,
  uint64_t off,
  const ContainerReverseMapEntry& entry)
{
  PackOperation op(PackOpType::UPDATE_CONTAINER_OMAP);
  op.container_object = container_obj;
  op.offset = off;
  op.reverse_entry = entry;
  return op;
}

PackOperation PackOperation::read_from_container(
  const hobject_t& logical_obj,
  const hobject_t& container_obj,
  uint64_t off,
  uint64_t len)
{
  PackOperation op(PackOpType::READ_FROM_CONTAINER);
  op.logical_object = logical_obj;
  op.container_object = container_obj;
  op.offset = off;
  op.length = len;
  return op;
}

PackOperation PackOperation::mark_garbage(
  const hobject_t& container_obj,
  uint64_t off,
  uint64_t len)
{
  PackOperation op(PackOpType::MARK_GARBAGE);
  op.container_object = container_obj;
  op.offset = off;
  op.length = len;
  return op;
}

PackOperation PackOperation::promote_to_standalone(
  const hobject_t& logical_obj)
{
  PackOperation op(PackOpType::PROMOTE_TO_STANDALONE);
  op.logical_object = logical_obj;
  return op;
}

PackOperation PackOperation::compact_container(
  const hobject_t& container_obj)
{
  PackOperation op(PackOpType::COMPACT_CONTAINER);
  op.container_object = container_obj;
  return op;
}

PackOperation PackOperation::delete_container(
  const hobject_t& container_obj)
{
  PackOperation op(PackOpType::DELETE_CONTAINER);
  op.container_object = container_obj;
  return op;
}

// ============================================================================
// ObjectPackEngine Implementation
// ============================================================================

hobject_t ObjectPackEngine::generate_container_id(const hobject_t& logical_obj) const {
  // Generate a container ID based on the logical object's pool and hash
  // This ensures objects with similar hashes are packed together
  std::ostringstream ss;
  ss << ".container_" << logical_obj.pool << "_" << logical_obj.get_hash();
  
  hobject_t container_id;
  container_id.pool = logical_obj.pool;
  container_id.nspace = logical_obj.nspace;
  container_id.oid = object_t(ss.str());
  container_id.set_hash(logical_obj.get_hash());
  container_id.snap = CEPH_NOSNAP;
  
  return container_id;
}

uint64_t ObjectPackEngine::calculate_aligned_offset(
  uint64_t current_offset, 
  uint64_t object_size) const 
{
  uint64_t alignment = config_.get_alignment(object_size);
  return config_.align_offset(current_offset, alignment);
}

bool ObjectPackEngine::should_trigger_gc(const ContainerInfo& container) const {
  // Trigger GC if fragmentation exceeds threshold AND we have enough garbage
  return container.fragmentation_ratio() >= config_.gc_fragmentation_threshold &&
         container.garbage_bytes >= config_.gc_min_garbage_bytes;
}

std::vector<PackOperation> ObjectPackEngine::generate_gc_operations(
  const ContainerInfo& container,
  const std::map<hobject_t, PackedObjectInfo>& affected_objects) const
{
  std::vector<PackOperation> ops;
  
  // This is a placeholder for GC operation generation
  // The actual implementation would:
  // 1. Create a new container
  // 2. Copy live objects to new container
  // 3. Update all affected object OIs
  // 4. Delete old container
  
  ops.push_back(PackOperation::compact_container(container.container_id));
  
  return ops;
}

// ============================================================================
// Main Planning Methods
// ============================================================================

PackResult ObjectPackEngine::plan_write(
  const hobject_t& logical_obj,
  const ceph::buffer::list& data,
  const std::optional<ContainerInfo>& current_container) const
{
  std::vector<PackOperation> ops;
  
  uint64_t data_size = data.length();
  
  // Check if object should be packed
  if (!should_pack_object(data_size)) {
    return PackResult::error("Object size exceeds packing threshold");
  }
  
  // Determine target container
  hobject_t container_id;
  uint64_t write_offset;
  
  if (current_container.has_value() && 
      !current_container->is_sealed &&
      current_container->available_space() >= data_size) {
    // Use existing container
    container_id = current_container->container_id;
    write_offset = calculate_aligned_offset(
      current_container->next_offset, 
      data_size);
    
    // Check if aligned offset still fits
    if (write_offset + data_size > current_container->total_size) {
      // Need a new container
      container_id = generate_container_id(logical_obj);
      write_offset = 0;
    }
  } else {
    // Create new container
    container_id = generate_container_id(logical_obj);
    write_offset = 0;
  }
  
  // Generate operations
  
  // 1. Write data to container
  ops.push_back(PackOperation::write_to_container(
    logical_obj, container_id, write_offset, data));
  
  // 2. Update logical object's OI with packing info
  PackedObjectInfo packed_info(container_id, write_offset, data_size);
  ops.push_back(PackOperation::update_object_oi(logical_obj, packed_info));
  
  // 3. Update container's reverse map
  ContainerReverseMapEntry reverse_entry(logical_obj, data_size, false);
  ops.push_back(PackOperation::update_container_omap(
    container_id, write_offset, reverse_entry));
  
  return PackResult::ok(std::move(ops));
}

PackResult ObjectPackEngine::plan_read(
  const hobject_t& logical_obj,
  const PackedObjectInfo& packed_info) const
{
  std::vector<PackOperation> ops;
  
  if (!packed_info.is_packed()) {
    return PackResult::error("Object is not packed");
  }
  
  // Generate read operation from container
  ops.push_back(PackOperation::read_from_container(
    logical_obj,
    packed_info.container_object_id,
    packed_info.offset,
    packed_info.length));
  
  return PackResult::ok(std::move(ops));
}

PackResult ObjectPackEngine::plan_modify(
  const hobject_t& logical_obj,
  const ceph::buffer::list& existing_data,
  const ceph::buffer::list& new_data,
  const PackedObjectInfo& packed_info,
  const std::optional<ContainerInfo>& current_container) const
{
  std::vector<PackOperation> ops;
  
  // Combine existing and new data
  ceph::buffer::list combined_data;
  combined_data.append(existing_data);
  combined_data.append(new_data);
  
  uint64_t new_size = combined_data.length();
  
  // Check if object should be promoted to standalone
  if (!should_pack_object(new_size)) {
    ops.push_back(PackOperation::promote_to_standalone(logical_obj));
    
    // Mark old location as garbage
    ops.push_back(PackOperation::mark_garbage(
      packed_info.container_object_id,
      packed_info.offset,
      packed_info.length));
    
    return PackResult::ok(std::move(ops));
  }
  
  // Use copy-on-write: write to new location
  
  // 1. Mark old location as garbage
  ops.push_back(PackOperation::mark_garbage(
    packed_info.container_object_id,
    packed_info.offset,
    packed_info.length));
  
  // 2. Write to new location (reuse plan_write logic)
  PackResult write_result = plan_write(logical_obj, combined_data, current_container);
  
  if (!write_result.success) {
    return write_result;
  }
  
  // Append write operations
  ops.insert(ops.end(), 
             write_result.operations.begin(), 
             write_result.operations.end());
  
  return PackResult::ok(std::move(ops));
}

PackResult ObjectPackEngine::plan_delete(
  const hobject_t& logical_obj,
  const PackedObjectInfo& packed_info,
  const ContainerInfo& container_info) const
{
  std::vector<PackOperation> ops;
  
  if (!packed_info.is_packed()) {
    return PackResult::error("Object is not packed");
  }
  
  // Mark space as garbage in container
  ops.push_back(PackOperation::mark_garbage(
    packed_info.container_object_id,
    packed_info.offset,
    packed_info.length));
  
  // Check if GC should be triggered
  // Note: We create a hypothetical container state after this delete
  ContainerInfo updated_container = container_info;
  updated_container.used_bytes -= packed_info.length;
  updated_container.garbage_bytes += packed_info.length;
  
  if (should_trigger_gc(updated_container)) {
    // Note: Actual GC would be triggered asynchronously
    // This is just a hint that GC should be scheduled
    ops.push_back(PackOperation::compact_container(container_info.container_id));
  }
  
  return PackResult::ok(std::move(ops));
}

PackResult ObjectPackEngine::plan_gc(
  const ContainerInfo& container_info,
  const std::map<hobject_t, PackedObjectInfo>& live_objects) const
{
  std::vector<PackOperation> ops;
  
  // Check if container meets GC criteria (both conditions must be met)
  if (container_info.fragmentation_ratio() < config_.gc_fragmentation_threshold) {
    return PackResult::error("Container fragmentation below threshold");
  }
  if (container_info.garbage_bytes < config_.gc_min_garbage_bytes) {
    return PackResult::error("Container garbage bytes below minimum");
  }
  
  // Generate new container ID
  hobject_t new_container_id = container_info.container_id;
  // Modify to create a new version (simple approach: append timestamp or counter)
  std::ostringstream ss;
  ss << new_container_id.oid.name << "_gc";
  new_container_id.oid = object_t(ss.str());
  
  uint64_t new_offset = 0;
  
  // Iterate through live objects in order
  for (const auto& [logical_obj, packed_info] : live_objects) {
    // Skip if not in this container
    if (packed_info.container_object_id != container_info.container_id) {
      continue;
    }
    
    // Find the object in reverse map to get its data
    auto it = container_info.reverse_map.find(packed_info.offset);
    if (it == container_info.reverse_map.end() || it->second.is_garbage) {
      continue; // Skip garbage entries
    }
    
    uint64_t object_size = it->second.length;
    
    // Calculate aligned offset in new container
    uint64_t aligned_offset = calculate_aligned_offset(new_offset, object_size);
    
    // 1. Read from old container (caller must execute this)
    ops.push_back(PackOperation::read_from_container(
      logical_obj,
      container_info.container_id,
      packed_info.offset,
      object_size));
    
    // 2. Write to new container
    // Note: Data will be provided by caller after read
    ops.push_back(PackOperation::write_to_container(
      logical_obj,
      new_container_id,
      aligned_offset,
      ceph::buffer::list())); // Empty, caller fills after read
    
    // 3. Update object OI
    PackedObjectInfo new_packed_info(new_container_id, aligned_offset, object_size);
    ops.push_back(PackOperation::update_object_oi(logical_obj, new_packed_info));
    
    // 4. Update new container reverse map
    ContainerReverseMapEntry new_entry(logical_obj, object_size, false);
    ops.push_back(PackOperation::update_container_omap(
      new_container_id, aligned_offset, new_entry));
    
    new_offset = aligned_offset + object_size;
  }
  
  // 5. Delete old container
  ops.push_back(PackOperation::delete_container(container_info.container_id));
  
  return PackResult::ok(std::move(ops));
}

PackResult ObjectPackEngine::plan_promote(
  const hobject_t& logical_obj,
  const PackedObjectInfo& packed_info,
  uint64_t new_size) const
{
  std::vector<PackOperation> ops;
  
  if (should_pack_object(new_size)) {
    return PackResult::error("Object size still within packing threshold");
  }
  
  if (!packed_info.is_packed()) {
    return PackResult::error("Object is not packed");
  }
  
  // 1. Promote to standalone
  ops.push_back(PackOperation::promote_to_standalone(logical_obj));
  
  // 2. Mark old location as garbage
  ops.push_back(PackOperation::mark_garbage(
    packed_info.container_object_id,
    packed_info.offset,
    packed_info.length));
  
  return PackResult::ok(std::move(ops));
}

} // namespace osd
} // namespace ceph

// Made with Bob
