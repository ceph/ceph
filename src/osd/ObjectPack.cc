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
#include <iomanip>

namespace ceph {
namespace osd {

// ============================================================================
// PackedObjectInfo Implementation
// ============================================================================

void PackedObjectInfo::encode(ceph::buffer::list& bl) const {
  ENCODE_START(1, 1, bl);
  encode(container_index, bl);
  encode(offset, bl);
  encode(length, bl);
  encode(packed, bl);
  ENCODE_FINISH(bl);
}

void PackedObjectInfo::decode(ceph::buffer::list::const_iterator& p) {
  DECODE_START(1, p);
  decode(container_index, p);
  decode(offset, p);
  decode(length, p);
  decode(packed, p);
  DECODE_FINISH(p);
}

void PackedObjectInfo::dump(ceph::Formatter *f) const {
  f->dump_unsigned("container_index", container_index);
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("length", length);
  f->dump_bool("packed", packed);
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
  encode(container_index, bl);
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
  decode(container_index, p);
  decode(total_size, p);
  decode(used_bytes, p);
  decode(garbage_bytes, p);
  decode(next_offset, p);
  decode(is_sealed, p);
  decode(reverse_map, p);
  DECODE_FINISH(p);
}

void ContainerInfo::dump(ceph::Formatter *f) const {
  f->dump_unsigned("container_index", container_index);
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
// ObjectPackEngine Implementation
// ============================================================================

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

void ObjectPackEngine::encode_omap_entry(
  std::map<std::string, ceph::buffer::list>& omap,
  uint64_t offset,
  const ContainerReverseMapEntry& entry) const
{
  using ceph::encode;
  std::ostringstream key_stream;
  key_stream << std::setw(20) << std::setfill('0') << offset;
  std::string key = key_stream.str();
  
  ceph::buffer::list value_bl;
  encode(entry, value_bl);
  omap[key] = value_bl;
}

// ============================================================================
// Main Planning Methods
// ============================================================================

PackResult ObjectPackEngine::plan_write(
  const hobject_t& logical_obj,
  const ceph::buffer::list& data,
  const std::optional<ContainerInfo>& current_container) const
{
  using ceph::encode;
  
  uint64_t data_size = data.length();
  
  // Check if object should be packed
  if (!should_pack_object(data_size)) {
    return PackResult::error("Object size exceeds packing threshold");
  }
  
  // Determine target container index
  uint64_t container_index;
  uint64_t write_offset;
  
  if (current_container.has_value() &&
      !current_container->is_sealed &&
      current_container->available_space() >= data_size) {
    // Use existing container
    container_index = current_container->container_index;
    write_offset = calculate_aligned_offset(
      current_container->next_offset,
      data_size);
    
    // Check if aligned offset still fits
    if (write_offset + data_size > current_container->total_size) {
      // Need a new container - caller must provide the next index
      return PackResult::error("Current container full - caller must allocate new container");
    }
  } else {
    // Caller must provide a container index
    return PackResult::error("No current container - caller must allocate container");
  }
  
  // Build transaction
  ceph::os::Transaction t;
  
  // Note: We need a collection ID (cid) to build transactions
  // The caller must provide the correct cid when executing
  coll_t cid;  // Placeholder - caller must set this
  
  // Generate container hobject from index
  hobject_t container_hobj = container_hobject_from_index(
    container_index, logical_obj.pool, logical_obj.nspace);
  
  // 1. Write data to container at offset
  t.write(cid, ghobject_t(container_hobj), write_offset, data_size, data);
  
  // 2. Update logical object's OI with packing info
  // The OI update is typically done by the caller after this transaction
  // We return the packed_info for the caller to use
  PackedObjectInfo packed_info(container_index, write_offset, data_size);
  
  // 3. Update container's reverse map (OMAP)
  ContainerReverseMapEntry reverse_entry(logical_obj, data_size, false);
  std::map<std::string, ceph::buffer::list> omap_updates;
  encode_omap_entry(omap_updates, write_offset, reverse_entry);
  t.omap_setkeys(cid, ghobject_t(container_hobj), omap_updates);
  
  return PackResult::ok_with_info(std::move(t), packed_info);
}

PackResult ObjectPackEngine::plan_read(
  const hobject_t& logical_obj,
  const PackedObjectInfo& packed_info) const
{
  if (!packed_info.is_packed()) {
    return PackResult::error("Object is not packed");
  }
  
  // Return read specification - caller will perform the actual read
  PackReadSpec read_spec(
    packed_info.container_object_id,
    packed_info.offset,
    packed_info.length);
  
  return PackResult::ok_with_read(read_spec);
}

PackResult ObjectPackEngine::plan_modify(
  const hobject_t& logical_obj,
  const ceph::buffer::list& existing_data,
  const ceph::buffer::list& new_data,
  const PackedObjectInfo& packed_info,
  const std::optional<ContainerInfo>& current_container) const
{
  using ceph::encode;
  
  // Combine existing and new data
  ceph::buffer::list combined_data;
  combined_data.append(existing_data);
  combined_data.append(new_data);
  
  uint64_t new_size = combined_data.length();
  
  // Check if object should be promoted to standalone
  if (!should_pack_object(new_size)) {
    // For promotion, we return an empty transaction
    // The caller needs to handle the actual promotion (copy data out, create standalone object)
    // We just mark the old location as garbage
    ceph::os::Transaction t;
    coll_t cid;  // Placeholder
    
    // Mark old location as garbage in OMAP
    ContainerReverseMapEntry garbage_entry(logical_obj, packed_info.length, true);
    std::map<std::string, ceph::buffer::list> omap_updates;
    encode_omap_entry(omap_updates, packed_info.offset, garbage_entry);
    t.omap_setkeys(cid, ghobject_t(packed_info.container_object_id), omap_updates);
    
    return PackResult::ok(std::move(t));
  }
  
  // Use copy-on-write: write to new location
  ceph::os::Transaction t;
  coll_t cid;  // Placeholder
  
  // 1. Mark old location as garbage
  ContainerReverseMapEntry garbage_entry(logical_obj, packed_info.length, true);
  std::map<std::string, ceph::buffer::list> omap_updates;
  encode_omap_entry(omap_updates, packed_info.offset, garbage_entry);
  t.omap_setkeys(cid, ghobject_t(packed_info.container_object_id), omap_updates);
  
  // 2. Write to new location (reuse plan_write logic)
  PackResult write_result = plan_write(logical_obj, combined_data, current_container);
  
  if (!write_result.success) {
    return write_result;
  }
  
  // Append write transaction
  t.append(write_result.transaction);
  
  return PackResult::ok_with_info(std::move(t), *write_result.packed_info);
}

PackResult ObjectPackEngine::plan_delete(
  const hobject_t& logical_obj,
  const PackedObjectInfo& packed_info,
  const ContainerInfo& container_info) const
{
  using ceph::encode;
  
  if (!packed_info.is_packed()) {
    return PackResult::error("Object is not packed");
  }
  
  ceph::os::Transaction t;
  coll_t cid;  // Placeholder
  
  // Mark space as garbage in container OMAP
  ContainerReverseMapEntry garbage_entry(logical_obj, packed_info.length, true);
  std::map<std::string, ceph::buffer::list> omap_updates;
  encode_omap_entry(omap_updates, packed_info.offset, garbage_entry);
  t.omap_setkeys(cid, ghobject_t(packed_info.container_object_id), omap_updates);
  
  // Note: GC triggering is a policy decision that should be made by the caller
  // based on the updated container state
  
  return PackResult::ok(std::move(t));
}

PackResult ObjectPackEngine::plan_gc(
  const ContainerInfo& container_info,
  const std::map<hobject_t, PackedObjectInfo>& live_objects) const
{
  using ceph::encode;
  
  // Check if container meets GC criteria (both conditions must be met)
  if (container_info.fragmentation_ratio() < config_.gc_fragmentation_threshold) {
    return PackResult::error("Container fragmentation below threshold");
  }
  if (container_info.garbage_bytes < config_.gc_min_garbage_bytes) {
    return PackResult::error("Container garbage bytes below minimum");
  }
  
  // GC is a complex multi-step operation that requires:
  // 1. Reading all live objects from old container
  // 2. Writing them to new container
  // 3. Updating all object OIs
  // 4. Deleting old container
  //
  // This cannot be done in a single transaction because it requires reads.
  // The caller must orchestrate this as a multi-phase operation.
  //
  // For now, we return an error indicating GC needs special handling
  return PackResult::error("GC requires multi-phase operation - caller must orchestrate");
}

PackResult ObjectPackEngine::plan_promote(
  const hobject_t& logical_obj,
  const PackedObjectInfo& packed_info,
  uint64_t new_size) const
{
  using ceph::encode;
  
  if (should_pack_object(new_size)) {
    return PackResult::error("Object size still within packing threshold");
  }
  
  if (!packed_info.is_packed()) {
    return PackResult::error("Object is not packed");
  }
  
  ceph::os::Transaction t;
  coll_t cid;  // Placeholder
  
  // Mark old location as garbage
  ContainerReverseMapEntry garbage_entry(logical_obj, packed_info.length, true);
  std::map<std::string, ceph::buffer::list> omap_updates;
  encode_omap_entry(omap_updates, packed_info.offset, garbage_entry);
  t.omap_setkeys(cid, ghobject_t(packed_info.container_object_id), omap_updates);
  
  // Note: The actual promotion (creating standalone object) must be done by caller
  // This transaction just marks the old location as garbage
  
  return PackResult::ok(std::move(t));
}

} // namespace osd
} // namespace ceph

// Made with Bob
