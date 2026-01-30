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

#ifndef CEPH_OSD_OBJECTPACK_H
#define CEPH_OSD_OBJECTPACK_H

#include <map>
#include <set>
#include <vector>
#include <optional>
#include <cstdint>

#include "include/types.h"
#include "common/hobject.h"
#include "osd/osd_types.h"
#include "os/Transaction.h"

/**
 * ObjectPack - Small Object Packing Library for Ceph OSD
 *
 * This library provides deterministic, non-blocking logic for packing
 * multiple small logical objects into larger physical "container" objects.
 * It is designed to be used by both Classic OSD and Crimson OSD.
 *
 * Key Design Principles:
 * - Pure logic, no I/O operations
 * - No blocking calls or coroutine yields
 * - Caller must pre-fetch all necessary metadata
 * - Returns abstract operations/transaction plans
 * - Thread-safe and deterministic
 */

namespace ceph {
namespace osd {

// Forward declarations
struct PackedObjectInfo;
struct ContainerInfo;
struct PackingConfig;
class ObjectPackEngine;

/**
 * Metadata stored in logical object's OI to track packing location
 */
struct PackedObjectInfo {
  uint64_t container_index;       ///< Index of container holding the data
  uint64_t offset;                ///< Byte offset within container
  uint64_t length;                ///< Length of data in container
  bool packed;                    ///< True if object is packed
  
  PackedObjectInfo() : container_index(0), offset(0), length(0), packed(false) {}
  
  PackedObjectInfo(uint64_t container_idx, uint64_t off, uint64_t len)
    : container_index(container_idx), offset(off), length(len), packed(true) {}
  
  bool is_packed() const {
    return packed;
  }
  
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER(PackedObjectInfo)

/**
 * Reverse mapping entry for container OMAP
 * Maps offset -> logical object ID
 */
struct ContainerReverseMapEntry {
  hobject_t logical_object_id;
  uint64_t length;
  bool is_garbage;  ///< True if this space is freed/garbage
  
  ContainerReverseMapEntry() : length(0), is_garbage(false) {}
  
  ContainerReverseMapEntry(const hobject_t& oid, uint64_t len, bool garbage = false)
    : logical_object_id(oid), length(len), is_garbage(garbage) {}
  
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER(ContainerReverseMapEntry)

/**
 * Container metadata tracking utilization and fragmentation
 */
struct ContainerInfo {
  uint64_t container_index;      ///< Index of this container
  uint64_t total_size;           ///< Total size of container
  uint64_t used_bytes;           ///< Bytes actually used by live objects
  uint64_t garbage_bytes;        ///< Bytes wasted by deleted/relocated objects
  uint64_t next_offset;          ///< Next available offset for appends
  bool is_sealed;                ///< True if container is full/sealed
  
  // Reverse mapping: offset -> logical object info
  std::map<uint64_t, ContainerReverseMapEntry> reverse_map;
  
  ContainerInfo()
    : container_index(0), total_size(0), used_bytes(0), garbage_bytes(0),
      next_offset(0), is_sealed(false) {}
  
  explicit ContainerInfo(uint64_t idx, uint64_t size = 0)
    : container_index(idx), total_size(size), used_bytes(0),
      garbage_bytes(0), next_offset(0), is_sealed(false) {}
  
  double fragmentation_ratio() const {
    if (used_bytes + garbage_bytes == 0) return 0.0;
    return static_cast<double>(garbage_bytes) / (used_bytes + garbage_bytes);
  }
  
  uint64_t available_space() const {
    return total_size > next_offset ? total_size - next_offset : 0;
  }
  
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER(ContainerInfo)

/**
 * Configuration for packing behavior
 */
struct PackingConfig {
  uint64_t small_object_threshold;     ///< Objects below this size are packed
  uint64_t container_size;             ///< Target size for container objects
  uint64_t alignment_small;            ///< Alignment for objects < 2KB (64 bytes)
  uint64_t alignment_large;            ///< Alignment for objects >= 2KB (4KB)
  double gc_fragmentation_threshold;   ///< Trigger GC when fragmentation exceeds this
  uint64_t gc_min_garbage_bytes;       ///< Minimum garbage bytes to trigger GC
  uint32_t ec_k;                       ///< EC k parameter (data chunks)
  uint32_t ec_m;                       ///< EC m parameter (parity chunks)
  uint64_t stripe_unit;                ///< EC stripe unit size
  
  PackingConfig()
    : small_object_threshold(64 * 1024),  // 64KB default
      container_size(4 * 1024 * 1024),    // 4MB default
      alignment_small(64),                 // CPU cache line
      alignment_large(4096),               // NVMe block
      gc_fragmentation_threshold(0.20),    // 20% waste
      gc_min_garbage_bytes(256 * 1024),    // 256KB minimum
      ec_k(0), ec_m(0), stripe_unit(0) {}
  
  uint64_t get_alignment(uint64_t object_size) const {
    return object_size < 2048 ? alignment_small : alignment_large;
  }
  
  uint64_t align_offset(uint64_t offset, uint64_t alignment) const {
    return (offset + alignment - 1) & ~(alignment - 1);
  }
};

/**
 * Read specification for packed objects
 * Since reads are not part of transactions, we return read specs separately
 */
struct PackReadSpec {
  uint64_t container_index;    ///< Index of container to read from
  uint64_t offset;             ///< Offset within container
  uint64_t length;             ///< Length to read
  
  PackReadSpec() : container_index(0), offset(0), length(0) {}
  
  PackReadSpec(uint64_t container_idx, uint64_t off, uint64_t len)
    : container_index(container_idx), offset(off), length(len) {}
};

/**
 * Result of a packing operation request
 * Contains a transaction to execute and optional read specifications
 */
struct PackResult {
  bool success;
  std::string error_message;
  ceph::os::Transaction transaction;           ///< Transaction to execute
  std::optional<PackReadSpec> read_spec;       ///< Read specification (for reads)
  std::optional<PackedObjectInfo> packed_info; ///< Updated packing info (for caller)
  
  PackResult() : success(true) {}
  
  static PackResult ok(ceph::os::Transaction&& t) {
    PackResult r;
    r.success = true;
    r.transaction = std::move(t);
    return r;
  }
  
  static PackResult ok_with_read(const PackReadSpec& read) {
    PackResult r;
    r.success = true;
    r.read_spec = read;
    return r;
  }
  
  static PackResult ok_with_info(ceph::os::Transaction&& t, const PackedObjectInfo& info) {
    PackResult r;
    r.success = true;
    r.transaction = std::move(t);
    r.packed_info = info;
    return r;
  }
  
  static PackResult error(const std::string& msg) {
    PackResult r;
    r.success = false;
    r.error_message = msg;
    return r;
  }
};

/**
 * Main packing engine - pure logic, no I/O
 */
class ObjectPackEngine {
private:
  PackingConfig config_;
  
  // Helper methods
  uint64_t calculate_aligned_offset(uint64_t current_offset, uint64_t object_size) const;
  bool should_trigger_gc(const ContainerInfo& container) const;
  
  // Helper to encode OMAP entries
  void encode_omap_entry(
    std::map<std::string, ceph::buffer::list>& omap,
    uint64_t offset,
    const ContainerReverseMapEntry& entry) const;
  
public:
  /**
   * Generate a container object name from an index
   * Format: ".pack_container_<index>"
   */
  static std::string container_name_from_index(uint64_t index) {
    std::ostringstream ss;
    ss << ".pack_container_" << index;
    return ss.str();
  }
  
  /**
   * Generate a hobject_t for a container from its index
   */
  static hobject_t container_hobject_from_index(
    uint64_t index,
    int64_t pool,
    const std::string& nspace = "") {
    hobject_t hobj;
    hobj.pool = pool;
    hobj.nspace = nspace;
    hobj.oid = object_t(container_name_from_index(index));
    hobj.snap = CEPH_NOSNAP;
    return hobj;
  }
  
private:
  
public:
  explicit ObjectPackEngine(const PackingConfig& config = PackingConfig())
    : config_(config) {}
  
  // Configuration
  const PackingConfig& get_config() const { return config_; }
  void set_config(const PackingConfig& config) { config_ = config; }
  
  /**
   * Determine if an object should be packed
   */
  bool should_pack_object(uint64_t object_size) const {
    return object_size > 0 && object_size <= config_.small_object_threshold;
  }
  
  /**
   * Plan a write operation for a new small object
   * 
   * @param logical_obj The logical object being written
   * @param data The data to write
   * @param current_container Current active container (if any)
   * @return PackResult with operations to execute
   */
  PackResult plan_write(
    const hobject_t& logical_obj,
    const ceph::buffer::list& data,
    const std::optional<ContainerInfo>& current_container) const;
  
  /**
   * Plan a read operation for a packed object
   * 
   * @param logical_obj The logical object to read
   * @param packed_info Packing info from object's OI
   * @return PackResult with operations to execute
   */
  PackResult plan_read(
    const hobject_t& logical_obj,
    const PackedObjectInfo& packed_info) const;
  
  /**
   * Plan an append/modify operation (uses copy-on-write)
   * 
   * @param logical_obj The logical object being modified
   * @param existing_data Existing data from old location
   * @param new_data New data to append/write
   * @param packed_info Current packing info
   * @param current_container Current active container
   * @return PackResult with operations to execute
   */
  PackResult plan_modify(
    const hobject_t& logical_obj,
    const ceph::buffer::list& existing_data,
    const ceph::buffer::list& new_data,
    const PackedObjectInfo& packed_info,
    const std::optional<ContainerInfo>& current_container) const;
  
  /**
   * Plan deletion of a packed object
   * 
   * @param logical_obj The logical object being deleted
   * @param packed_info Packing info from object's OI
   * @param container_info Container metadata
   * @return PackResult with operations to execute
   */
  PackResult plan_delete(
    const hobject_t& logical_obj,
    const PackedObjectInfo& packed_info,
    const ContainerInfo& container_info) const;
  
  /**
   * Plan garbage collection for a container
   * 
   * @param container_info Container to compact
   * @param live_objects Map of live objects in the container
   * @return PackResult with operations to execute
   */
  PackResult plan_gc(
    const ContainerInfo& container_info,
    const std::map<hobject_t, PackedObjectInfo>& live_objects) const;
  
  /**
   * Check if GC should be triggered for a container
   */
  bool needs_gc(const ContainerInfo& container_info) const {
    return should_trigger_gc(container_info);
  }
  
  /**
   * Plan promotion of a packed object to standalone
   * (when it grows beyond threshold)
   * 
   * @param logical_obj The object to promote
   * @param packed_info Current packing info
   * @param new_size New size after growth
   * @return PackResult with operations to execute
   */
  PackResult plan_promote(
    const hobject_t& logical_obj,
    const PackedObjectInfo& packed_info,
    uint64_t new_size) const;
};

} // namespace osd
} // namespace ceph

#endif // CEPH_OSD_OBJECTPACK_H

// Made with Bob
