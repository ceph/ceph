// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OS_BLUESTORE_ZONED_TYPES_H
#define CEPH_OS_BLUESTORE_ZONED_TYPES_H

#include "include/types.h"
#include "kv/KeyValueDB.h"
#include "os/kv.h"

// Tracks two bits of information about the state of a zone: (1) number of dead
// bytes in a zone and (2) the write pointer.  We assume that for now 32 bits is
// enough for the zone capacity and represent these as uint32_t, and we store
// them as a single 64-bit value in RocksDB so that we can use the existing
// Int64ArrayMergeOperator for merge and avoid the cost of point queries.
//
// We use the same struct for an on-disk and in-memory representation of the
// state.
struct zone_state_t {
  uint32_t num_dead_bytes = 0;
  uint32_t write_pointer = 0;

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &p);

  uint64_t get_num_dead_bytes() const {
    return num_dead_bytes;
  }

  uint64_t get_write_pointer() const {
    return write_pointer;
  }

  void increment_num_dead_bytes(uint64_t num_bytes) {
    num_dead_bytes += num_bytes;
  }

  void increment_write_pointer(uint64_t num_bytes) {
    write_pointer += num_bytes;
  }
};

std::ostream& operator<<(std::ostream& out, const zone_state_t& zone_state);

#endif
