// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OS_BLUESTORE_ZONED_TYPES_H
#define CEPH_OS_BLUESTORE_ZONED_TYPES_H

#include "include/types.h"
#include "kv/KeyValueDB.h"
#include "os/kv.h"

// Tracks two bits of information about the state of a zone: (1) number of dead
// bytes in a zone and (2) the write pointer.  We use the existing
// Int64ArrayMergeOperator for merge and avoid the cost of point queries.
//
// We use the same struct for an on-disk and in-memory representation of the
// state.
struct zone_state_t {
  uint64_t num_dead_bytes = 0;  ///< dead bytes deallocated (behind the write pointer)
  uint64_t write_pointer = 0;   ///< relative offset within the zone

  void encode(ceph::buffer::list &bl) const {
    using ceph::encode;
    encode(write_pointer, bl);
    encode(num_dead_bytes, bl);
  }
  void decode(ceph::buffer::list::const_iterator &p) {
    using ceph::decode;
    decode(write_pointer, p);
    decode(num_dead_bytes, p);
  }

  void reset() {
    write_pointer = 0;
    num_dead_bytes = 0;
  }

  uint64_t get_num_dead_bytes() const {
    return num_dead_bytes;
  }

  uint64_t get_num_live_bytes() const {
    return write_pointer - num_dead_bytes;
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

  friend std::ostream& operator<<(
    std::ostream& out,
    const zone_state_t& zone_state) {
    return out << std::hex
	       << " dead bytes: 0x" << zone_state.get_num_dead_bytes()
	       << " write pointer: 0x"  << zone_state.get_write_pointer()
	       << " " << std::dec;
  }
};

#endif
