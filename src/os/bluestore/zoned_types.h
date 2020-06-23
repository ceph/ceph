// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OS_BLUESTORE_ZONED_TYPES_H
#define CEPH_OS_BLUESTORE_ZONED_TYPES_H

#include "bluestore_types.h"
#include "include/types.h"
#include "kv/KeyValueDB.h"
#include "os/kv.h"

// Tracks two bits of information about the state of a zone: (1) number of dead
// bytes in a zone and (2) the write pointer.  We represent these as a
// two-element array of uint64_t because we use the existing RocksDB merge
// operator, Int64ArrayMergeOperator, to update these.
// 
// We use the same struct for an on-disk and in-memory representation of the
// state.  Calling persist method inserts the in-memory state to the database.
struct zone_state_t {
  uint64_t v[2];
  uint64_t zone_num;
  uint64_t zone_size;

  zone_state_t(uint64_t zone_num, uint64_t zone_size)
      : zone_num(zone_num), zone_size(zone_size) {
    v[0] = v[1] = 0;
  }

  zone_state_t(KeyValueDB::Iterator& it, uint64_t zone_size)
      : zone_size(zone_size) {
    ceph_assert(it->valid());

    string k = it->key();
    _key_decode_u64(k.c_str(), &zone_num);

    bufferlist bl = it->value();
    auto p = bl.cbegin();
    decode(v[0], p);
    decode(v[1], p);
  }

  uint64_t get_zone_num() const {
    return zone_num;
  }

  uint64_t get_num_dead_bytes() const {
    return v[0];
  }

  uint64_t get_write_pointer() const {
    return v[1];
  }

  void increment_num_dead_bytes(uint64_t num_bytes) {
    v[0] += num_bytes;
    ceph_assert(v[0] <= zone_size);
  }

  void increment_write_pointer(uint64_t num_bytes) {
    v[1] += num_bytes;
    ceph_assert(v[1] <= zone_size);
  }

  void persist(const std::string& prefix, KeyValueDB::Transaction txn) {
    string key;
    _key_encode_u64(zone_num, &key);

    bufferlist bl;
    encode(v[0], bl);
    encode(v[1], bl);

    txn->merge(prefix, key, bl);
  }

  uint64_t get_offset() const {
    return zone_size * zone_num + v[1];
  }
  uint64_t get_remaining_space() const {
    return zone_size - v[1];
  }
};

std::ostream& operator<<(std::ostream& out, const zone_state_t& zone_state);

#endif
