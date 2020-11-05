// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <limits>
#include <iostream>

#include "include/byteorder.h"
#include "include/denc.h"
#include "include/buffer.h"
#include "include/cmp.h"
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <vector>

namespace crimson::os::poseidonstore {


/* 
 *
 *  Overall architecture
 *
 *   Write          Read           Write            Read
 *    |               v              |                 v
 *    |         +-------+            |          +-------+
 *    |-------> | cache |            |--------> | cache |
 *    |         +-------+            |          +-------+
 *    V             ^ load           V              ^ load
 *
 *  +----------------------------------------------------+
 *  |              NVMe Library (xNVMe)                  |
 *  +----------------------------------------------------+
 *
 *  +-----+----------------+      +-----+----------------+   
 *  | WAL | Data partition |      | WAL | Data partition |
 *  +-----+----------------+      +-----+----------------+
 *
 */

using checksum_t = uint32_t;
using paddr_t = uint64_t; ///< physical address. (e.g., absolute address from the device point of view)
using laddr_t = uint64_t; ///< logical address. (e.g., address 0 from the onode point of view)
using wal_trans_id_t = uint64_t;
const uint32_t ce_aligned_size = 4096; ///< cache extent aligned size

constexpr wal_trans_id_t NULL_WAL_TRANS_ID =
  std::numeric_limits<wal_trans_id_t>::max();

struct wal_seq_t {
  laddr_t addr = 0;
  wal_trans_id_t id = NULL_WAL_TRANS_ID;
  uint64_t length = 0;

  DENC(wal_seq_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.addr, p);
    denc(v.id, p);
    denc(v.length, p);
    DENC_FINISH(p);
  }
  bool operator ==(const wal_seq_t &r) {
    return (addr == r.addr) &&
	   (length == r.length) &&
	   (id == r.id);
  }
};
WRITE_CMP_OPERATORS_2(wal_seq_t, addr, length)

enum class ce_types_t : uint8_t {
  // Test Block Types
  TEST_BLOCK = 0xF0,
  // None
  NONE = 0xFF
};

std::ostream &operator<<(std::ostream &out, ce_types_t t);

struct write_item {
  bufferlist bl; ///< actual data
  ce_types_t type; ///< encoded type
  laddr_t laddr; ///< write position in data partition
  DENC(write_item, v, p) {
    DENC_START(1, 1, p);
    denc(v.bl, p);
    denc(v.type, p);
    denc(v.laddr, p);
    DENC_FINISH(p);
  }
};

enum class record_state_t : uint8_t {
  ISSUING = 0x01,
  COMMITTED = 0x02,
};

class Cache;
class WAL;
class Record: public boost::intrusive_ref_counter<
  Record,
  boost::thread_unsafe_counter>
{
  std::vector<write_item> to_write;
  laddr_t location_to_wal; ///< write position to wal
  wal_seq_t cur_pos;  ///< contain acutal data length
  record_state_t state;

  friend class Cache;
  friend class WAL;
public:
  uint64_t get_write_items_length() {
    uint64_t size = 0;
    for (auto &p : to_write) {
      size += p.bl.length();
      return size;
    }
    return size;
  }
  void add_write_item(write_item &w) {
    to_write.push_back(w);
  }
  std::vector<write_item>& get_write_items() {
    return to_write;
  }
};

using RecordRef = boost::intrusive_ptr<Record>;

std::ostream &operator<<(std::ostream &out, const wal_seq_t &seq);

}

WRITE_CLASS_DENC_BOUNDED(crimson::os::poseidonstore::wal_seq_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::poseidonstore::write_item)
