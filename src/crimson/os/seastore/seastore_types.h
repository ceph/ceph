// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <limits>
#include <iostream>

#include "include/denc.h"
#include "include/buffer.h"
#include "include/cmp.h"

namespace crimson::os::seastore {

using checksum_t = uint32_t;

// Identifies segment location on disk, see SegmentManager,
using segment_id_t = uint32_t;
constexpr segment_id_t NULL_SEG_ID =
  std::numeric_limits<segment_id_t>::max() - 1;
/* Used to denote relative paddr_t */
constexpr segment_id_t REL_SEG_ID =
  std::numeric_limits<segment_id_t>::max() - 2;

std::ostream &segment_to_stream(std::ostream &, const segment_id_t &t);

// Offset within a segment on disk, see SegmentManager
// may be negative for relative offsets
using segment_off_t = int32_t;
constexpr segment_off_t NULL_SEG_OFF =
  std::numeric_limits<segment_id_t>::max();

std::ostream &offset_to_stream(std::ostream &, const segment_off_t &t);

/* Monotonically increasing segment seq, uniquely identifies
 * the incarnation of a segment */
using segment_seq_t = uint32_t;
static constexpr segment_seq_t NULL_SEG_SEQ =
  std::numeric_limits<segment_seq_t>::max();

// Offset of delta within a record
using record_delta_idx_t = uint32_t;
constexpr record_delta_idx_t NULL_DELTA_IDX =
  std::numeric_limits<record_delta_idx_t>::max();

// <segment, offset> offset on disk, see SegmentManager
struct paddr_t {
  segment_id_t segment = NULL_SEG_ID;
  segment_off_t offset = NULL_SEG_OFF;

  bool is_relative() const {
    return segment == REL_SEG_ID;
  }

  paddr_t add_offset(segment_off_t o) const {
    return paddr_t{segment, offset + o};
  }

  paddr_t add_relative(paddr_t o) const {
    assert(o.is_relative());
    assert(!is_relative());
    return paddr_t{segment, offset + o.offset};
  }

  paddr_t operator-(paddr_t rhs) const {
    assert(rhs.is_relative() && is_relative());
    return paddr_t{
      REL_SEG_ID,
      offset - rhs.offset
    };
  }

  paddr_t maybe_relative_to(paddr_t base) const {
    if (is_relative())
      return base.add_relative(*this);
    else
      return *this;
  }

  DENC(paddr_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.segment, p);
    denc(v.offset, p);
    DENC_FINISH(p);
  }
};
WRITE_CMP_OPERATORS_2(paddr_t, segment, offset)
WRITE_EQ_OPERATORS_2(paddr_t, segment, offset)
constexpr paddr_t P_ADDR_NULL = paddr_t{};
constexpr paddr_t make_relative_paddr(segment_off_t off) {
  return paddr_t{REL_SEG_ID, off};
}

std::ostream &operator<<(std::ostream &out, const paddr_t &rhs);

// logical addr, see LBAManager, TransactionManager
using laddr_t = uint64_t;
constexpr laddr_t L_ADDR_MIN = std::numeric_limits<laddr_t>::min();
constexpr laddr_t L_ADDR_MAX = std::numeric_limits<laddr_t>::max();
constexpr laddr_t L_ADDR_NULL = std::numeric_limits<laddr_t>::max();
constexpr laddr_t L_ADDR_ROOT = std::numeric_limits<laddr_t>::max() - 1;
constexpr laddr_t L_ADDR_LBAT = std::numeric_limits<laddr_t>::max() - 2;

// logical offset, see LBAManager, TransactionManager
using extent_len_t = uint32_t;
constexpr extent_len_t EXTENT_LEN_MAX =
  std::numeric_limits<extent_len_t>::max();

struct laddr_list_t : std::list<std::pair<laddr_t, extent_len_t>> {
  template <typename... T>
  laddr_list_t(T&&... args)
    : std::list<std::pair<laddr_t, extent_len_t>>(std::forward<T>(args)...) {}
};
struct paddr_list_t : std::list<std::pair<paddr_t, extent_len_t>> {
  template <typename... T>
  paddr_list_t(T&&... args)
    : std::list<std::pair<paddr_t, extent_len_t>>(std::forward<T>(args)...) {}
};

std::ostream &operator<<(std::ostream &out, const laddr_list_t &rhs);
std::ostream &operator<<(std::ostream &out, const paddr_list_t &rhs);

/* identifies type of extent, used for interpretting deltas, managing
 * writeback */
enum class extent_types_t : uint8_t {
  ROOT_LOCATION = 0, // delta only
  ROOT = 1,
  LADDR_INTERNAL = 2,
  LADDR_LEAF = 3,
  LBA_BLOCK = 4,

  // Test Block Types
  TEST_BLOCK = 0xF0,

  // None
  NONE = 0xFF
};

std::ostream &operator<<(std::ostream &out, extent_types_t t);

/* description of a new physical extent */
struct extent_t {
  ceph::bufferlist bl;  ///< payload, bl.length() == length, aligned
};

using extent_version_t = uint32_t;
constexpr extent_version_t EXTENT_VERSION_NULL = 0;

/* description of a mutation to a physical extent */
struct delta_info_t {
  extent_types_t type = extent_types_t::NONE;  ///< delta type
  paddr_t paddr;                               ///< physical address
  /* logical address -- needed for repopulating cache -- TODO don't actually need */
  // laddr_t laddr = L_ADDR_NULL;
  segment_off_t length = NULL_SEG_OFF;         ///< extent length
  extent_version_t pversion;                   ///< prior version
  ceph::bufferlist bl;                         ///< payload

  DENC(delta_info_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.type, p);
    denc(v.paddr, p);
    //denc(v.laddr, p);
    denc(v.length, p);
    denc(v.pversion, p);
    denc(v.bl, p);
    DENC_FINISH(p);
  }

  bool operator==(const delta_info_t &rhs) const {
    return (
      type == rhs.type &&
      paddr == rhs.paddr &&
      length == rhs.length &&
      pversion == rhs.pversion &&
      bl == rhs.bl
    );
  }
};

struct record_t {
  std::vector<extent_t> extents;
  std::vector<delta_info_t> deltas;
};

}

WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::paddr_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::delta_info_t)
