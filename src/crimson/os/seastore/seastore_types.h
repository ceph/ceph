// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <limits>
#include <numeric>
#include <iostream>

#include "include/byteorder.h"
#include "include/denc.h"
#include "include/buffer.h"
#include "include/cmp.h"
#include "include/uuid.h"
#include "include/interval_set.h"

namespace crimson::os::seastore {

using depth_t = uint32_t;
using depth_le_t = ceph_le32;

inline depth_le_t init_depth_le(uint32_t i) {
  return ceph_le32(i);
}

using checksum_t = uint32_t;

// Immutable metadata for seastore to set at mkfs time
struct seastore_meta_t {
  uuid_d seastore_id;

  DENC(seastore_meta_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.seastore_id, p);
    DENC_FINISH(p);
  }
};

// identifies a specific physical device within seastore
using device_id_t = uint8_t;

// order of device_id_t
constexpr uint16_t DEVICE_ID_LEN_BITS = 4;

// maximum number of devices supported
constexpr uint16_t DEVICE_ID_MAX = (1 << DEVICE_ID_LEN_BITS);

// segment ids without a device id encapsulated
using device_segment_id_t = uint32_t;

// Identifies segment location on disk, see SegmentManager,
struct segment_id_t {
private:
  // internal segment id type of segment_id_t, basically
  // this is a unsigned int with the top "DEVICE_ID_LEN_BITS"
  // bits representing the id of the device on which the
  // segment resides
  using internal_segment_id_t = uint32_t;

  // mask for segment manager id
  static constexpr internal_segment_id_t SM_ID_MASK =
    0xF << (std::numeric_limits<internal_segment_id_t>::digits - DEVICE_ID_LEN_BITS);
  // default internal segment id
  static constexpr internal_segment_id_t DEFAULT_INTERNAL_SEG_ID =
    std::numeric_limits<internal_segment_id_t>::max() - 1;

  internal_segment_id_t segment = DEFAULT_INTERNAL_SEG_ID;

  constexpr segment_id_t(uint32_t encoded) : segment(encoded) {}
public:
  constexpr static segment_id_t make_max() {
    return std::numeric_limits<internal_segment_id_t>::max();
  }
  constexpr static segment_id_t make_null() {
    return std::numeric_limits<internal_segment_id_t>::max() - 1;
  }
  /* Used to denote relative paddr_t */
  constexpr static segment_id_t make_record_relative() {
    return std::numeric_limits<internal_segment_id_t>::max() - 2;
  }
  constexpr static segment_id_t make_block_relative() {
    return std::numeric_limits<internal_segment_id_t>::max() - 3;
  }
  // for tests which generate fake paddrs
  constexpr static segment_id_t make_fake() {
    return std::numeric_limits<internal_segment_id_t>::max() - 4;
  }

  /* Used to denote references to notional zero filled segment, mainly
   * in denoting reserved laddr ranges for unallocated object data.
   */
  constexpr static segment_id_t make_zero() {
    return std::numeric_limits<internal_segment_id_t>::max() - 5;
  }
  constexpr static segment_id_t make_delayed() {
    return std::numeric_limits<internal_segment_id_t>::max() - 6;
  }

  segment_id_t() = default;
  segment_id_t(device_id_t id, device_segment_id_t segment)
    : segment(make_internal(segment, id)) {
    // only lower 4 bits are effective, and we have to reserve 0x0F for
    // special XXX_SEG_IDs
    assert(id < DEVICE_ID_MAX);
  }

  [[gnu::always_inline]]
  device_id_t device_id() const {
    return internal_to_device(segment);
  }

  [[gnu::always_inline]]
  device_segment_id_t device_segment_id() const {
    return internal_to_segment(segment);
  }

  bool operator==(const segment_id_t& other) const {
    return segment == other.segment;
  }
  bool operator!=(const segment_id_t& other) const {
    return segment != other.segment;
  }
  bool operator<(const segment_id_t& other) const {
    return segment < other.segment;
  }
  bool operator<=(const segment_id_t& other) const {
    return segment <= other.segment;
  }
  bool operator>(const segment_id_t& other) const {
    return segment > other.segment;
  }
  bool operator>=(const segment_id_t& other) const {
    return segment >= other.segment;
  }

  DENC(segment_id_t, v, p) {
    denc(v.segment, p);
  }
private:
  static constexpr unsigned segment_bits = (
    std::numeric_limits<internal_segment_id_t>::digits - DEVICE_ID_LEN_BITS
  );

  static inline device_id_t internal_to_device(internal_segment_id_t id) {
    return (static_cast<device_id_t>(id) & SM_ID_MASK) >> segment_bits;
  }

  static inline device_segment_id_t internal_to_segment(
    internal_segment_id_t id) {
    return id & (~SM_ID_MASK);
  }

  static inline internal_segment_id_t make_internal(
    device_segment_id_t id,
    device_id_t sm_id) {
    return static_cast<internal_segment_id_t>(id) |
      (static_cast<internal_segment_id_t>(sm_id) << segment_bits);
  }

  friend struct segment_id_le_t;
};

// ondisk type of segment_id_t
struct __attribute((packed)) segment_id_le_t {
  ceph_le32 segment = ceph_le32(segment_id_t::DEFAULT_INTERNAL_SEG_ID);

  segment_id_le_t(const segment_id_t id) :
    segment(ceph_le32(id.segment)) {}

  operator segment_id_t() const {
    return segment_id_t(segment);
  }
};

constexpr segment_id_t MAX_SEG_ID = segment_id_t::make_max();
constexpr segment_id_t NULL_SEG_ID = segment_id_t::make_null();
constexpr segment_id_t RECORD_REL_SEG_ID = segment_id_t::make_record_relative();
constexpr segment_id_t BLOCK_REL_SEG_ID = segment_id_t::make_block_relative();
// for tests which generate fake paddrs
constexpr segment_id_t FAKE_SEG_ID = segment_id_t::make_fake();
/* Used to denote references to notional zero filled segment, mainly
 * in denoting reserved laddr ranges for unallocated object data.
 */
constexpr segment_id_t ZERO_SEG_ID = segment_id_t::make_zero();
constexpr segment_id_t DELAYED_TEMP_SEG_ID = segment_id_t::make_delayed();

std::ostream &operator<<(std::ostream &out, const segment_id_t&);


std::ostream &segment_to_stream(std::ostream &, const segment_id_t &t);

// Offset within a segment on disk, see SegmentManager
// may be negative for relative offsets
using segment_off_t = int32_t;
constexpr segment_off_t NULL_SEG_OFF =
  std::numeric_limits<segment_off_t>::max();
constexpr segment_off_t MAX_SEG_OFF =
  std::numeric_limits<segment_off_t>::max();

std::ostream &offset_to_stream(std::ostream &, const segment_off_t &t);

/* Monotonically increasing segment seq, uniquely identifies
 * the incarnation of a segment */
using segment_seq_t = uint32_t;
static constexpr segment_seq_t NULL_SEG_SEQ =
  std::numeric_limits<segment_seq_t>::max();
static constexpr segment_seq_t MAX_SEG_SEQ =
  std::numeric_limits<segment_seq_t>::max();

// Offset of delta within a record
using record_delta_idx_t = uint32_t;
constexpr record_delta_idx_t NULL_DELTA_IDX =
  std::numeric_limits<record_delta_idx_t>::max();

/**
 * segment_map_t
 *
 * Compact templated mapping from a segment_id_t to a value type.
 */
template <typename T>
class segment_map_t {
public:
  segment_map_t() {
    // initializes top vector with 0 length vectors to indicate that they
    // are not yet present
    device_to_segments.resize(DEVICE_ID_MAX);
  }
  void add_device(device_id_t device, size_t segments, const T& init) {
    assert(device < DEVICE_ID_MAX);
    assert(device_to_segments[device].size() == 0);
    device_to_segments[device].resize(segments, init);
    total_segments += segments;
  }
  void clear() {
    device_to_segments.clear();
    device_to_segments.resize(DEVICE_ID_MAX);
    total_segments = 0;
  }

  T& operator[](segment_id_t id) {
    assert(id.device_segment_id() < device_to_segments[id.device_id()].size());
    return device_to_segments[id.device_id()][id.device_segment_id()];
  }
  const T& operator[](segment_id_t id) const {
    assert(id.device_segment_id() < device_to_segments[id.device_id()].size());
    return device_to_segments[id.device_id()][id.device_segment_id()];
  }

  auto begin() {
    return iterator<false>::lower_bound(*this, 0, 0);
  }
  auto begin() const {
    return iterator<true>::lower_bound(*this, 0, 0);
  }

  auto end() {
    return iterator<false>::end_iterator(*this);
  }
  auto end() const {
    return iterator<true>::end_iterator(*this);
  }

  auto device_begin(device_id_t id) {
    auto ret = iterator<false>::lower_bound(*this, id, 0);
    assert(ret->first.device_id() == id);
    return ret;
  }
  auto device_end(device_id_t id) {
    return iterator<false>::lower_bound(*this, id + 1, 0);
  }

  size_t size() const {
    return total_segments;
  }

private:
  template <bool is_const = false>
  class iterator {
    /// points at set being iterated over
    std::conditional_t<
      is_const,
      const segment_map_t &,
      segment_map_t &> parent;

    /// points at current device, or DEVICE_ID_MAX if is_end()
    device_id_t device_id;

    /// segment at which we are pointing, 0 if is_end()
    device_segment_id_t device_segment_id;

    /// holds referent for operator* and operator-> when !is_end()
    std::optional<
      std::pair<
        const segment_id_t,
	std::conditional_t<is_const, const T&, T&>
	>> current;

    bool is_end() const {
      return device_id == DEVICE_ID_MAX;
    }

    void find_valid() {
      assert(!is_end());
      auto &device_vec = parent.device_to_segments[device_id];
      if (device_vec.size() == 0 ||
	  device_segment_id == device_vec.size()) {
	while (++device_id < DEVICE_ID_MAX &&
	       parent.device_to_segments[device_id].size() == 0);
	device_segment_id = 0;
      }
      if (is_end()) {
	current = std::nullopt;
      } else {
	current.emplace(
	  segment_id_t{device_id, device_segment_id},
	  parent.device_to_segments[device_id][device_segment_id]
	);
      }
    }

    iterator(
      decltype(parent) &parent,
      device_id_t device_id,
      device_segment_id_t device_segment_id)
      : parent(parent), device_id(device_id),
	device_segment_id(device_segment_id) {}

  public:
    static iterator lower_bound(
      decltype(parent) &parent,
      device_id_t device_id,
      device_segment_id_t device_segment_id) {
      if (device_id == DEVICE_ID_MAX) {
	return end_iterator(parent);
      } else {
	auto ret = iterator{parent, device_id, device_segment_id};
	ret.find_valid();
	return ret;
      }
    }

    static iterator end_iterator(
      decltype(parent) &parent) {
      return iterator{parent, DEVICE_ID_MAX, 0};
    }

    iterator<is_const>& operator++() {
      assert(!is_end());
      ++device_segment_id;
      find_valid();
      return *this;
    }

    bool operator==(iterator<is_const> rit) {
      return (device_id == rit.device_id &&
	      device_segment_id == rit.device_segment_id);
    }

    bool operator!=(iterator<is_const> rit) {
      return !(*this == rit);
    }

    template <bool c = is_const, std::enable_if_t<c, int> = 0>
    const std::pair<const segment_id_t, const T&> *operator->() {
      assert(!is_end());
      return &*current;
    }
    template <bool c = is_const, std::enable_if_t<!c, int> = 0>
    std::pair<const segment_id_t, T&> *operator->() {
      assert(!is_end());
      return &*current;
    }
    template <bool c = is_const, std::enable_if_t<c, int> = 0>
    const std::pair<const segment_id_t, const T&> &operator*() {
      assert(!is_end());
      return *current;
    }
    template <bool c = is_const, std::enable_if_t<!c, int> = 0>
    std::pair<const segment_id_t, T&> &operator*() {
      assert(!is_end());
      return *current;
    }
  };

  /**
   * device_to_segments
   *
   * device -> segment -> T mapping.  device_to_segments[d].size() > 0 iff
   * device <d> has been added.
   */
  std::vector<std::vector<T>> device_to_segments;

  /// total number of added segments
  size_t total_segments = 0;
};

/**
 * paddr_t
 *
 * <segment, offset> offset on disk, see SegmentManager
 *
 * May be absolute, record_relative, or block_relative.
 *
 * Blocks get read independently of the surrounding record,
 * so paddrs embedded directly within a block need to refer
 * to other blocks within the same record by a block_relative
 * addr relative to the block's own offset.  By contrast,
 * deltas to existing blocks need to use record_relative
 * addrs relative to the first block of the record.
 *
 * Fresh extents during a transaction are refered to by
 * record_relative paddrs.
 */
struct paddr_t {
  segment_id_t segment = NULL_SEG_ID;
  segment_off_t offset = NULL_SEG_OFF;

  paddr_t() = default;
  paddr_t(device_id_t id, device_segment_id_t sgt, segment_off_t offset)
    : segment(segment_id_t(id, sgt)), offset(offset) {}

  constexpr paddr_t(segment_id_t segment, segment_off_t offset)
    : segment(segment), offset(offset) {}

  bool is_relative() const {
    return segment == RECORD_REL_SEG_ID ||
      segment == BLOCK_REL_SEG_ID;
  }

  bool is_record_relative() const {
    return segment == RECORD_REL_SEG_ID;
  }

  bool is_block_relative() const {
    return segment == BLOCK_REL_SEG_ID;
  }

  /// Denotes special zero segment addr
  bool is_zero() const {
    return segment == ZERO_SEG_ID;
  }

  /// Denotes special null segment addr
  bool is_null() const {
    return segment == NULL_SEG_ID;
  }

  /**
   * is_real
   *
   * indicates whether addr reflects a physical location, absolute
   * or relative.  FAKE segments also count as real so as to reflect
   * the way in which unit tests use them.
   */
  bool is_real() const {
    return !is_zero() && !is_null();
  }

  paddr_t add_offset(segment_off_t o) const {
    return paddr_t{segment, offset + o};
  }

  paddr_t add_relative(paddr_t o) const {
    assert(o.is_relative());
    return paddr_t{segment, offset + o.offset};
  }

  paddr_t add_block_relative(paddr_t o) const {
    // special version mainly for documentation purposes
    assert(o.is_block_relative());
    return add_relative(o);
  }

  paddr_t add_record_relative(paddr_t o) const {
    // special version mainly for documentation purposes
    assert(o.is_record_relative());
    return add_relative(o);
  }

  /**
   * paddr_t::operator-
   *
   * Only defined for record_relative paddr_ts.  Yields a
   * block_relative address.
   */
  paddr_t operator-(paddr_t rhs) const {
    assert(rhs.is_relative() && is_relative());
    assert(rhs.segment == segment);
    return paddr_t{
      BLOCK_REL_SEG_ID,
      offset - rhs.offset
    };
  }

  /**
   * maybe_relative_to
   *
   * Helper for the case where an in-memory paddr_t may be
   * either block_relative or absolute (not record_relative).
   *
   * base must be either absolute or record_relative.
   */
  paddr_t maybe_relative_to(paddr_t base) const {
    assert(!base.is_block_relative());
    if (is_block_relative())
      return base.add_block_relative(*this);
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
constexpr paddr_t P_ADDR_MIN = paddr_t{ZERO_SEG_ID, 0};
constexpr paddr_t P_ADDR_MAX = paddr_t{
  MAX_SEG_ID,
  MAX_SEG_OFF
};
constexpr paddr_t make_record_relative_paddr(segment_off_t off) {
  return paddr_t{RECORD_REL_SEG_ID, off};
}
constexpr paddr_t make_block_relative_paddr(segment_off_t off) {
  return paddr_t{BLOCK_REL_SEG_ID, off};
}
constexpr paddr_t make_fake_paddr(segment_off_t off) {
  return paddr_t{FAKE_SEG_ID, off};
}
constexpr paddr_t zero_paddr() {
  return paddr_t{ZERO_SEG_ID, 0};
}
constexpr paddr_t delayed_temp_paddr(segment_off_t off) {
  return paddr_t{DELAYED_TEMP_SEG_ID, off};
}

struct __attribute((packed)) paddr_le_t {
  segment_id_le_t segment = segment_id_le_t(NULL_SEG_ID);
  ceph_les32 offset = ceph_les32(NULL_SEG_OFF);

  paddr_le_t() = default;
  paddr_le_t(segment_id_t segment, segment_off_t offset)
    : segment(segment), offset(ceph_les32(offset)) {}
  paddr_le_t(const paddr_t &addr) : paddr_le_t(addr.segment, addr.offset) {}

  operator paddr_t() const {
    return paddr_t{segment, offset};
  }
};

std::ostream &operator<<(std::ostream &out, const paddr_t &rhs);

using objaddr_t = uint32_t;
constexpr objaddr_t OBJ_ADDR_MAX = std::numeric_limits<objaddr_t>::max();
constexpr objaddr_t OBJ_ADDR_NULL = OBJ_ADDR_MAX - 1;

enum class placement_hint_t {
  HOT = 0,   // Most of the metadata
  COLD,      // Object data
  REWRITE,   // Cold metadata and data (probably need further splits)
  NUM_HINTS  // Constant for number of hints
};

enum device_type_t {
  NONE = 0,
  SEGMENTED, // i.e. Hard_Disk, SATA_SSD, NAND_NVME
  RANDOM_BLOCK, // i.e. RANDOM_BD
  PMEM, // i.e. NVDIMM, PMEM
  NUM_TYPES
};

bool can_delay_allocation(device_type_t type);
device_type_t string_to_device_type(std::string type);
std::string device_type_to_string(device_type_t type);

/* Monotonically increasing identifier for the location of a
 * journal_record.
 */
struct journal_seq_t {
  segment_seq_t segment_seq = 0;
  paddr_t offset;

  DENC(journal_seq_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.segment_seq, p);
    denc(v.offset, p);
    DENC_FINISH(p);
  }
};
WRITE_CMP_OPERATORS_2(journal_seq_t, segment_seq, offset)
WRITE_EQ_OPERATORS_2(journal_seq_t, segment_seq, offset)
constexpr journal_seq_t JOURNAL_SEQ_MIN{
  0,
  paddr_t{ZERO_SEG_ID, 0}
};
constexpr journal_seq_t JOURNAL_SEQ_MAX{
  MAX_SEG_SEQ,
  P_ADDR_MAX
};

std::ostream &operator<<(std::ostream &out, const journal_seq_t &seq);

static constexpr journal_seq_t NO_DELTAS = journal_seq_t{
  NULL_SEG_SEQ,
  P_ADDR_NULL
};

// logical addr, see LBAManager, TransactionManager
using laddr_t = uint64_t;
constexpr laddr_t L_ADDR_MIN = std::numeric_limits<laddr_t>::min();
constexpr laddr_t L_ADDR_MAX = std::numeric_limits<laddr_t>::max();
constexpr laddr_t L_ADDR_NULL = std::numeric_limits<laddr_t>::max();
constexpr laddr_t L_ADDR_ROOT = std::numeric_limits<laddr_t>::max() - 1;
constexpr laddr_t L_ADDR_LBAT = std::numeric_limits<laddr_t>::max() - 2;

struct __attribute((packed)) laddr_le_t {
  ceph_le64 laddr = ceph_le64(L_ADDR_NULL);

  laddr_le_t() = default;
  laddr_le_t(const laddr_le_t &) = default;
  explicit laddr_le_t(const laddr_t &addr)
    : laddr(ceph_le64(addr)) {}

  operator laddr_t() const {
    return laddr_t(laddr);
  }
  laddr_le_t& operator=(laddr_t addr) {
    ceph_le64 val;
    val = addr;
    laddr = val;
    return *this;
  }
};

// logical offset, see LBAManager, TransactionManager
using extent_len_t = uint32_t;
constexpr extent_len_t EXTENT_LEN_MAX =
  std::numeric_limits<extent_len_t>::max();

using extent_len_le_t = ceph_le32;
inline extent_len_le_t init_extent_len_le(extent_len_t len) {
  return ceph_le32(len);
}

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
 * writeback.
 *
 * Note that any new extent type needs to be added to
 * Cache::get_extent_by_type in cache.cc
 */
enum class extent_types_t : uint8_t {
  ROOT = 0,
  LADDR_INTERNAL = 1,
  LADDR_LEAF = 2,
  OMAP_INNER = 3,
  OMAP_LEAF = 4,
  ONODE_BLOCK_STAGED = 5,
  COLL_BLOCK = 6,
  OBJECT_DATA_BLOCK = 7,
  RETIRED_PLACEHOLDER = 8,
  RBM_ALLOC_INFO = 9,
  // Test Block Types
  TEST_BLOCK = 10,
  TEST_BLOCK_PHYSICAL = 11,
  // None and the number of valid extent_types_t
  NONE = 12,
};
constexpr auto EXTENT_TYPES_MAX = static_cast<uint8_t>(extent_types_t::NONE);

constexpr bool is_logical_type(extent_types_t type) {
  switch (type) {
  case extent_types_t::ROOT:
  case extent_types_t::LADDR_INTERNAL:
  case extent_types_t::LADDR_LEAF:
    return false;
  default:
    return true;
  }
}

std::ostream &operator<<(std::ostream &out, extent_types_t t);

/* description of a new physical extent */
struct extent_t {
  extent_types_t type;  ///< type of extent
  laddr_t addr;         ///< laddr of extent (L_ADDR_NULL for non-logical)
  ceph::bufferlist bl;  ///< payload, bl.length() == length, aligned
};

using extent_version_t = uint32_t;
constexpr extent_version_t EXTENT_VERSION_NULL = 0;

/* description of a mutation to a physical extent */
struct delta_info_t {
  extent_types_t type = extent_types_t::NONE;  ///< delta type
  paddr_t paddr;                               ///< physical address
  laddr_t laddr = L_ADDR_NULL;                 ///< logical address
  uint32_t prev_crc = 0;
  uint32_t final_crc = 0;
  segment_off_t length = NULL_SEG_OFF;         ///< extent length
  extent_version_t pversion;                   ///< prior version
  ceph::bufferlist bl;                         ///< payload

  DENC(delta_info_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.type, p);
    denc(v.paddr, p);
    denc(v.laddr, p);
    denc(v.prev_crc, p);
    denc(v.final_crc, p);
    denc(v.length, p);
    denc(v.pversion, p);
    denc(v.bl, p);
    DENC_FINISH(p);
  }

  bool operator==(const delta_info_t &rhs) const {
    return (
      type == rhs.type &&
      paddr == rhs.paddr &&
      laddr == rhs.laddr &&
      prev_crc == rhs.prev_crc &&
      final_crc == rhs.final_crc &&
      length == rhs.length &&
      pversion == rhs.pversion &&
      bl == rhs.bl
    );
  }

  friend std::ostream &operator<<(std::ostream &lhs, const delta_info_t &rhs);
};

std::ostream &operator<<(std::ostream &lhs, const delta_info_t &rhs);

struct record_t {
  std::vector<extent_t> extents;
  std::vector<delta_info_t> deltas;

  std::size_t get_raw_data_size() const {
    auto extent_size = std::accumulate(
        extents.begin(), extents.end(), 0,
        [](uint64_t sum, auto& extent) {
          return sum + extent.bl.length();
        }
    );
    auto delta_size = std::accumulate(
        deltas.begin(), deltas.end(), 0,
        [](uint64_t sum, auto& delta) {
          return sum + delta.bl.length();
        }
    );
    return extent_size + delta_size;
  }
};

class object_data_t {
  laddr_t reserved_data_base = L_ADDR_NULL;
  extent_len_t reserved_data_len = 0;

  bool dirty = false;
public:
  object_data_t(
    laddr_t reserved_data_base,
    extent_len_t reserved_data_len)
    : reserved_data_base(reserved_data_base),
      reserved_data_len(reserved_data_len) {}

  laddr_t get_reserved_data_base() const {
    return reserved_data_base;
  }

  extent_len_t get_reserved_data_len() const {
    return reserved_data_len;
  }

  bool is_null() const {
    return reserved_data_base == L_ADDR_NULL;
  }

  bool must_update() const {
    return dirty;
  }

  void update_reserved(
    laddr_t base,
    extent_len_t len) {
    dirty = true;
    reserved_data_base = base;
    reserved_data_len = len;
  }

  void update_len(
    extent_len_t len) {
    dirty = true;
    reserved_data_len = len;
  }

  void clear() {
    dirty = true;
    reserved_data_base = L_ADDR_NULL;
    reserved_data_len = 0;
  }
};

struct __attribute__((packed)) object_data_le_t {
  laddr_le_t reserved_data_base = laddr_le_t(L_ADDR_NULL);
  extent_len_le_t reserved_data_len = init_extent_len_le(0);

  void update(const object_data_t &nroot) {
    reserved_data_base = nroot.get_reserved_data_base();
    reserved_data_len = init_extent_len_le(nroot.get_reserved_data_len());
  }

  object_data_t get() const {
    return object_data_t(
      reserved_data_base,
      reserved_data_len);
  }
};

struct omap_root_t {
  laddr_t addr = L_ADDR_NULL;
  depth_t depth = 0;
  laddr_t hint = L_ADDR_MIN;
  bool mutated = false;

  omap_root_t() = default;
  omap_root_t(laddr_t addr, depth_t depth, laddr_t addr_min)
    : addr(addr),
      depth(depth),
      hint(addr_min) {}

  omap_root_t(const omap_root_t &o) = default;
  omap_root_t(omap_root_t &&o) = default;
  omap_root_t &operator=(const omap_root_t &o) = default;
  omap_root_t &operator=(omap_root_t &&o) = default;

  bool is_null() const {
    return addr == L_ADDR_NULL;
  }

  bool must_update() const {
    return mutated;
  }
  
  void update(laddr_t _addr, depth_t _depth, laddr_t _hint) {
    mutated = true;
    addr = _addr;
    depth = _depth;
    hint = _hint;
  }
  
  laddr_t get_location() const {
    return addr;
  }

  depth_t get_depth() const {
    return depth;
  }

  laddr_t get_hint() const {
    return hint;
  }
};

class __attribute__((packed)) omap_root_le_t {
  laddr_le_t addr = laddr_le_t(L_ADDR_NULL);
  depth_le_t depth = init_depth_le(0);

public: 
  omap_root_le_t() = default;
  
  omap_root_le_t(laddr_t addr, depth_t depth)
    : addr(addr), depth(init_depth_le(depth)) {}

  omap_root_le_t(const omap_root_le_t &o) = default;
  omap_root_le_t(omap_root_le_t &&o) = default;
  omap_root_le_t &operator=(const omap_root_le_t &o) = default;
  omap_root_le_t &operator=(omap_root_le_t &&o) = default;
  
  void update(const omap_root_t &nroot) {
    addr = nroot.get_location();
    depth = init_depth_le(nroot.get_depth());
  }
  
  omap_root_t get(laddr_t hint) const {
    return omap_root_t(addr, depth, hint);
  }
};

/**
 * lba_root_t 
 */
class __attribute__((packed)) lba_root_t {
  paddr_le_t root_addr;
  depth_le_t depth = init_extent_len_le(0);
  
public:
  lba_root_t() = default;
  
  lba_root_t(paddr_t addr, depth_t depth)
    : root_addr(addr), depth(init_depth_le(depth)) {}

  lba_root_t(const lba_root_t &o) = default;
  lba_root_t(lba_root_t &&o) = default;
  lba_root_t &operator=(const lba_root_t &o) = default;
  lba_root_t &operator=(lba_root_t &&o) = default;
  
  paddr_t get_location() const {
    return root_addr;
  }

  void set_location(paddr_t location) {
    root_addr = location;
  }

  depth_t get_depth() const {
    return depth;
  }

  void set_depth(depth_t ndepth) {
    depth = ndepth;
  }

  void adjust_addrs_from_base(paddr_t base) {
    paddr_t _root_addr = root_addr;
    if (_root_addr.is_relative()) {
      root_addr = base.add_record_relative(_root_addr);
    }
  }
};

class coll_root_t {
  laddr_t addr = L_ADDR_NULL;
  extent_len_t size = 0;

  bool mutated = false;

public:
  coll_root_t() = default;
  coll_root_t(laddr_t addr, extent_len_t size) : addr(addr), size(size) {}

  coll_root_t(const coll_root_t &o) = default;
  coll_root_t(coll_root_t &&o) = default;
  coll_root_t &operator=(const coll_root_t &o) = default;
  coll_root_t &operator=(coll_root_t &&o) = default;
  
  bool must_update() const {
    return mutated;
  }
  
  void update(laddr_t _addr, extent_len_t _s) {
    mutated = true;
    addr = _addr;
    size = _s;
  }
  
  laddr_t get_location() const {
    return addr;
  }

  extent_len_t get_size() const {
    return size;
  }
};

/**
 * coll_root_le_t
 *
 * Information for locating CollectionManager information, to be embedded
 * in root block.
 */
class __attribute__((packed)) coll_root_le_t {
  laddr_le_t addr;
  extent_len_le_t size = init_extent_len_le(0);
  
public:
  coll_root_le_t() = default;
  
  coll_root_le_t(laddr_t laddr, segment_off_t size)
    : addr(laddr), size(init_extent_len_le(size)) {}


  coll_root_le_t(const coll_root_le_t &o) = default;
  coll_root_le_t(coll_root_le_t &&o) = default;
  coll_root_le_t &operator=(const coll_root_le_t &o) = default;
  coll_root_le_t &operator=(coll_root_le_t &&o) = default;
  
  void update(const coll_root_t &nroot) {
    addr = nroot.get_location();
    size = init_extent_len_le(nroot.get_size());
  }
  
  coll_root_t get() const {
    return coll_root_t(addr, size);
  }
};


/**
 * root_t
 *
 * Contains information required to find metadata roots.
 * TODO: generalize this to permit more than one lba_manager implementation
 */
struct __attribute__((packed)) root_t {
  using meta_t = std::map<std::string, std::string>;

  static constexpr int MAX_META_LENGTH = 1024;

  lba_root_t lba_root;
  laddr_le_t onode_root;
  coll_root_le_t collection_root;

  char meta[MAX_META_LENGTH];

  root_t() {
    set_meta(meta_t{});
  }

  void adjust_addrs_from_base(paddr_t base) {
    lba_root.adjust_addrs_from_base(base);
  }

  meta_t get_meta() {
    bufferlist bl;
    bl.append(ceph::buffer::create_static(MAX_META_LENGTH, meta));
    meta_t ret;
    auto iter = bl.cbegin();
    decode(ret, iter);
    return ret;
  }

  void set_meta(const meta_t &m) {
    ceph::bufferlist bl;
    encode(m, bl);
    ceph_assert(bl.length() < MAX_META_LENGTH);
    bl.rebuild();
    auto &bptr = bl.front();
    ::memset(meta, 0, MAX_META_LENGTH);
    ::memcpy(meta, bptr.c_str(), bl.length());
  }
};

using blk_id_t = uint64_t;
constexpr blk_id_t NULL_BLK_ID =
  std::numeric_limits<blk_id_t>::max();

// use absolute address
using blk_paddr_t = uint64_t;
struct rbm_alloc_delta_t {
  enum class op_types_t : uint8_t {
    NONE = 0,
    SET = 1,
    CLEAR = 2
  };
  std::vector<std::pair<paddr_t, size_t>> alloc_blk_ranges;
  op_types_t op = op_types_t::NONE;

  rbm_alloc_delta_t() = default;

  DENC(rbm_alloc_delta_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.alloc_blk_ranges, p);
    denc(v.op, p);
    DENC_FINISH(p);
  }
};

paddr_t convert_blk_paddr_to_paddr(blk_paddr_t addr, size_t block_size,
    uint32_t blocks_per_segment, device_id_t d_id);
blk_paddr_t convert_paddr_to_blk_paddr(paddr_t addr, size_t block_size,
	uint32_t blocks_per_segment);

struct extent_info_t {
  extent_types_t type = extent_types_t::NONE;
  laddr_t addr = L_ADDR_NULL;
  extent_len_t len = 0;

  extent_info_t() = default;
  extent_info_t(const extent_t &et)
    : type(et.type), addr(et.addr), len(et.bl.length()) {}

  DENC(extent_info_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.type, p);
    denc(v.addr, p);
    denc(v.len, p);
    DENC_FINISH(p);
  }
};

using segment_nonce_t = uint32_t;

/**
 * Segment header
 *
 * Every segment contains and encode segment_header_t in the first block.
 * Our strategy for finding the journal replay point is:
 * 1) Find the segment with the highest journal_segment_seq
 * 2) Replay starting at record located at that segment's journal_tail
 */
struct segment_header_t {
  segment_seq_t journal_segment_seq;
  segment_id_t physical_segment_id; // debugging

  journal_seq_t journal_tail;
  segment_nonce_t segment_nonce;
  bool out_of_line;

  DENC(segment_header_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.journal_segment_seq, p);
    denc(v.physical_segment_id, p);
    denc(v.journal_tail, p);
    denc(v.segment_nonce, p);
    denc(v.out_of_line, p);
    DENC_FINISH(p);
  }
};
std::ostream &operator<<(std::ostream &out, const segment_header_t &header);

struct record_header_t {
  // Fixed portion
  extent_len_t  mdlength;       // block aligned, length of metadata
  extent_len_t  dlength;        // block aligned, length of data
  uint32_t deltas;                // number of deltas
  uint32_t extents;               // number of extents
  segment_nonce_t segment_nonce;// nonce of containing segment
  segment_off_t committed_to;   // records in this segment prior to committed_to
                                // have been fully written
  checksum_t data_crc;          // crc of data payload


  DENC(record_header_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.mdlength, p);
    denc(v.dlength, p);
    denc(v.deltas, p);
    denc(v.extents, p);
    denc(v.segment_nonce, p);
    denc(v.committed_to, p);
    denc(v.data_crc, p);
    DENC_FINISH(p);
  }
};

std::ostream &operator<<(std::ostream &out, const extent_info_t &header);

struct record_size_t {
  extent_len_t raw_mdlength = 0;
  extent_len_t mdlength = 0;
  extent_len_t dlength = 0;
};

extent_len_t get_encoded_record_raw_mdlength(
  const record_t &record,
  size_t block_size);

/**
 * Return <mdlength, dlength> pair denoting length of
 * metadata and blocks respectively.
 */
record_size_t get_encoded_record_length(
  const record_t &record,
  size_t block_size);

ceph::bufferlist encode_record(
  record_size_t rsize,
  record_t &&record,
  size_t block_size,
  segment_off_t committed_to,
  segment_nonce_t current_segment_nonce = 0);

/// scan segment for end incrementally
struct scan_valid_records_cursor {
  bool last_valid_header_found = false;
  paddr_t offset;
  paddr_t last_committed;

  struct found_record_t {
    paddr_t offset;
    record_header_t header;
    bufferlist mdbuffer;

    found_record_t(
      paddr_t offset,
      const record_header_t &header,
      const bufferlist &mdbuffer)
      : offset(offset), header(header), mdbuffer(mdbuffer) {}
  };
  std::deque<found_record_t> pending_records;

  bool is_complete() const {
    return last_valid_header_found && pending_records.empty();
  }

  paddr_t get_offset() const {
    return offset;
  }

  scan_valid_records_cursor(
    paddr_t offset)
    : offset(offset) {}
};

}

WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::seastore_meta_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::segment_id_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::paddr_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::journal_seq_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::delta_info_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::record_header_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::extent_info_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::segment_header_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::rbm_alloc_delta_t)

template<>
struct denc_traits<crimson::os::seastore::device_type_t> {
  static constexpr bool supported = true;
  static constexpr bool featured = false;
  static constexpr bool bounded = true;
  static constexpr bool need_contiguous = false;

  static void bound_encode(
    const crimson::os::seastore::device_type_t &o,
    size_t& p,
    uint64_t f=0) {
    p += sizeof(crimson::os::seastore::device_type_t);
  }
  template<class It>
  static std::enable_if_t<!is_const_iterator_v<It>>
  encode(
    const crimson::os::seastore::device_type_t &o,
    It& p,
    uint64_t f=0) {
    get_pos_add<crimson::os::seastore::device_type_t>(p) = o;
  }
  template<class It>
  static std::enable_if_t<is_const_iterator_v<It>>
  decode(
    crimson::os::seastore::device_type_t& o,
    It& p,
    uint64_t f=0) {
    o = get_pos_add<crimson::os::seastore::device_type_t>(p);
  }
  static void decode(
    crimson::os::seastore::device_type_t& o,
    ceph::buffer::list::const_iterator &p) {
    p.copy(sizeof(crimson::os::seastore::device_type_t),
           reinterpret_cast<char*>(&o));
  }
};
