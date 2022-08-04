// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <limits>
#include <numeric>
#include <optional>
#include <iostream>
#include <vector>
#include <boost/core/ignore_unused.hpp>

#include <seastar/core/lowres_clock.hh>

#include "include/byteorder.h"
#include "include/denc.h"
#include "include/buffer.h"
#include "include/uuid.h"
#include "include/interval_set.h"

namespace crimson::os::seastore {

/* using a special xattr key "omap_header" to store omap header */
  const std::string OMAP_HEADER_XATTR_KEY = "omap_header";

/*
 * Note: NULL value is usually the default and max value.
 */

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

std::ostream& operator<<(std::ostream& out, const seastore_meta_t& meta);

bool is_aligned(uint64_t offset, uint64_t alignment);

// identifies a specific physical device within seastore
using device_id_t = uint8_t;

constexpr auto DEVICE_ID_BITS = std::numeric_limits<device_id_t>::digits;

constexpr uint16_t SEGMENT_ID_LEN_BITS = 24;

// 1 bit to identify address type

// segment ids without a device id encapsulated
using device_segment_id_t = uint32_t;

constexpr device_id_t DEVICE_ID_GLOBAL_MAX =
  std::numeric_limits<device_id_t>::max();
// the max value regardless of addrs_type_t prefix
constexpr device_id_t DEVICE_ID_MAX = DEVICE_ID_GLOBAL_MAX >> 1;
constexpr device_id_t DEVICE_ID_NULL = DEVICE_ID_MAX;
constexpr device_id_t DEVICE_ID_RECORD_RELATIVE = DEVICE_ID_MAX - 1;
constexpr device_id_t DEVICE_ID_BLOCK_RELATIVE = DEVICE_ID_MAX - 2;
constexpr device_id_t DEVICE_ID_DELAYED = DEVICE_ID_MAX - 3;
constexpr device_id_t DEVICE_ID_FAKE = DEVICE_ID_MAX - 4;
constexpr device_id_t DEVICE_ID_ZERO = DEVICE_ID_MAX - 5;
constexpr device_id_t DEVICE_ID_MAX_VALID = DEVICE_ID_MAX - 6;

struct device_id_printer_t {
  device_id_t id;
};

std::ostream &operator<<(std::ostream &out, const device_id_printer_t &id);

constexpr device_segment_id_t DEVICE_SEGMENT_ID_MAX =
  (1 << SEGMENT_ID_BITS) - 1;

// Identifies segment location on disk, see SegmentManager,
struct segment_id_t {
private:
  // internal segment id type of segment_id_t, basically
  // this is a unsigned int with the top "DEVICE_ID_BITS"
  // bits representing the id of the device on which the
  // segment resides
  using internal_segment_id_t = uint32_t;

  // mask for segment manager id
  static constexpr internal_segment_id_t SM_ID_MASK =
    0xFF << (std::numeric_limits<internal_segment_id_t>::digits - DEVICE_ID_BITS);
  // default internal segment id
  static constexpr internal_segment_id_t DEFAULT_INTERNAL_SEG_ID =
    (std::numeric_limits<internal_segment_id_t>::max() >> 1) - 1;

  internal_segment_id_t segment = DEFAULT_INTERNAL_SEG_ID;

  constexpr segment_id_t(uint32_t encoded) : segment(encoded) {}

public:
  segment_id_t() = default;
  constexpr segment_id_t(device_id_t id, device_segment_id_t segment)
    : segment(make_internal(segment, id)) {}

  [[gnu::always_inline]]
  device_id_t device_id() const {
    return internal_to_device(segment);
  }

  [[gnu::always_inline]]
  constexpr device_segment_id_t device_segment_id() const {
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
    std::numeric_limits<internal_segment_id_t>::digits - DEVICE_ID_BITS
  );

  static inline device_id_t internal_to_device(internal_segment_id_t id) {
    return static_cast<device_id_t>((id & SM_ID_MASK) >> segment_bits);
  }

  constexpr static inline device_segment_id_t internal_to_segment(
    internal_segment_id_t id) {
    return id & (~SM_ID_MASK);
  }

  constexpr static inline internal_segment_id_t make_internal(
    device_segment_id_t id,
    device_id_t sm_id) {
    return static_cast<internal_segment_id_t>(id) |
      (static_cast<internal_segment_id_t>(sm_id) << segment_bits);
  }

  friend struct segment_id_le_t;
  friend struct seg_paddr_t;
  friend struct paddr_t;
  friend struct paddr_le_t;
};

std::ostream &operator<<(std::ostream &out, const segment_id_t&);

// ondisk type of segment_id_t
struct __attribute((packed)) segment_id_le_t {
  ceph_le32 segment = ceph_le32(segment_id_t::DEFAULT_INTERNAL_SEG_ID);

  segment_id_le_t(const segment_id_t id) :
    segment(ceph_le32(id.segment)) {}

  operator segment_id_t() const {
    return segment_id_t(segment);
  }
};

constexpr segment_id_t MIN_SEG_ID = segment_id_t(0, 0);
constexpr segment_id_t MAX_SEG_ID = segment_id_t(
  DEVICE_ID_MAX,
  DEVICE_SEGMENT_ID_MAX
);
// for tests which generate fake paddrs
constexpr segment_id_t NULL_SEG_ID = MAX_SEG_ID;
constexpr segment_id_t FAKE_SEG_ID = segment_id_t(DEVICE_ID_FAKE, 0);

// Offset within a segment on disk, see SegmentManager
// may be negative for relative offsets
using seastore_off_t = int32_t;
constexpr seastore_off_t MAX_SEG_OFF =
  std::numeric_limits<seastore_off_t>::max();
constexpr seastore_off_t NULL_SEG_OFF = MAX_SEG_OFF;

struct seastore_off_printer_t {
  seastore_off_t off;
};

std::ostream &operator<<(std::ostream&, const seastore_off_printer_t&);

/* Monotonically increasing segment seq, uniquely identifies
 * the incarnation of a segment */
using segment_seq_t = uint32_t;
static constexpr segment_seq_t MAX_SEG_SEQ =
  std::numeric_limits<segment_seq_t>::max();
static constexpr segment_seq_t NULL_SEG_SEQ = MAX_SEG_SEQ;
static constexpr segment_seq_t MAX_VALID_SEG_SEQ = MAX_SEG_SEQ - 2;

enum class segment_type_t : uint8_t {
  JOURNAL = 0,
  OOL,
  NULL_SEG,
};

std::ostream& operator<<(std::ostream& out, segment_type_t t);

struct segment_seq_printer_t {
  segment_seq_t seq;
};

std::ostream& operator<<(std::ostream& out, segment_seq_printer_t seq);

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
    device_to_segments.resize(DEVICE_ID_MAX_VALID);
  }
  void add_device(device_id_t device, std::size_t segments, const T& init) {
    ceph_assert(device <= DEVICE_ID_MAX_VALID);
    ceph_assert(device_to_segments[device].size() == 0);
    ceph_assert(segments > 0);
    device_to_segments[device].resize(segments, init);
    total_segments += segments;
  }
  void clear() {
    device_to_segments.clear();
    device_to_segments.resize(DEVICE_ID_MAX_VALID);
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

  bool contains(segment_id_t id) {
    bool b = id.device_id() < device_to_segments.size();
    if (!b) {
      return b;
    }
    b = id.device_segment_id() < device_to_segments[id.device_id()].size();
    return b;
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

    /// points at current device, or DEVICE_ID_MAX_VALID if is_end()
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
      return device_id == DEVICE_ID_MAX_VALID;
    }

    void find_valid() {
      assert(!is_end());
      auto &device_vec = parent.device_to_segments[device_id];
      if (device_vec.size() == 0 ||
	  device_segment_id == device_vec.size()) {
	while (++device_id < DEVICE_ID_MAX_VALID &&
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
      if (device_id == DEVICE_ID_MAX_VALID) {
	return end_iterator(parent);
      } else {
	auto ret = iterator{parent, device_id, device_segment_id};
	ret.find_valid();
	return ret;
      }
    }

    static iterator end_iterator(
      decltype(parent) &parent) {
      return iterator{parent, DEVICE_ID_MAX_VALID, 0};
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

using block_off_t = uint64_t;
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
constexpr uint16_t DEV_ADDR_LEN_BITS = 64 - DEVICE_ID_BITS;
static constexpr uint16_t SEG_OFF_LEN_BITS = 32;
enum class addr_types_t : uint8_t {
  SEGMENT = 0,
  RANDOM_BLOCK = 1
};
struct seg_paddr_t;
struct blk_paddr_t;
struct paddr_t {
protected:
  using common_addr_t = uint64_t;
  common_addr_t dev_addr;
private:
  constexpr paddr_t(segment_id_t seg, seastore_off_t offset)
    : dev_addr((static_cast<common_addr_t>(seg.segment)
	<< SEG_OFF_LEN_BITS) | static_cast<uint32_t>(offset)) {}
  constexpr paddr_t(common_addr_t val) : dev_addr(val) {}
  constexpr paddr_t(device_id_t d_id, block_off_t offset)
    : dev_addr(
      (static_cast<common_addr_t>(d_id) <<
	(std::numeric_limits<block_off_t>::digits - DEVICE_ID_BITS)) |
      (offset & (std::numeric_limits<block_off_t>::max() >> DEVICE_ID_BITS)))
  {}
public:
  static constexpr paddr_t make_seg_paddr(
    segment_id_t seg, seastore_off_t offset) {
    return paddr_t(seg, offset);
  }
  static constexpr paddr_t make_seg_paddr(
    device_id_t device,
    device_segment_id_t seg,
    seastore_off_t offset) {
    return paddr_t(segment_id_t(device, seg), offset);
  }
  // P_ADDR_MAX == P_ADDR_NULL == paddr_t{}
  constexpr paddr_t() : paddr_t(NULL_SEG_ID, NULL_SEG_OFF) {}
  static constexpr paddr_t make_blk_paddr(
    device_id_t device,
    block_off_t offset) {
    return paddr_t(device, offset);
  }

  device_id_t get_device_id() const {
    return static_cast<device_id_t>(dev_addr >> DEV_ADDR_LEN_BITS);
  }
  addr_types_t get_addr_type() const {
    return (addr_types_t)((dev_addr
	    >> (std::numeric_limits<common_addr_t>::digits - 1)) & 1);
  }

  paddr_t add_offset(int32_t o) const;
  paddr_t add_relative(paddr_t o) const;
  paddr_t add_block_relative(paddr_t o) const;
  paddr_t add_record_relative(paddr_t o) const;
  paddr_t maybe_relative_to(paddr_t base) const;

  seg_paddr_t& as_seg_paddr();
  const seg_paddr_t& as_seg_paddr() const;
  blk_paddr_t& as_blk_paddr();
  const blk_paddr_t& as_blk_paddr() const;

  paddr_t operator-(paddr_t rhs) const;
  paddr_t operator+(int32_t o) const {
    return add_offset(o);
  }

  bool is_delayed() const {
    return get_device_id() == DEVICE_ID_DELAYED;
  }
  bool is_block_relative() const {
    return get_device_id() == DEVICE_ID_BLOCK_RELATIVE;
  }
  bool is_record_relative() const {
    return get_device_id() == DEVICE_ID_RECORD_RELATIVE;
  }
  bool is_relative() const {
    return is_block_relative() || is_record_relative();
  }
  /// Denotes special null addr
  bool is_null() const {
    return get_device_id() == DEVICE_ID_NULL;
  }
  /// Denotes special zero addr
  bool is_zero() const {
    return get_device_id() == DEVICE_ID_ZERO;
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

  bool is_absolute() const {
    return get_device_id() <= DEVICE_ID_MAX_VALID;
  }

  DENC(paddr_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.dev_addr, p);
    DENC_FINISH(p);
  }
  friend struct paddr_le_t;
  friend struct seg_paddr_t;

  auto operator<=>(const paddr_t &) const = default;
};

std::ostream &operator<<(std::ostream &out, const paddr_t &rhs);

struct seg_paddr_t : public paddr_t {
  static constexpr uint64_t SEG_OFF_MASK = std::numeric_limits<uint32_t>::max();
  // mask for segment manager id
  static constexpr uint64_t SEG_ID_MASK =
    static_cast<common_addr_t>(0xFFFFFFFF) << SEG_OFF_LEN_BITS;

  seg_paddr_t(const seg_paddr_t&) = delete;
  seg_paddr_t(seg_paddr_t&) = delete;
  seg_paddr_t& operator=(const seg_paddr_t&) = delete;
  seg_paddr_t& operator=(seg_paddr_t&) = delete;
  segment_id_t get_segment_id() const {
    return segment_id_t((dev_addr & SEG_ID_MASK) >> SEG_OFF_LEN_BITS);
  }
  seastore_off_t get_segment_off() const {
    return seastore_off_t(dev_addr & SEG_OFF_MASK);
  }
  void set_segment_id(const segment_id_t id) {
    dev_addr &= static_cast<common_addr_t>(
      std::numeric_limits<device_segment_id_t>::max());
    dev_addr |= static_cast<common_addr_t>(id.segment) << SEG_OFF_LEN_BITS;
  }
  void set_segment_off(const seastore_off_t off) {
    dev_addr &= static_cast<common_addr_t>(
      std::numeric_limits<device_segment_id_t>::max()) << SEG_OFF_LEN_BITS;
    dev_addr |= (uint32_t)off;
  }

  paddr_t add_offset(seastore_off_t o) const {
    return paddr_t::make_seg_paddr(get_segment_id(), get_segment_off() + o);
  }

  paddr_t add_relative(paddr_t o) const {
    assert(o.is_relative());
    seg_paddr_t& s = o.as_seg_paddr();
    return paddr_t::make_seg_paddr(get_segment_id(),
	    get_segment_off() + s.get_segment_off());
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
    seg_paddr_t& r = rhs.as_seg_paddr();
    assert(rhs.is_relative() && is_relative());
    assert(r.get_segment_id() == get_segment_id());
    return paddr_t::make_seg_paddr(
      segment_id_t{DEVICE_ID_BLOCK_RELATIVE, 0},
      get_segment_off() - r.get_segment_off()
      );
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
    if (is_block_relative()) {
      return base.add_block_relative(*this);
    } else
      return *this;
  }
};

constexpr block_off_t BLK_OFF_MAX =
  std::numeric_limits<block_off_t>::max() >> DEVICE_ID_BITS;
struct blk_paddr_t : public paddr_t {

  blk_paddr_t(const blk_paddr_t&) = delete;
  blk_paddr_t(blk_paddr_t&) = delete;
  blk_paddr_t& operator=(const blk_paddr_t&) = delete;
  blk_paddr_t& operator=(blk_paddr_t&) = delete;

  static constexpr uint64_t BLK_OFF_MASK = std::numeric_limits<uint64_t>::max()
    >> DEVICE_ID_BITS;
  void set_block_off(const block_off_t off) {
    check_blk_off_valid(off);
    uint64_t val = off & BLK_OFF_MASK;
    dev_addr |= val;
  }
  block_off_t get_block_off() const {
    const block_off_t ret = static_cast<block_off_t>(
      dev_addr & BLK_OFF_MASK);
    check_blk_off_valid(ret);
    return ret;
  }

  paddr_t add_offset(seastore_off_t o) const {
    return paddr_t::make_blk_paddr(get_device_id(), get_block_off() + o);
  }

  paddr_t add_relative(paddr_t o) const {
    seastore_off_t off;
    ceph_assert(o.get_addr_type() == addr_types_t::SEGMENT);
    // segment addr is allocated when alloc_new_extent is called.
    // But, if random block device is used,
    // segment-based relative addr needs to be added to block addr
    off = o.as_seg_paddr().get_segment_off();
    return add_offset(off);
  }

  // all blk_paddr_t are absolute, relative addrs are always segmented
  paddr_t maybe_relative_to(paddr_t base) const {
    return *this;
  }

  paddr_t add_block_relative(paddr_t o) const {
    assert(o.is_block_relative());
    return add_relative(o);
  }

  paddr_t add_record_relative(paddr_t o) const {
    assert(o.is_record_relative());
    return add_relative(o);
  }

private:
  void check_blk_off_valid(const block_off_t offset) const {
    assert(offset <= BLK_OFF_MAX);
  }
};

constexpr paddr_t P_ADDR_MIN = paddr_t::make_seg_paddr(MIN_SEG_ID, 0);
constexpr paddr_t P_ADDR_MAX = paddr_t::make_seg_paddr(MAX_SEG_ID, MAX_SEG_OFF);
constexpr paddr_t P_ADDR_NULL = paddr_t{}; // P_ADDR_MAX == P_ADDR_NULL == paddr_t{}
constexpr paddr_t P_ADDR_ZERO = paddr_t::make_seg_paddr(
  DEVICE_ID_ZERO, 0, 0);

constexpr paddr_t make_record_relative_paddr(seastore_off_t off) {
  return paddr_t::make_seg_paddr(
    segment_id_t{DEVICE_ID_RECORD_RELATIVE, 0},
    off);
}
constexpr paddr_t make_block_relative_paddr(seastore_off_t off) {
  return paddr_t::make_seg_paddr(
    segment_id_t{DEVICE_ID_BLOCK_RELATIVE, 0},
    off);
}
constexpr paddr_t make_fake_paddr(seastore_off_t off) {
  return paddr_t::make_seg_paddr(FAKE_SEG_ID, off);
}
constexpr paddr_t make_delayed_temp_paddr(seastore_off_t off) {
  return paddr_t::make_seg_paddr(
    segment_id_t{DEVICE_ID_DELAYED, 0},
    off);
}

struct __attribute((packed)) paddr_le_t {
  ceph_le64 dev_addr =
    ceph_le64(P_ADDR_NULL.dev_addr);

  using orig_type = paddr_t;

  paddr_le_t() = default;
  paddr_le_t(const paddr_t &addr) : dev_addr(ceph_le64(addr.dev_addr)) {}

  operator paddr_t() const {
    return paddr_t{dev_addr};
  }
};

using objaddr_t = uint32_t;
constexpr objaddr_t OBJ_ADDR_MAX = std::numeric_limits<objaddr_t>::max();
constexpr objaddr_t OBJ_ADDR_NULL = OBJ_ADDR_MAX;

enum class placement_hint_t {
  HOT = 0,   // The default user hint that expects mutations or retirement
  COLD,      // Expect no mutations and no retirement in the near future
  REWRITE,   // Hint for the internal rewrites
  NUM_HINTS  // Constant for number of hints or as NULL
};

constexpr auto PLACEMENT_HINT_NULL = placement_hint_t::NUM_HINTS;

std::ostream& operator<<(std::ostream& out, placement_hint_t h);

enum alignas(4) device_type_t : uint_fast8_t {
  NONE = 0,
  SEGMENTED, // i.e. Hard_Disk, SATA_SSD, NAND_NVME
  RANDOM_BLOCK, // i.e. RANDOM_BD
  PMEM, // i.e. NVDIMM, PMEM
  NUM_TYPES
};

std::ostream& operator<<(std::ostream& out, device_type_t t);

bool can_delay_allocation(device_type_t type);
device_type_t string_to_device_type(std::string type);

/* Monotonically increasing identifier for the location of a
 * journal_record.
 */
// JOURNAL_SEQ_NULL == JOURNAL_SEQ_MAX == journal_seq_t{}
struct journal_seq_t {
  segment_seq_t segment_seq = NULL_SEG_SEQ;
  paddr_t offset = P_ADDR_NULL;

  journal_seq_t add_offset(seastore_off_t o) const {
    return {segment_seq, offset.add_offset(o)};
  }

  DENC(journal_seq_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.segment_seq, p);
    denc(v.offset, p);
    DENC_FINISH(p);
  }

  bool operator==(const journal_seq_t &o) const { return cmp(o) == 0; }
  bool operator!=(const journal_seq_t &o) const { return cmp(o) != 0; }
  bool operator<(const journal_seq_t &o) const { return cmp(o) < 0; }
  bool operator<=(const journal_seq_t &o) const { return cmp(o) <= 0; }
  bool operator>(const journal_seq_t &o) const { return cmp(o) > 0; }
  bool operator>=(const journal_seq_t &o) const { return cmp(o) >= 0; }

private:
  int cmp(const journal_seq_t &other) const {
    if (segment_seq > other.segment_seq) {
      return 1;
    } else if (segment_seq < other.segment_seq) {
      return -1;
    }
    using ret_t = std::pair<int64_t, segment_id_t>;
    auto to_pair = [](const paddr_t &addr) -> ret_t {
      if (addr.get_addr_type() == addr_types_t::SEGMENT) {
	auto &seg_addr = addr.as_seg_paddr();
	return ret_t(seg_addr.get_segment_off(), seg_addr.get_segment_id());
      } else if (addr.get_addr_type() == addr_types_t::RANDOM_BLOCK) {
	auto &blk_addr = addr.as_blk_paddr();
	return ret_t(blk_addr.get_block_off(), MAX_SEG_ID);
      } else {
	assert(0 == "impossible");
	return ret_t(0, MAX_SEG_ID);
      }
    };
    auto left = to_pair(offset);
    auto right = to_pair(other.offset);
    if (left > right) {
      return 1;
    } else if (left < right) {
      return -1;
    } else {
      return 0;
    }
  }
};

std::ostream &operator<<(std::ostream &out, const journal_seq_t &seq);

constexpr journal_seq_t JOURNAL_SEQ_MIN{
  0,
  P_ADDR_MIN
};
constexpr journal_seq_t JOURNAL_SEQ_MAX{
  MAX_SEG_SEQ,
  P_ADDR_MAX
};
// JOURNAL_SEQ_NULL == JOURNAL_SEQ_MAX == journal_seq_t{}
constexpr journal_seq_t JOURNAL_SEQ_NULL = JOURNAL_SEQ_MAX;

// logical addr, see LBAManager, TransactionManager
using laddr_t = uint64_t;
constexpr laddr_t L_ADDR_MIN = std::numeric_limits<laddr_t>::min();
constexpr laddr_t L_ADDR_MAX = std::numeric_limits<laddr_t>::max();
constexpr laddr_t L_ADDR_NULL = L_ADDR_MAX;
constexpr laddr_t L_ADDR_ROOT = L_ADDR_MAX - 1;
constexpr laddr_t L_ADDR_LBAT = L_ADDR_MAX - 2;

struct __attribute((packed)) laddr_le_t {
  ceph_le64 laddr = ceph_le64(L_ADDR_NULL);

  using orig_type = laddr_t;

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
  // the following two types are not extent types,
  // they are just used to indicates paddr allocation deltas
  ALLOC_INFO = 9,
  JOURNAL_TAIL = 10,
  // Test Block Types
  TEST_BLOCK = 11,
  TEST_BLOCK_PHYSICAL = 12,
  BACKREF_INTERNAL = 13,
  BACKREF_LEAF = 14,
  // None and the number of valid extent_types_t
  NONE = 15,
};
using extent_types_le_t = uint8_t;
constexpr auto EXTENT_TYPES_MAX = static_cast<uint8_t>(extent_types_t::NONE);

constexpr size_t BACKREF_NODE_SIZE = 4096;

std::ostream &operator<<(std::ostream &out, extent_types_t t);

constexpr bool is_logical_type(extent_types_t type) {
  switch (type) {
  case extent_types_t::ROOT:
  case extent_types_t::LADDR_INTERNAL:
  case extent_types_t::LADDR_LEAF:
  case extent_types_t::BACKREF_INTERNAL:
  case extent_types_t::BACKREF_LEAF:
    return false;
  default:
    return true;
  }
}

constexpr bool is_retired_placeholder(extent_types_t type)
{
  return type == extent_types_t::RETIRED_PLACEHOLDER;
}

constexpr bool is_lba_node(extent_types_t type)
{
  return type == extent_types_t::LADDR_INTERNAL ||
    type == extent_types_t::LADDR_LEAF;
}

constexpr bool is_backref_node(extent_types_t type)
{
  return type == extent_types_t::BACKREF_INTERNAL ||
    type == extent_types_t::BACKREF_LEAF;
}

std::ostream &operator<<(std::ostream &out, extent_types_t t);

using reclaim_gen_t = uint8_t;

constexpr reclaim_gen_t DIRTY_GENERATION = 1;
constexpr reclaim_gen_t COLD_GENERATION = 1;
constexpr reclaim_gen_t RECLAIM_GENERATIONS = 3;
constexpr reclaim_gen_t NULL_GENERATION =
  std::numeric_limits<reclaim_gen_t>::max();

struct reclaim_gen_printer_t {
  reclaim_gen_t gen;
};

std::ostream &operator<<(std::ostream &out, reclaim_gen_printer_t gen);

enum class data_category_t : uint8_t {
  METADATA = 0,
  DATA,
  NUM
};

std::ostream &operator<<(std::ostream &out, data_category_t c);

constexpr data_category_t get_extent_category(extent_types_t type) {
  if (type == extent_types_t::OBJECT_DATA_BLOCK ||
      type == extent_types_t::COLL_BLOCK) {
    return data_category_t::DATA;
  } else {
    return data_category_t::METADATA;
  }
}

// type for extent modification time, milliseconds since the epoch
using sea_time_point = seastar::lowres_system_clock::time_point;
using sea_duration = seastar::lowres_system_clock::duration;
using mod_time_point_t = int64_t;

constexpr mod_time_point_t
timepoint_to_mod(const sea_time_point &t) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      t.time_since_epoch()).count();
}

constexpr sea_time_point
mod_to_timepoint(mod_time_point_t t) {
  return sea_time_point(std::chrono::duration_cast<sea_duration>(
      std::chrono::milliseconds(t)));
}

constexpr auto NULL_TIME = sea_time_point();
constexpr auto NULL_MOD_TIME = timepoint_to_mod(NULL_TIME);

struct sea_time_point_printer_t {
  sea_time_point tp;
};
std::ostream &operator<<(std::ostream &out, sea_time_point_printer_t tp);

struct mod_time_point_printer_t {
  mod_time_point_t tp;
};
std::ostream &operator<<(std::ostream &out, mod_time_point_printer_t tp);

constexpr sea_time_point
get_average_time(const sea_time_point& t1, std::size_t n1,
                 const sea_time_point& t2, std::size_t n2) {
  assert(t1 != NULL_TIME);
  assert(t2 != NULL_TIME);
  auto new_size = n1 + n2;
  assert(new_size > 0);
  auto c1 = t1.time_since_epoch().count();
  auto c2 = t2.time_since_epoch().count();
  auto c_ret = c1 / new_size * n1 + c2 / new_size * n2;
  return sea_time_point(sea_duration(c_ret));
}

/* description of a new physical extent */
struct extent_t {
  extent_types_t type;  ///< type of extent
  laddr_t addr;         ///< laddr of extent (L_ADDR_NULL for non-logical)
  ceph::bufferlist bl;  ///< payload, bl.length() == length, aligned
};

using extent_version_t = uint32_t;

/* description of a mutation to a physical extent */
struct delta_info_t {
  extent_types_t type = extent_types_t::NONE;  ///< delta type
  paddr_t paddr;                               ///< physical address
  laddr_t laddr = L_ADDR_NULL;                 ///< logical address
  uint32_t prev_crc = 0;
  uint32_t final_crc = 0;
  seastore_off_t length = NULL_SEG_OFF;         ///< extent length
  extent_version_t pversion;                   ///< prior version
  segment_seq_t ext_seq;		       ///< seq of the extent's segment
  segment_type_t seg_type;
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
    denc(v.ext_seq, p);
    denc(v.seg_type, p);
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
      ext_seq == rhs.ext_seq &&
      bl == rhs.bl
    );
  }
};

std::ostream &operator<<(std::ostream &out, const delta_info_t &delta);

/* contains the latest journal tail information */
struct journal_tail_delta_t {
  journal_seq_t alloc_tail;
  journal_seq_t dirty_tail;

  DENC(journal_tail_delta_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.alloc_tail, p);
    denc(v.dirty_tail, p);
    DENC_FINISH(p);
  }
};

std::ostream &operator<<(std::ostream &out, const journal_tail_delta_t &delta);

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
std::ostream &operator<<(std::ostream &out, const omap_root_t &root);

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
 * phy_tree_root_t
 */
class __attribute__((packed)) phy_tree_root_t {
  paddr_le_t root_addr;
  depth_le_t depth = init_extent_len_le(0);
  
public:
  phy_tree_root_t() = default;
  
  phy_tree_root_t(paddr_t addr, depth_t depth)
    : root_addr(addr), depth(init_depth_le(depth)) {}

  phy_tree_root_t(const phy_tree_root_t &o) = default;
  phy_tree_root_t(phy_tree_root_t &&o) = default;
  phy_tree_root_t &operator=(const phy_tree_root_t &o) = default;
  phy_tree_root_t &operator=(phy_tree_root_t &&o) = default;
  
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
  
  coll_root_le_t(laddr_t laddr, seastore_off_t size)
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

using lba_root_t = phy_tree_root_t;
using backref_root_t = phy_tree_root_t;

/**
 * root_t
 *
 * Contains information required to find metadata roots.
 * TODO: generalize this to permit more than one lba_manager implementation
 */
struct __attribute__((packed)) root_t {
  using meta_t = std::map<std::string, std::string>;

  static constexpr int MAX_META_LENGTH = 1024;

  backref_root_t backref_root;
  lba_root_t lba_root;
  laddr_le_t onode_root;
  coll_root_le_t collection_root;

  char meta[MAX_META_LENGTH];

  root_t() {
    set_meta(meta_t{});
  }

  void adjust_addrs_from_base(paddr_t base) {
    lba_root.adjust_addrs_from_base(base);
    backref_root.adjust_addrs_from_base(base);
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

struct alloc_blk_t {
  alloc_blk_t(
    paddr_t paddr,
    laddr_t laddr,
    extent_len_t len,
    extent_types_t type)
    : paddr(paddr), laddr(laddr), len(len), type(type)
  {}

  explicit alloc_blk_t() = default;

  paddr_t paddr = P_ADDR_NULL;
  laddr_t laddr = L_ADDR_NULL;
  extent_len_t len = 0;
  extent_types_t type = extent_types_t::ROOT;
  DENC(alloc_blk_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.paddr, p);
    denc(v.laddr, p);
    denc(v.len, p);
    denc(v.type, p);
    DENC_FINISH(p);
  }
};

// use absolute address
struct alloc_delta_t {
  enum class op_types_t : uint8_t {
    NONE = 0,
    SET = 1,
    CLEAR = 2
  };
  std::vector<alloc_blk_t> alloc_blk_ranges;
  op_types_t op = op_types_t::NONE;

  alloc_delta_t() = default;

  DENC(alloc_delta_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.alloc_blk_ranges, p);
    denc(v.op, p);
    DENC_FINISH(p);
  }
};

struct extent_info_t {
  extent_types_t type = extent_types_t::NONE;
  laddr_t addr = L_ADDR_NULL;
  extent_len_t len = 0;

  extent_info_t() = default;
  extent_info_t(const extent_t &et)
    : type(et.type), addr(et.addr),
      len(et.bl.length())
  {}

  DENC(extent_info_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.type, p);
    denc(v.addr, p);
    denc(v.len, p);
    DENC_FINISH(p);
  }
};
std::ostream &operator<<(std::ostream &out, const extent_info_t &header);

using segment_nonce_t = uint32_t;

/**
 * Segment header
 *
 * Every segment contains and encode segment_header_t in the first block.
 * Our strategy for finding the journal replay point is:
 * 1) Find the segment with the highest journal_segment_seq
 * 2) Get dirty_tail and alloc_tail from the segment header
 * 3) Scan forward to update tails from journal_tail_delta_t
 * 4) Replay from the latest tails
 */
struct segment_header_t {
  segment_seq_t segment_seq;
  segment_id_t physical_segment_id; // debugging

  journal_seq_t dirty_tail;
  journal_seq_t alloc_tail;
  segment_nonce_t segment_nonce;

  segment_type_t type;

  data_category_t category;
  reclaim_gen_t generation;

  segment_type_t get_type() const {
    return type;
  }

  DENC(segment_header_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.segment_seq, p);
    denc(v.physical_segment_id, p);
    denc(v.dirty_tail, p);
    denc(v.alloc_tail, p);
    denc(v.segment_nonce, p);
    denc(v.type, p);
    denc(v.category, p);
    denc(v.generation, p);
    DENC_FINISH(p);
  }
};
std::ostream &operator<<(std::ostream &out, const segment_header_t &header);

struct segment_tail_t {
  segment_seq_t segment_seq;
  segment_id_t physical_segment_id; // debugging

  segment_nonce_t segment_nonce;

  segment_type_t type;

  mod_time_point_t modify_time;
  std::size_t num_extents;

  segment_type_t get_type() const {
    return type;
  }

  DENC(segment_tail_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.segment_seq, p);
    denc(v.physical_segment_id, p);
    denc(v.segment_nonce, p);
    denc(v.type, p);
    denc(v.modify_time, p);
    denc(v.num_extents, p);
    DENC_FINISH(p);
  }
};
std::ostream &operator<<(std::ostream &out, const segment_tail_t &tail);

enum class transaction_type_t : uint8_t {
  MUTATE = 0,
  READ, // including weak and non-weak read transactions
  CLEANER_TRIM_DIRTY,
  CLEANER_TRIM_ALLOC,
  CLEANER_RECLAIM,
  MAX
};

static constexpr auto TRANSACTION_TYPE_NULL = transaction_type_t::MAX;

static constexpr auto TRANSACTION_TYPE_MAX = static_cast<std::size_t>(
    transaction_type_t::MAX);

std::ostream &operator<<(std::ostream &os, transaction_type_t type);

constexpr bool is_valid_transaction(transaction_type_t type) {
  return type < transaction_type_t::MAX;
}

constexpr bool is_cleaner_transaction(transaction_type_t type) {
  return (type >= transaction_type_t::CLEANER_TRIM_DIRTY &&
          type < transaction_type_t::MAX);
}

struct record_size_t {
  extent_len_t plain_mdlength = 0; // mdlength without the record header
  extent_len_t dlength = 0;

  extent_len_t get_raw_mdlength() const;

  bool is_empty() const {
    return plain_mdlength == 0 &&
           dlength == 0;
  }

  void account_extent(extent_len_t extent_len);

  void account(const extent_t& extent) {
    account_extent(extent.bl.length());
  }

  void account(const delta_info_t& delta);

  bool operator==(const record_size_t &) const = default;
};
std::ostream &operator<<(std::ostream&, const record_size_t&);

struct record_t {
  transaction_type_t type = TRANSACTION_TYPE_NULL;
  std::vector<extent_t> extents;
  std::vector<delta_info_t> deltas;
  record_size_t size;
  sea_time_point modify_time = NULL_TIME;

  record_t(transaction_type_t type) : type{type} { }

  // unit test only
  record_t() {
    type = transaction_type_t::MUTATE;
  }

  // unit test only
  record_t(std::vector<extent_t>&& _extents,
           std::vector<delta_info_t>&& _deltas) {
    auto modify_time = seastar::lowres_system_clock::now();
    for (auto& e: _extents) {
      push_back(std::move(e), modify_time);
    }
    for (auto& d: _deltas) {
      push_back(std::move(d));
    }
    type = transaction_type_t::MUTATE;
  }

  bool is_empty() const {
    return extents.size() == 0 &&
           deltas.size() == 0;
  }

  std::size_t get_delta_size() const {
    auto delta_size = std::accumulate(
        deltas.begin(), deltas.end(), 0,
        [](uint64_t sum, auto& delta) {
          return sum + delta.bl.length();
        }
    );
    return delta_size;
  }

  void push_back(extent_t&& extent, sea_time_point &t) {
    ceph_assert(t != NULL_TIME);
    if (extents.size() == 0) {
      assert(modify_time == NULL_TIME);
      modify_time = t;
    } else {
      modify_time = get_average_time(modify_time, extents.size(), t, 1);
    }
    size.account(extent);
    extents.push_back(std::move(extent));
  }

  void push_back(delta_info_t&& delta) {
    size.account(delta);
    deltas.push_back(std::move(delta));
  }
};
std::ostream &operator<<(std::ostream&, const record_t&);

struct record_header_t {
  transaction_type_t type;
  uint32_t deltas;              // number of deltas
  uint32_t extents;             // number of extents
  mod_time_point_t modify_time;

  DENC(record_header_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.type, p);
    denc(v.deltas, p);
    denc(v.extents, p);
    denc(v.modify_time, p);
    DENC_FINISH(p);
  }
};
std::ostream &operator<<(std::ostream&, const record_header_t&);

struct record_group_header_t {
  uint32_t      records;
  extent_len_t  mdlength;       // block aligned, length of metadata
  extent_len_t  dlength;        // block aligned, length of data
  segment_nonce_t segment_nonce;// nonce of containing segment
  journal_seq_t committed_to;   // records prior to committed_to have been
                                // fully written, maybe in another segment.
  checksum_t data_crc;          // crc of data payload


  DENC(record_group_header_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.records, p);
    denc(v.mdlength, p);
    denc(v.dlength, p);
    denc(v.segment_nonce, p);
    denc(v.committed_to, p);
    denc(v.data_crc, p);
    DENC_FINISH(p);
  }
};
std::ostream& operator<<(std::ostream&, const record_group_header_t&);

struct record_group_size_t {
  extent_len_t plain_mdlength = 0; // mdlength without the group header
  extent_len_t dlength = 0;
  extent_len_t block_size = 0;

  record_group_size_t() = default;
  record_group_size_t(
      const record_size_t& rsize,
      extent_len_t block_size) {
    account(rsize, block_size);
  }

  extent_len_t get_raw_mdlength() const;

  extent_len_t get_mdlength() const {
    assert(block_size > 0);
    return p2roundup(get_raw_mdlength(), block_size);
  }

  extent_len_t get_encoded_length() const {
    assert(block_size > 0);
    assert(dlength % block_size == 0);
    return get_mdlength() + dlength;
  }

  record_group_size_t get_encoded_length_after(
      const record_size_t& rsize,
      extent_len_t block_size) const {
    record_group_size_t tmp = *this;
    tmp.account(rsize, block_size);
    return tmp;
  }

  double get_fullness() const {
    assert(block_size > 0);
    return ((double)(get_raw_mdlength() + dlength) /
            get_encoded_length());
  }

  void account(const record_size_t& rsize,
               extent_len_t block_size);

  bool operator==(const record_group_size_t &) const = default;
};
std::ostream& operator<<(std::ostream&, const record_group_size_t&);

struct record_group_t {
  std::vector<record_t> records;
  record_group_size_t size;

  record_group_t() = default;
  record_group_t(
      record_t&& record,
      extent_len_t block_size) {
    push_back(std::move(record), block_size);
  }

  std::size_t get_size() const {
    return records.size();
  }

  void push_back(
      record_t&& record,
      extent_len_t block_size) {
    size.account(record.size, block_size);
    records.push_back(std::move(record));
    assert(size.get_encoded_length() < MAX_SEG_OFF);
  }

  void reserve(std::size_t limit) {
    records.reserve(limit);
  }

  void clear() {
    records.clear();
    size = {};
  }
};
std::ostream& operator<<(std::ostream&, const record_group_t&);

ceph::bufferlist encode_record(
  record_t&& record,
  extent_len_t block_size,
  const journal_seq_t& committed_to,
  segment_nonce_t current_segment_nonce);

ceph::bufferlist encode_records(
  record_group_t& record_group,
  const journal_seq_t& committed_to,
  segment_nonce_t current_segment_nonce);

std::optional<record_group_header_t>
try_decode_records_header(
    const ceph::bufferlist& header_bl,
    segment_nonce_t expected_nonce);

bool validate_records_metadata(
    const ceph::bufferlist& md_bl);

bool validate_records_data(
    const record_group_header_t& header,
    const ceph::bufferlist& data_bl);

struct record_extent_infos_t {
  record_header_t header;
  std::vector<extent_info_t> extent_infos;
};
std::optional<std::vector<record_extent_infos_t> >
try_decode_extent_infos(
    const record_group_header_t& header,
    const ceph::bufferlist& md_bl);
std::optional<std::vector<record_header_t>>
try_decode_record_headers(
    const record_group_header_t& header,
    const ceph::bufferlist& md_bl);

struct record_deltas_t {
  paddr_t record_block_base;
  std::vector<std::pair<sea_time_point, delta_info_t>> deltas;
};
std::optional<std::vector<record_deltas_t> >
try_decode_deltas(
    const record_group_header_t& header,
    const ceph::bufferlist& md_bl,
    paddr_t record_block_base);

struct write_result_t {
  journal_seq_t start_seq;
  seastore_off_t length;

  journal_seq_t get_end_seq() const {
    return start_seq.add_offset(length);
  }
};
std::ostream& operator<<(std::ostream&, const write_result_t&);

struct record_locator_t {
  paddr_t record_block_base;
  write_result_t write_result;
};
std::ostream& operator<<(std::ostream&, const record_locator_t&);

/// scan segment for end incrementally
struct scan_valid_records_cursor {
  bool last_valid_header_found = false;
  journal_seq_t seq;
  journal_seq_t last_committed;
  std::size_t num_consumed_records = 0;

  struct found_record_group_t {
    paddr_t offset;
    record_group_header_t header;
    bufferlist mdbuffer;

    found_record_group_t(
      paddr_t offset,
      const record_group_header_t &header,
      const bufferlist &mdbuffer)
      : offset(offset), header(header), mdbuffer(mdbuffer) {}
  };
  std::deque<found_record_group_t> pending_record_groups;

  bool is_complete() const {
    return last_valid_header_found && pending_record_groups.empty();
  }

  segment_id_t get_segment_id() const {
    return seq.offset.as_seg_paddr().get_segment_id();
  }

  seastore_off_t get_segment_offset() const {
    return seq.offset.as_seg_paddr().get_segment_off();
  }

  void increment_seq(seastore_off_t off) {
    auto& seg_addr = seq.offset.as_seg_paddr();
    seg_addr.set_segment_off(
      seg_addr.get_segment_off() + off);
  }

  void emplace_record_group(const record_group_header_t&, ceph::bufferlist&&);

  void pop_record_group() {
    assert(!pending_record_groups.empty());
    ++num_consumed_records;
    pending_record_groups.pop_front();
  }

  scan_valid_records_cursor(
    journal_seq_t seq)
    : seq(seq) {}
};
std::ostream& operator<<(std::ostream&, const scan_valid_records_cursor&);

inline const seg_paddr_t& paddr_t::as_seg_paddr() const {
  assert(get_addr_type() == addr_types_t::SEGMENT);
  return *static_cast<const seg_paddr_t*>(this);
}

inline seg_paddr_t& paddr_t::as_seg_paddr() {
  assert(get_addr_type() == addr_types_t::SEGMENT);
  return *static_cast<seg_paddr_t*>(this);
}

inline const blk_paddr_t& paddr_t::as_blk_paddr() const {
  assert(get_addr_type() == addr_types_t::RANDOM_BLOCK);
  return *static_cast<const blk_paddr_t*>(this);
}

inline blk_paddr_t& paddr_t::as_blk_paddr() {
  assert(get_addr_type() == addr_types_t::RANDOM_BLOCK);
  return *static_cast<blk_paddr_t*>(this);
}

inline paddr_t paddr_t::operator-(paddr_t rhs) const {
  if (get_addr_type() == addr_types_t::SEGMENT) {
    auto& seg_addr = as_seg_paddr();
    return seg_addr - rhs;
  }
  ceph_assert(0 == "not supported type");
  return P_ADDR_NULL;
}

#define PADDR_OPERATION(a_type, base, func)        \
  if (get_addr_type() == a_type) {                 \
    return static_cast<const base*>(this)->func;   \
  }

inline paddr_t paddr_t::add_offset(int32_t o) const {
  PADDR_OPERATION(addr_types_t::SEGMENT, seg_paddr_t, add_offset(o))
  PADDR_OPERATION(addr_types_t::RANDOM_BLOCK, blk_paddr_t, add_offset(o))
  ceph_assert(0 == "not supported type");
  return P_ADDR_NULL;
}

inline paddr_t paddr_t::add_relative(paddr_t o) const {
  PADDR_OPERATION(addr_types_t::SEGMENT, seg_paddr_t, add_relative(o))
  PADDR_OPERATION(addr_types_t::RANDOM_BLOCK, blk_paddr_t, add_relative(o))
  ceph_assert(0 == "not supported type");
  return P_ADDR_NULL;
}

inline paddr_t paddr_t::add_block_relative(paddr_t o) const {
  PADDR_OPERATION(addr_types_t::SEGMENT, seg_paddr_t, add_block_relative(o))
  PADDR_OPERATION(addr_types_t::RANDOM_BLOCK, blk_paddr_t, add_block_relative(o))
  ceph_assert(0 == "not supported type");
  return P_ADDR_NULL;
}

inline paddr_t paddr_t::add_record_relative(paddr_t o) const {
  PADDR_OPERATION(addr_types_t::SEGMENT, seg_paddr_t, add_record_relative(o))
  PADDR_OPERATION(addr_types_t::RANDOM_BLOCK, blk_paddr_t, add_record_relative(o))
  ceph_assert(0 == "not supported type");
  return P_ADDR_NULL;
}

inline paddr_t paddr_t::maybe_relative_to(paddr_t o) const {
  PADDR_OPERATION(addr_types_t::SEGMENT, seg_paddr_t, maybe_relative_to(o))
  PADDR_OPERATION(addr_types_t::RANDOM_BLOCK, blk_paddr_t, maybe_relative_to(o))
  ceph_assert(0 == "not supported type");
  return P_ADDR_NULL;
}

}

WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::seastore_meta_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::segment_id_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::paddr_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::journal_seq_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::delta_info_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::journal_tail_delta_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::record_header_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::record_group_header_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::extent_info_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::segment_header_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::alloc_blk_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::alloc_delta_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::segment_tail_t)

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

template<>
struct denc_traits<crimson::os::seastore::segment_type_t> {
  static constexpr bool supported = true;
  static constexpr bool featured = false;
  static constexpr bool bounded = true;
  static constexpr bool need_contiguous = false;

  static void bound_encode(
    const crimson::os::seastore::segment_type_t &o,
    size_t& p,
    uint64_t f=0) {
    p += sizeof(crimson::os::seastore::segment_type_t);
  }
  template<class It>
  static std::enable_if_t<!is_const_iterator_v<It>>
  encode(
    const crimson::os::seastore::segment_type_t &o,
    It& p,
    uint64_t f=0) {
    get_pos_add<crimson::os::seastore::segment_type_t>(p) = o;
  }
  template<class It>
  static std::enable_if_t<is_const_iterator_v<It>>
  decode(
    crimson::os::seastore::segment_type_t& o,
    It& p,
    uint64_t f=0) {
    o = get_pos_add<crimson::os::seastore::segment_type_t>(p);
  }
  static void decode(
    crimson::os::seastore::segment_type_t& o,
    ceph::buffer::list::const_iterator &p) {
    p.copy(sizeof(crimson::os::seastore::segment_type_t),
           reinterpret_cast<char*>(&o));
  }
};
