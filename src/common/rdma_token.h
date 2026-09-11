// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "include/encoding.h"

namespace ceph::rdma {

/// Client memory window advertised by an RDMA token.
struct token_window {
  uint64_t addr = 0;  ///< client buffer virtual address
  uint64_t size = 0;  ///< registered window size in bytes
};

/// Maximum token length accepted anywhere in ceph. The cuObject
/// descriptor is 81 characters today; leave generous headroom for
/// future revisions while still bounding untrusted input.
inline constexpr size_t RDMA_TOKEN_MAX_LEN = 512;

/// Parse the addr and size fields of an RDMA descriptor token as
/// produced by cuObject clients: colon-separated hex fields
/// "raddr:rsize:rkey:lid:qp:has_gid:gid". Only the leading two fields
/// are interpreted; the rest of the token is opaque and must be
/// forwarded verbatim to the RDMA library. Returns std::nullopt on
/// malformed input.
std::optional<token_window> parse_rdma_token(std::string_view token);

/**
 * Per-op out-of-band delivery descriptor carried on a MOSDOp.
 *
 * The descriptor names a client memory window and an offset into it.
 * It does not name a direction: the fields say where the client's
 * bytes live, not who moves them, and the encoding is versioned so a
 * direction that needs more can add it. Today the only implemented
 * direction is read delivery, described below; an OSD pulling its
 * share of a write payload out of client memory would address it the
 * same way.
 *
 * The MOSDOp carries one descriptor per op (a vector aligned with the
 * ops, mirroring the reply's per-op oob results); an entry with an
 * empty token means that op's data stays inline. The descriptor is
 * advisory: an OSD that can and will deliver the op's read data out
 * of band RDMA-writes it into the client memory window named by the
 * opaque token, at the token's base address plus base_offset plus the
 * data's offset relative to the read's extent, and reports the pushed
 * byte count in the reply's oob result for that op; any OSD that
 * cannot (or will not: expired lease, retransmitted op, unknown
 * flags) replies with the data inline exactly as if no descriptor
 * were present. Degradation is therefore always plain, correct,
 * in-band data.
 *
 * The lease bounding how long after receipt an OSD may still
 * initiate a transfer against a descriptor is not carried here: it is
 * the pool's rdma_delivery_lease option
 * (pg_pool_t::get_rdma_delivery_lease()), so the OSD that enforces it
 * and the client that reasons about it read the same value.
 *
 * The lease bounds the OSD's side only - when a transfer may start,
 * not how long a client must keep its window registered. On the read
 * path the two coincide: nothing else will touch the window, so a
 * client that abandons a request may reuse it once the lease plus its
 * transport's drain bound have elapsed. A caller whose window must
 * stay valid until some later event instead - every OSD in a PG
 * having pulled its share of a write payload, say - waits for that
 * event. The lease does not bound it, and must not be read as though
 * it did.
 */
struct delivery_t {
  /// request the canonical CRC-64/NVME of the delivered bytes in the
  /// reply's oob result (best effort - check
  /// oob_result_t::FLAG_CRC64NVME; fold crc64 only under
  /// FLAG_CRC64_COMBINABLE, else fold the per-range values under
  /// FLAG_CRC64_RANGES)
  static constexpr uint32_t FLAG_CRC64NVME = 1u << 0;
  /// flag bits the OSD understands; unknown bits deliver inline
  static constexpr uint32_t KNOWN_FLAGS = FLAG_CRC64NVME;

  std::string token;      ///< opaque cuObject RDMA descriptor
  uint64_t base_offset = 0; ///< client-window offset for the op's first byte
  uint32_t flags = 0;     ///< FLAG_* above; OSDs deliver inline on unknown bits

  /// true when no delivery is requested for this op
  bool empty() const {
    return token.empty();
  }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    ceph::encode(token, bl);
    ceph::encode(base_offset, bl);
    ceph::encode(flags, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    ceph::decode(token, p);
    ceph::decode(base_offset, p);
    ceph::decode(flags, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(delivery_t)

/**
 * One contiguous range of an out-of-band transfer: the bytes that
 * landed at client-window offset [ofs, ofs+len), and their canonical
 * CRC-64/NVME. A placement plan is a list of such ranges by
 * construction, so an executor can report one of these per plan
 * triple, and any set of them covering a window range without gaps
 * concatenate-combines in ofs order (see fold_crc64_ranges()).
 */
struct crc_range_t {
  uint64_t ofs = 0;    ///< client-window offset (token base relative)
  uint64_t len = 0;
  uint64_t crc64 = 0;

  bool operator==(const crc_range_t&) const = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    ceph::encode(ofs, bl);
    ceph::encode(len, bl);
    ceph::encode(crc64, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    ceph::decode(ofs, p);
    ceph::decode(len, p);
    ceph::decode(crc64, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(crc_range_t)

/**
 * Per-op out-of-band delivery result carried on the MOSDOpReply.
 * bytes is how much of the op's data went out of band (0 = inline);
 * crc64 is the canonical CRC-64/NVME of exactly those bytes, in the
 * order they were pushed.
 *
 * Three separate properties, because they have separate consumers.
 * FLAG_CRC64NVME says crc64 covers the bytes this OSD moved, which is
 * what a caller verifying one transfer needs. FLAG_CRC64_COMBINABLE
 * additionally says those bytes are one contiguous logical extent,
 * so crc64 concatenate-combines with adjacent results in logical
 * order - what a caller reassembling a whole object's checksum out
 * of per-stripe results needs. A scattered placement (interleaved
 * EC-direct chunks, sparse extents) cannot offer that for its single
 * crc64: a CRC over the concatenation of every other chunk is not a
 * function of the per-chunk values. But each of its ranges is one
 * contiguous extent, so FLAG_CRC64_RANGES carries one crc_range_t per
 * placement triple instead, and a caller that has every range of a
 * window can fold them in ofs order regardless of which OSD moved
 * which chunk. crc64 alone is the N=1 special case of that.
 */
struct oob_result_t {
  /// crc64 covers exactly the bytes that went out of band
  static constexpr uint32_t FLAG_CRC64NVME = 1u << 0;
  /// ...and those bytes are one contiguous logical extent, so crc64
  /// may be concatenate-combined in logical order
  static constexpr uint32_t FLAG_CRC64_COMBINABLE = 1u << 1;
  /// ranges holds one crc_range_t per contiguous placed extent
  static constexpr uint32_t FLAG_CRC64_RANGES = 1u << 2;

  uint64_t bytes = 0;
  uint64_t crc64 = 0;
  uint32_t flags = 0;  ///< FLAG_* above
  std::vector<crc_range_t> ranges;  ///< valid with FLAG_CRC64_RANGES

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    ceph::encode(bytes, bl);
    ceph::encode(crc64, bl);
    ceph::encode(flags, bl);
    ceph::encode(ranges, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    ceph::decode(bytes, p);
    ceph::decode(crc64, p);
    ceph::decode(flags, p);
    ceph::decode(ranges, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(oob_result_t)

/**
 * Concatenate-combine a set of ranges into the CRC-64/NVME of the
 * window range they cover. The ranges may arrive in any order (they
 * are sorted by ofs here) but must tile [first.ofs, last.ofs+len)
 * without gaps or overlaps; returns nullopt otherwise, or when empty.
 */
std::optional<uint64_t> fold_crc64_ranges(std::vector<crc_range_t> ranges);

} // namespace ceph::rdma
