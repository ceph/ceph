// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

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
  /// reply's oob result (linear placements only; best effort - check
  /// oob_result_t::FLAG_CRC64NVME)
  static constexpr uint32_t FLAG_CRC64NVME = 1u << 0;
  /// flag bits the OSD understands; unknown bits deliver inline
  static constexpr uint32_t KNOWN_FLAGS = FLAG_CRC64NVME;

  std::string token;      ///< opaque cuObject RDMA descriptor
  uint64_t base_offset = 0; ///< client-window offset for the read's first byte
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
 * Per-op out-of-band delivery result carried on the MOSDOpReply.
 * bytes is how much of the op's data went out of band (0 = inline);
 * crc64 is the canonical CRC-64/NVME of exactly those bytes, valid
 * only when FLAG_CRC64NVME is set (the OSD computes it on request for
 * linear placements).
 */
struct oob_result_t {
  static constexpr uint32_t FLAG_CRC64NVME = 1u << 0; ///< crc64 is valid

  uint64_t bytes = 0;
  uint64_t crc64 = 0;
  uint32_t flags = 0;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    ceph::encode(bytes, bl);
    ceph::encode(crc64, bl);
    ceph::encode(flags, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    ceph::decode(bytes, p);
    ceph::decode(crc64, p);
    ceph::decode(flags, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(oob_result_t)

} // namespace ceph::rdma
