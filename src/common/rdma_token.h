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
 * Out-of-band delivery descriptor carried on a MOSDOp.
 *
 * The descriptor is advisory: an OSD that can and will deliver a
 * read's data out of band RDMA-writes it into the client memory
 * window named by the opaque token, at the token's base address plus
 * base_offset plus the data's offset relative to the read's extent,
 * and reports the pushed byte count in the reply's oob_bytes; any OSD
 * that cannot (or will not: expired lease, retransmitted op, unknown
 * flags) replies with the data inline exactly as if no descriptor
 * were present. Degradation is therefore always plain, correct,
 * in-band data.
 */
struct delivery_t {
  std::string token;      ///< opaque cuObject RDMA descriptor
  uint64_t base_offset = 0; ///< client-window offset for the read's first byte
  uint32_t lease_ms = 0;  ///< do not START an RDMA write later than this after
                          ///< op receipt; 0 = no lease
  uint32_t flags = 0;     ///< reserved; OSDs deliver inline when nonzero

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    ceph::encode(token, bl);
    ceph::encode(base_offset, bl);
    ceph::encode(lease_ms, bl);
    ceph::encode(flags, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    ceph::decode(token, p);
    ceph::decode(base_offset, p);
    ceph::decode(lease_ms, p);
    ceph::decode(flags, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(delivery_t)

} // namespace ceph::rdma
