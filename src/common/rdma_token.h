// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>
#include <optional>
#include <string_view>

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

} // namespace ceph::rdma
