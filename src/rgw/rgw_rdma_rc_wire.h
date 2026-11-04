// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "include/interval_set.h"
#include "rgw_rdma_cksum.h"

class DoutPrefixProvider;
struct ifaddrs;
struct req_info;
struct sockaddr;

/**
 * Wire encoding of the hipobj-rc-v2 control protocol.
 *
 * hipobj-rc-v2 carries S3 object data over an RDMA Reliable Connection
 * (RC) queue pair. Unlike cuObject's Dynamically Connected transport,
 * where a client token alone lets any initiator write into the
 * client's memory, an RC transfer needs a paired queue pair on each
 * side, so the protocol exchanges the pairing parameters over three
 * SigV4-signed HTTP requests on a dedicated control path:
 *
 *   POST /.hipobj-rc/prepare  client token, PSN, cookie, op, target,
 *                             size, offset  ->  session id, server
 *                             token, server PSN and QP number
 *   POST /.hipobj-rc/ready    session, cookie, client QP number and
 *                             memory region  ->  the server runs the
 *                             transfer and answers with the outcome
 *   POST /.hipobj-rc/cancel   session  ->  tears the session down
 *
 * Everything here is free of the transport, so it can be unit tested on
 * hosts without RDMA hardware: parsers and formatters for the header
 * values, which hold no request state, and check_signed(), which reads
 * a request's signature headers. The protocol is defined
 * by AMD's hipObject client library; the header names and value
 * formats below mirror its reference server.
 */
namespace rgw::rdma::rc {

/// value of the x-amz-rdma-protocol header on every control request
inline constexpr std::string_view PROTOCOL = "hipobj-rc-v2";

/// control path prefix the routes live under
inline constexpr std::string_view CONTROL_PREFIX = "/.hipobj-rc";

/// value of x-amz-rdma-protocol-status on a 501 that tells the client
/// to fall back to plain HTTP
inline constexpr std::string_view STATUS_UNSUPPORTED = "unsupported";

/// largest transfer a session may describe (2^31-1 bytes)
inline constexpr uint64_t MAX_TRANSFER_SIZE = 0x7fffffff;

/// transport named by the first byte of a token
enum class transport_t : uint8_t {
  DC = 0x00,
  RC = 0x01,
};

/**
 * A fixed-width RDMA endpoint token: 44 bytes, hex-encoded as 88
 * characters. Multi-byte fields are little-endian; the GID is copied
 * verbatim. The client sends one in PREPARE naming its queue pair (and,
 * for a PUT, the memory region the server may write into); the server
 * answers with one naming its own queue pair.
 */
struct token_t {
  transport_t transport = transport_t::RC;
  uint32_t qpn = 0;
  uint8_t gid[16] = {};
  uint32_t rkey = 0;
  uint64_t addr = 0;
  uint64_t length = 0;
  uint8_t port = 1;
  uint16_t lid = 0;

  bool operator==(const token_t&) const = default;

  /// true when the GID is all zeroes (no routable peer named)
  bool gid_is_zero() const;
};

inline constexpr size_t TOKEN_BINARY_LEN = 44;
inline constexpr size_t TOKEN_HEX_LEN = 2 * TOKEN_BINARY_LEN;

/// encode as 88 lowercase hex characters
std::string encode_token(const token_t& token);

/// decode exactly 88 hex characters (either case); nullopt otherwise
std::optional<token_t> decode_token(std::string_view hex);

/// true for the all-zero token, the protocol's explicit loopback marker
bool token_is_zero(std::string_view hex);

/// whether addr, an IPv4 or IPv6 address as text (IPv4-mapped too), is
/// a loopback address: 127.0.0.0/8 or ::1. Only a request whose
/// connection comes from one can be from a client on the gateway's
/// host. An address of the host's interfaces does not tell: a proxy on
/// the host (cephadm's ingress) connects from one, and so do clients
/// that a container network translates to a node address
bool is_loopback_address(std::string_view addr);

/// a GID as its 16 bytes
using gid_bytes = std::array<uint8_t, 16>;
using gid_set = std::set<gid_bytes>;

/// the GID that RoCE gives an address of an interface: an IPv6 address
/// as it is, an IPv4 address in its IPv4-mapped form; nullopt for an
/// address of another family
std::optional<gid_bytes> roce_gid_of(const sockaddr* sa);
/// add the RoCE GID of each address in ifa to gids
void add_interface_gids(const ifaddrs* ifa, gid_set& gids);
/// whether a peer's GID names the gateway's host: a loopback address
/// (::1, or IPv4-mapped 127.0.0.0/8), or one of host, the GIDs of the
/// host's ports and interfaces
bool gid_names_host(const uint8_t (&gid)[16], const gid_set& host);

/// session ids are 128 random bits as 32 hex characters
inline constexpr size_t SESSION_HEX_LEN = 32;
bool valid_session_id(std::string_view s);

/// packet sequence numbers are 24-bit, sent as 6 hex digits, never 0
std::optional<uint32_t> parse_psn(std::string_view s);
std::string format_psn(uint32_t psn);

/// the cookie is a 32-bit client nonce, sent as 8 hex digits, never 0;
/// it rides the transfer as the immediate value so a completion can be
/// matched to its session
std::optional<uint32_t> parse_cookie(std::string_view s);
std::string format_cookie(uint32_t cookie);

/// bare hex (no 0x prefix), up to 16 digits: QP numbers, memory region
/// addresses and remote keys
std::optional<uint64_t> parse_hex(std::string_view s);
std::string format_hex(uint64_t v);

/// unsigned decimal: sizes and offsets
std::optional<uint64_t> parse_decimal(std::string_view s);

/// the object a session transfers, from the x-amz-rdma-target header
struct target_t {
  std::string bucket;
  std::string key;
  std::string query;  ///< canonical query string, possibly empty

  bool operator==(const target_t&) const = default;
};

/// parse "/bucket/key[?query]" (bucket and key percent-encoded);
/// nullopt when the shape is wrong or an escape is malformed
std::optional<target_t> parse_target(std::string_view target);

/// path-style unescape: "%XX" only, '+' stays literal; nullopt on a
/// truncated or non-hex escape
std::optional<std::string> percent_decode(std::string_view s);

/// build the canonical target value from its parts
std::string build_target(std::string_view bucket, std::string_view key,
                         std::string_view query = {});

/// true when every name (lowercase) is an entry of the ';'-separated
/// signed-headers list. Unsigned protocol headers could be rewritten by
/// anything on the path without breaking the signature, so the control
/// routes require all x-amz-rdma-* request headers to be signed.
bool all_signed(std::string_view signed_list,
                const std::vector<std::string>& names);

/// 0 when the signature of the request covers every x-amz-rdma-* header
/// it carries, -EACCES otherwise. The signature version and route are
/// the ones the S3 authenticator picks, and the signed list is the one
/// the SigV4 signature was verified over, read by the verifier's own
/// parser: a list placed where that parser does not look vouches for
/// nothing. SigV2, in the header or presigned, signs every x-amz-*
/// header.
int check_signed(const DoutPrefixProvider* dpp, const req_info& info);

/**
 * The byte ranges of a relay window that hold object data. OSDs place
 * stripes in any order and the staged path appends; the relay can push
 * only the contiguous run from the window's start. An OSD whose
 * transport does not promise delivery-complete writes (a libfabric
 * provider without FI_DELIVERY_COMPLETE, which the gateway does not
 * relay over, or an OSD of an older release) can reply before its bytes
 * are in the window: a stripe whose result does not say landed counts
 * only once the fence passed.
 */
class landed_ranges {
  interval_set<uint64_t> ranges;
  /// placed by an OSD whose result does not say landed
  interval_set<uint64_t> placed;

 public:
  /// bytes that are in the window: the gateway copied them, or the OSD's
  /// result says they landed before it replied
  void add(uint64_t ofs, uint64_t len);
  /// bytes that the OSD's result says it placed, but not that they
  /// landed: they may still be on their way, until the fence
  void add_placed(uint64_t ofs, uint64_t len);
  /// whether placed bytes wait for the fence
  bool unconfirmed() const { return !placed.empty(); }
  /// the fence passed: the placed bytes are in the window
  void confirm_placed();
  /// bytes complete from offset 0, placed ones only once confirmed
  uint64_t prefix() const;
  size_t count() const { return ranges.num_intervals(); }
};

/**
 * The CRCs that a client asked for of the bytes the gateway copies,
 * chained over the copies in object order while they are in the cache,
 * instead of a pass over the whole session buffer. It computes none
 * until reset() names them, so an op resets it once the request's
 * x-amz-rdma-checksum-algorithm is parsed.
 */
class chained_crcs {
  cksum_want want;
  crc_fold fold{want};

 public:
  /// start over, for the CRCs in w
  void reset(const cksum_want& w);
  /// the next len bytes, at p
  void append(const void* p, uint64_t len);
  /// those of everything appended since reset() that it asked for;
  /// none before the first append
  const crcs& result() const { return fold.result(); }
};

/// one RDMA write of window bytes [ofs, ofs+len) to the client
struct push_t {
  uint64_t ofs = 0;
  uint64_t len = 0;
  bool with_imm = false;  ///< the last write, carrying the session cookie

  bool operator==(const push_t&) const = default;
};

struct push_policy {
  /// stream in writes of at least this much, so small stripes do not
  /// become small writes
  uint64_t min_push = 1ull << 20;
  /// and at most this much, so pushing overlaps the object read
  uint64_t max_write = 8ull << 20;
  /// hold back this much of the end to ride the final write, so the
  /// completion the client waits for always carries data
  uint64_t tail_hold = 64ull << 10;
};

/**
 * The writes to post now, given the complete prefix and what was
 * already pushed. While streaming (final false) that is the prefix less
 * the held-back tail, once at least min_push is ready or the tail is
 * reached. At the end it is everything left, with the immediate on the
 * last write; nullopt then when the prefix does not cover total.
 */
std::optional<std::vector<push_t>> plan_pushes(uint64_t prefix, uint64_t pushed,
                                               uint64_t total, bool final,
                                               const push_policy& p = {});

} // namespace rgw::rdma::rc
