// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>
#include <list>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "include/encoding.h"

namespace ceph { class Formatter; }

namespace ceph::rdma {

/// Client memory window advertised by an RDMA token.
struct token_window {
  uint64_t addr = 0;  ///< client buffer virtual address
  uint64_t size = 0;  ///< registered window size in bytes
};

/// Maximum token length accepted anywhere in ceph. The cuObject
/// descriptor is 81 characters today, and an ofi1 token at most 618
/// (OFI_RMA_TOKEN_MAX is 640 with its NUL, and common/ofi_rma.h checks
/// it against this); this bounds untrusted input.
inline constexpr size_t RDMA_TOKEN_MAX_LEN = 640;

/// The third field of the libfabric tokens that ofi-rma writes
/// (OFI_RMA_TOKEN_TAG); common/ofi_rma.h checks that the two agree.
inline constexpr std::string_view OFI_TOKEN_TAG = "ofi1";

/// Parse the addr and size fields of an RDMA descriptor token as
/// produced by cuObject clients: colon-separated hex fields
/// "raddr:rsize:rkey:lid:qp:has_gid:gid". Only the leading two fields
/// are interpreted; the rest of the token is opaque and must be
/// forwarded verbatim to the RDMA library. Returns std::nullopt on
/// malformed input.
std::optional<token_window> parse_rdma_token(std::string_view token);

/// The cuObject descriptor of the window [addr, addr+size), registered
/// with rkey, behind the Dynamically Connected target dctn at lid and
/// gid: "raddr:rsize:rkey:lid:dctn:1:gid" in fixed-width lowercase hex,
/// the form the cuObject server library parses. The DC target of the
/// OSD's gather windows (common/rdma_dc_target.h) hands these out.
std::string format_cuobj_descriptor(uint64_t addr, uint32_t size,
				    uint32_t rkey, uint16_t lid,
				    uint32_t dctn,
				    std::span<const uint8_t, 16> gid);

/// Whether a token is a libfabric one (common/ofi_rma.h): its third
/// field is "ofi<N>", the libfabric family at any version N, where a
/// cuObject descriptor has its memory key in hex. The OSD's cuObject
/// executor and the gateway's PUT take any other token for a cuObject
/// descriptor, so a version this build does not know still counts here
/// and is declined by the libfabric code. Only the shape is checked; a
/// libfabric token can still fail to parse.
bool is_ofi_token(std::string_view token);

/// Whether a token is a cuObject descriptor that names a window: not a
/// libfabric token, and parse_rdma_token() reads its addr and size.
bool is_cuobj_descriptor(std::string_view token);

/// the transport names of oob_client_transports and
/// oob_cluster_transport that are not libfabric providers
inline constexpr std::string_view TRANSPORT_CUOBJ = "cuobj";

/// Split a transport list ("verbs_ofi_rxm, cuobj") into its names, in
/// order, lower-cased, without duplicates or empty entries. A name of
/// the form <core>_ofi_<utility> is the libfabric provider
/// <core>;ofi_<utility>, as fi_info names it, since a semicolon starts a
/// comment in a configuration file; the list holds the libfabric name.
std::vector<std::string> parse_transport_list(std::string_view list);

/// a transport name as libfabric spells it: <core>_ofi_<utility> becomes
/// <core>;ofi_<utility>, verbs becomes verbs;ofi_rxm, which is what
/// libfabric layers on verbs for RDM endpoints, and any other name stays
/// as it is
std::string libfabric_name(std::string_view name);

/// whether a transport name is a libfabric provider: any name but
/// cuobj
bool is_libfabric_provider(std::string_view name);

/**
 * The out-of-band transports of a cluster, from oob_client_transports
 * and oob_cluster_transport. A daemon opens one libfabric endpoint: for
 * the cluster transport's provider when that is one, else for the first
 * provider in the client list.
 */
struct oob_transports {
  /// oob_client_transports, parsed
  std::vector<std::string> client;
  /// oob_cluster_transport: a libfabric provider, cuobj, or empty
  std::string cluster;
  /// the provider of the daemon's libfabric endpoint, or empty
  std::string ofi_provider;
  /// what the daemon ignores, and why, to log
  std::vector<std::string> warnings;

  bool client_names(std::string_view t) const;
  bool client_cuobj() const { return client_names(TRANSPORT_CUOBJ); }
  /// whether the client list names a libfabric provider
  bool client_ofi() const;
  bool cluster_cuobj() const { return cluster == TRANSPORT_CUOBJ; }
  bool cluster_ofi() const { return !cluster.empty() && !cluster_cuobj(); }
  /// whether either option names cuobj
  bool cuobj() const { return client_cuobj() || cluster_cuobj(); }
  /// whether either option names this libfabric provider
  bool names_provider(std::string_view p) const {
    return !ofi_provider.empty() && ofi_provider == p;
  }
};

/// Read the two options' values.
oob_transports parse_oob_transports(std::string_view client,
				    std::string_view cluster);

/**
 * Per-op out-of-band delivery descriptor carried on a MOSDOp.
 *
 * The descriptor names a client memory window and an offset into it.
 * The fields say where the client's bytes live; FLAG_PULL says who
 * moves them. Without it a descriptor asks for read delivery,
 * described below. With it, on a WRITE or WRITEFULL op, the primary
 * pulls the op's payload out of the window instead (see FLAG_PULL).
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
 * path, and for a pulled write, the two coincide: nothing else will
 * touch the window, so a client that abandons a request may reuse it
 * once the lease plus its transport's drain bound have elapsed since
 * the last attempt of the request reached its OSD. A caller whose
 * window must stay valid until some later event instead waits for that
 * event. The lease does not bound it, and must not be read as though
 * it did.
 */
struct delivery_t {
  /// request the canonical CRC-64/NVME of the delivered bytes in the
  /// reply's oob result (best effort - check
  /// oob_result_t::FLAG_CRC64NVME; fold crc64 only under
  /// FLAG_COMBINABLE, else fold the per-range values under
  /// FLAG_RANGES)
  static constexpr uint32_t FLAG_CRC64NVME = 1u << 0;
  /// set by the client library on a resent op: every earlier attempt
  /// got a reply whose result for this op was declined or landed, or went
  /// out without its descriptors, so none of them can still write the
  /// window, and the OSD may deliver
  /// this attempt out of band as it would a first one. An OSD that
  /// does not know the bit delivers inline, as it does any resend.
  static constexpr uint32_t FLAG_PRIOR_SETTLED = 1u << 1;
  /// request the canonical CRC-32C (Castagnoli, S3's CRC32C) of the
  /// delivered bytes, as FLAG_CRC64NVME requests the CRC-64/NVME; the two
  /// may be asked for together. An OSD that predates it delivers inline,
  /// and fails a pull that asks for it.
  static constexpr uint32_t FLAG_CRC32C = 1u << 3;
  /// flag bits the OSD understands; unknown bits deliver inline
  static constexpr uint32_t KNOWN_FLAGS = FLAG_CRC64NVME | FLAG_PRIOR_SETTLED |
    FLAG_CRC32C;
  /**
   * The op is a WRITE or WRITEFULL that carries no data: its extent's
   * length bytes are in the client window, starting at the token's
   * base plus base_offset, and the primary RDMA-reads them before it
   * runs the op, as an OSD-direct PUT does. The op then runs as if the
   * client had sent the bytes: the primary encodes an erasure-coded
   * write and sends each shard to its OSD, or a replicated write to the
   * replicas, as usual. With FLAG_CRC64NVME, FLAG_CRC32C or both, the
   * reply's oob result carries those CRCs of the pulled bytes.
   *
   * A pull is not advisory: the op has no other data. An OSD that
   * cannot pull fails the op (-EOPNOTSUPP when it has no way to read
   * the window, when its client message throttle has no room for the
   * payload, when its transport refuses the read before any of it goes
   * out, or when the read would start after the pool's lease since the
   * OSD received the op; -EIO when the read failed or was cut off after
   * some of it may have gone out), and an OSD that predates pulls fails
   * it for its missing data (-EINVAL). Either way nothing is written.
   * On -EOPNOTSUPP nothing of the op's pull went out, and the client
   * may send the payload inline instead. An OSD may fail a request
   * that would fail whatever its payloads before it pulls any of them,
   * with the error the request would get inline, and its pulls
   * declined; a write that an erasure-coded pool without overwrites
   * refuses as no append then fails with -EINVAL, not -EOPNOTSUPP,
   * since inline it would fail again. The window must stay valid, and
   * unchanged, until the op's reply arrives, or, for an abandoned
   * request, until the lease and the drain passed as above.
   */
  static constexpr uint32_t FLAG_PULL = 1u << 2;
  /// flag bits an OSD that pulls understands on a pull; others fail it
  static constexpr uint32_t PULL_KNOWN_FLAGS = FLAG_CRC64NVME | FLAG_PULL |
    FLAG_PRIOR_SETTLED | FLAG_CRC32C;

  /// whether the op is a pull: FLAG_PULL with a token. A descriptor
  /// without a token requests nothing (empty()), whatever its flags, so
  /// every check for a pull, on the client and on the OSD, reads it
  /// as no pull
  bool is_pull() const {
    return !empty() && (flags & FLAG_PULL);
  }

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
  void dump(ceph::Formatter* f) const;
  static std::list<delivery_t> generate_test_instances();
};
WRITE_CLASS_ENCODER(delivery_t)

/**
 * One contiguous range of an out-of-band transfer: the bytes that
 * landed at client-window offset [ofs, ofs+len), and their canonical
 * CRC-64/NVME and CRC-32C, each valid when the result that carries the
 * range says so (oob_result_t::FLAG_CRC64NVME, FLAG_CRC32C). A
 * placement plan is a list of such ranges by construction, so an
 * executor can report one of these per plan triple, and any set of them
 * covering a window range without gaps concatenate-combines in ofs order
 * (see fold_crc64_ranges() and fold_crc32c_ranges()).
 */
struct crc_range_t {
  uint64_t ofs = 0;    ///< client-window offset (token base relative)
  uint64_t len = 0;
  uint64_t crc64 = 0;
  uint32_t crc32c = 0;

  bool operator==(const crc_range_t&) const = default;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    ceph::encode(ofs, bl);
    ceph::encode(len, bl);
    ceph::encode(crc64, bl);
    ceph::encode(crc32c, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    ceph::decode(ofs, p);
    ceph::decode(len, p);
    ceph::decode(crc64, p);
    ceph::decode(crc32c, p);
    DECODE_FINISH(p);
  }
  void dump(ceph::Formatter* f) const;
  static std::list<crc_range_t> generate_test_instances();
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
 * what a caller verifying one transfer needs. FLAG_COMBINABLE
 * additionally says those bytes are one contiguous logical extent,
 * so crc64 concatenate-combines with adjacent results in logical
 * order - what a caller reassembling a whole object's checksum out
 * of per-stripe results needs. A scattered placement (interleaved
 * EC-direct chunks, sparse extents) cannot offer that for its single
 * crc64: a CRC over the concatenation of every other chunk is not a
 * function of the per-chunk values. But each of its ranges is one
 * contiguous extent, so FLAG_RANGES carries one crc_range_t per
 * placement triple instead, and a caller that has every range of a
 * window can fold them in ofs order regardless of which OSD moved
 * which chunk. crc64 alone is the N=1 special case of that.
 *
 * The CRC-32C works the same way: FLAG_CRC32C says crc32c, and each
 * range's crc32c, are valid. FLAG_COMBINABLE and FLAG_RANGES describe
 * the shape of the placement, so they hold for every CRC the result
 * carries.
 */
struct oob_result_t {
  /// crc64 covers exactly the bytes that went out of band
  static constexpr uint32_t FLAG_CRC64NVME = 1u << 0;
  /// ...and those bytes are one contiguous logical extent, so crc64 and
  /// crc32c may be concatenate-combined in logical order
  static constexpr uint32_t FLAG_COMBINABLE = 1u << 1;
  /// ranges holds one crc_range_t per contiguous placed extent
  static constexpr uint32_t FLAG_RANGES = 1u << 2;
  /// set by the OSD, or by the client library for an op whose descriptor
  /// it withheld (require_osd_release below umbrella): no transfer was
  /// started for this op, so nothing of it reached the window; a read's
  /// data is inline, and a withheld pull fails. Older OSDs never set it,
  /// and a reader that does not know it treats the op as inline.
  static constexpr uint32_t FLAG_DECLINED = 1u << 3;
  /// set by the OSD: every write of this op completed, delivery-complete,
  /// before the OSD replied, so nothing of it lands after the reply (bar a
  /// transport's late duplicate of a completed write, which the window's
  /// owner retires by re-keying)
  static constexpr uint32_t FLAG_LANDED = 1u << 4;
  /// set by the client library, never on the wire: the operation was sent
  /// more than once, and an earlier attempt may have started a transfer
  /// this result knows nothing of
  static constexpr uint32_t FLAG_RESENT = 1u << 5;
  /// crc32c, and each range's crc32c, cover the bytes that went out of
  /// band, as FLAG_CRC64NVME says of the CRC-64/NVME
  static constexpr uint32_t FLAG_CRC32C = 1u << 6;

  uint64_t bytes = 0;
  uint64_t crc64 = 0;
  uint32_t flags = 0;  ///< FLAG_* above
  std::vector<crc_range_t> ranges;  ///< valid with FLAG_RANGES
  uint32_t crc32c = 0;  ///< valid with FLAG_CRC32C

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    ceph::encode(bytes, bl);
    ceph::encode(crc64, bl);
    ceph::encode(flags, bl);
    ceph::encode(ranges, bl);
    ceph::encode(crc32c, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    ceph::decode(bytes, p);
    ceph::decode(crc64, p);
    ceph::decode(flags, p);
    ceph::decode(ranges, p);
    ceph::decode(crc32c, p);
    DECODE_FINISH(p);
  }
  void dump(ceph::Formatter* f) const;
  static std::list<oob_result_t> generate_test_instances();
};
WRITE_CLASS_ENCODER(oob_result_t)

/**
 * The transfer flags an OSD reports for a read op whose delivery it
 * attempted. Placed: every byte is in the window, and landed when the
 * transport promised delivery-complete. Not placed: the data goes back
 * inline, declined when no write of it reached the transport, and
 * neither when one did - a push cut off after its writes went out may
 * have landed in part, and its reader fences the window before it falls
 * back.
 */
inline uint32_t attempt_flags(bool placed, bool started, bool landed_promise)
{
  if (placed) {
    return landed_promise ? oob_result_t::FLAG_LANDED : 0;
  }
  return started ? 0 : oob_result_t::FLAG_DECLINED;
}

/**
 * The declined, landed and resent flags of a result assembled from the
 * results of its parts, as a split read assembles its sub-reads': declined
 * when every part declined; landed when some bytes moved and every part
 * landed or declined, not all of them declined - a part that started no
 * transfer, as a replica's share wholly past the object's end, leaves
 * nothing to land after the reply; resent when any part was resent, which
 * says nothing of the other two, as on a result of one op. The crc flags
 * are the caller's.
 */
inline uint32_t fold_transfer_flags(const std::vector<uint32_t>& parts,
				    uint64_t bytes)
{
  if (parts.empty()) {
    return 0;
  }
  bool declined = true, landed = bytes > 0, resent = false;
  for (uint32_t f : parts) {
    declined = declined && (f & oob_result_t::FLAG_DECLINED);
    landed = landed &&
      (f & (oob_result_t::FLAG_DECLINED | oob_result_t::FLAG_LANDED));
    resent = resent || (f & oob_result_t::FLAG_RESENT);
  }
  landed = landed && !declined;
  return (declined ? oob_result_t::FLAG_DECLINED : 0) |
    (landed ? oob_result_t::FLAG_LANDED : 0) |
    (resent ? oob_result_t::FLAG_RESENT : 0);
}

/**
 * Whether an attempt whose descriptor-bearing ops got these result
 * flags is settled: every one declined (started no transfer) or landed
 * (every write completed before the reply), and none was resent. No
 * write of a settled attempt can land after its reply, so a retry of
 * it may deliver out of band like a first attempt. Mixed results
 * settle too - some parts declined, the others landed. An attempt with
 * no results (an old OSD, no reply) is not settled.
 */
inline bool attempt_settled(const std::vector<uint32_t>& parts)
{
  if (parts.empty()) {
    return false;
  }
  for (uint32_t f : parts) {
    if (!(f & (oob_result_t::FLAG_DECLINED | oob_result_t::FLAG_LANDED)) ||
        (f & oob_result_t::FLAG_RESENT)) {
      return false;
    }
  }
  return true;
}

/// whether an op of a request pulls its payload out of client memory
/// (delivery_t::is_pull()); deliveries is aligned with its ops
template <typename Deliveries>
bool any_pull(const Deliveries& deliveries)
{
  for (const auto& d : deliveries) {
    if (d.is_pull()) {
      return true;
    }
  }
  return false;
}

/**
 * Whether a resent op's client vouched that every earlier attempt is
 * settled (delivery_t::FLAG_PRIOR_SETTLED on every descriptor it
 * carries), so its OSD may deliver it out of band.
 */
inline bool prior_attempts_settled(const std::vector<delivery_t>& deliveries)
{
  bool any = false;
  for (const auto& d : deliveries) {
    if (d.empty()) {
      continue;
    }
    if (!(d.flags & delivery_t::FLAG_PRIOR_SETTLED)) {
      return false;
    }
    any = true;
  }
  return any;
}

/**
 * Concatenate-combine a set of ranges into the CRC-64/NVME of the
 * window range they cover. The ranges may arrive in any order (they
 * are sorted by ofs here) but must tile [first.ofs, last.ofs+len)
 * without gaps or overlaps; returns nullopt otherwise, or when empty.
 */
std::optional<uint64_t> fold_crc64_ranges(std::vector<crc_range_t> ranges);

/// fold_crc64_ranges() for the ranges' CRC-32C values
std::optional<uint32_t> fold_crc32c_ranges(std::vector<crc_range_t> ranges);

/**
 * The canonical CRC-32C (Castagnoli: init and xorout all ones,
 * reflected) of bl: the value that S3's x-amz-checksum-crc32c renders as
 * base64 of its big-endian bytes.
 */
uint32_t crc32c_canonical(const ceph::buffer::list& bl);

/// the canonical CRC-32C of A followed by B, from those of A and of B,
/// where len_b is the length of B
uint32_t crc32c_combine(uint32_t crc_a, uint32_t crc_b, uint64_t len_b);

} // namespace ceph::rdma
