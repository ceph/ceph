// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <cerrno>
#include <cstdint>
#include <optional>

#include "include/rados/librados.hpp"
#include "rgw_cksum.h"

struct req_state;

/**
 * Checksums of out-of-band transfers: the CRCs that the OSDs compute of
 * the bytes they deliver into, or pull out of, a client's memory, folded
 * in object order. A response carries each one the gateway has in its
 * own header, x-amz-rdma-checksum-<algorithm>, with the value rendered
 * as S3 renders x-amz-checksum-<algorithm>. Unlike S3's header, which
 * names the stored checksum of the whole object, it covers exactly the
 * bytes of this transfer: a range of a GET too.
 */
namespace rgw::rdma {

inline constexpr const char* HDR_CRC64NVME = "x-amz-rdma-checksum-crc64nvme";
inline constexpr const char* HDR_CRC32C = "x-amz-rdma-checksum-crc32c";

/// the CRCs a request asks the OSDs for
struct cksum_want {
  bool crc64nvme = false;
  bool crc32c = false;
  bool any() const { return crc64nvme || crc32c; }
};

/// the librados RDMA_DELIVERY_WANT_* request flags of want
inline uint32_t delivery_want_flags(const cksum_want& want)
{
  using R = librados::ObjectOperation;
  return (want.crc64nvme ? R::RDMA_DELIVERY_WANT_CRC64 : 0) |
    (want.crc32c ? R::RDMA_DELIVERY_WANT_CRC32C : 0);
}

/**
 * What a request's x-amz-rdma-checksum-algorithm asks for, given its
 * value or null: CRC64NVME, the default when the header is absent, or
 * CRC32C, in any case. -EINVAL for any other value.
 */
inline int parse_checksum_algorithm(const char* value, cksum_want* want)
{
  *want = cksum_want{};
  switch (value ? rgw::cksum::parse_cksum_type(value)
                : rgw::cksum::Type::crc64nvme) {
  case rgw::cksum::Type::crc64nvme:
    want->crc64nvme = true;
    return 0;
  case rgw::cksum::Type::crc32c:
    want->crc32c = true;
    return 0;
  default:
    return -EINVAL;
  }
}

/// parse_checksum_algorithm() of the request's header, which also sets
/// the error message of a value it rejects
int parse_checksum_header(req_state* s, cksum_want* want);

/// S3's rendering of a canonical CRC-64/NVME: base64 of its big-endian
/// bytes, as rgw::digest::Crc64Nvme::Final() lays out a stored checksum
/// on either byte order
inline rgw::cksum::Cksum cksum_crc64nvme(uint64_t canonical)
{
  const uint64_t stored = rgw::cksum::stored_crc_order(canonical);
  return rgw::cksum::Cksum(rgw::cksum::Type::crc64nvme,
                           reinterpret_cast<const char*>(&stored),
                           rgw::cksum::Cksum::CtorStyle::raw);
}

/// the same for a canonical CRC-32C, as rgw::digest::Crc32c::Final()
inline rgw::cksum::Cksum cksum_crc32c(uint32_t canonical)
{
  const uint32_t stored = rgw::cksum::stored_crc_order(canonical);
  return rgw::cksum::Cksum(rgw::cksum::Type::crc32c,
                           reinterpret_cast<const char*>(&stored),
                           rgw::cksum::Cksum::CtorStyle::raw);
}

/// the x-amz-rdma-checksum-<algorithm> header of each CRC given
void dump_checksums(req_state* s, const std::optional<uint64_t>& crc64,
                    const std::optional<uint32_t>& crc32c);

/// the canonical CRCs of some bytes, those that are known
struct crcs {
  std::optional<uint64_t> crc64;
  std::optional<uint32_t> crc32c;
};

/**
 * The CRCs of the bytes that an OSD reported for one operation, those
 * that fold with their neighbours': its own when it marked them
 * combinable (one contiguous extent), or the fold of its ranges when it
 * placed the bytes as several that cover them exactly, as a stripe
 * served by interleaved EC shards arrives. Neither without its valid
 * flag.
 */
crcs delivered_crcs(
  const librados::ObjectOperation::rdma_delivery_result& r);

/**
 * Folds the CRCs of consecutive extents, appended in object order, into
 * those of their concatenation, for the CRCs wanted. An extent without
 * one of them ends that CRC: skipping it would cover the wrong bytes.
 */
class crc_fold {
  cksum_want live;
  crcs folded;

public:
  explicit crc_fold(const cksum_want& want) : live(want) {}

  /// the next extent, of len bytes
  void append(const crcs& c, uint64_t len);

  /// those of everything appended; none before the first extent
  const crcs& result() const { return folded; }
};

} // namespace rgw::rdma
