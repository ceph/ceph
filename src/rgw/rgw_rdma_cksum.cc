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

#include "rgw_rdma_cksum.h"

#include <algorithm>
#include <utility>
#include <vector>

#include "common/crc64nvme.h"
#include "common/rdma_token.h"
#include "include/crc32c.h"
#include "rgw_rest.h"

namespace rgw::rdma {

int parse_checksum_header(req_state* s, cksum_want* want)
{
  const int r = parse_checksum_algorithm(
    s->info.env->get("HTTP_X_AMZ_RDMA_CHECKSUM_ALGORITHM", nullptr), want);
  if (r < 0) {
    s->err.message = "x-amz-rdma-checksum-algorithm must be CRC64NVME "
                     "or CRC32C";
  }
  return r;
}

void dump_checksums(req_state* s, const std::optional<uint64_t>& crc64,
                    const std::optional<uint32_t>& crc32c)
{
  if (crc64) {
    dump_header(s, HDR_CRC64NVME, cksum_crc64nvme(*crc64).to_armor());
  }
  if (crc32c) {
    dump_header(s, HDR_CRC32C, cksum_crc32c(*crc32c).to_armor());
  }
}

uint32_t crc32c_of(const void* p, uint64_t len)
{
  // ceph_crc32c() takes an unsigned length
  const auto* b = static_cast<const unsigned char*>(p);
  uint32_t crc = 0xffffffff;
  while (len > 0) {
    const unsigned n = static_cast<unsigned>(std::min<uint64_t>(len, 1u << 30));
    crc = ceph_crc32c(crc, b, n);
    b += n;
    len -= n;
  }
  return crc ^ 0xffffffff;
}

crcs delivered_crcs(
  const librados::ObjectOperation::rdma_delivery_result& r)
{
  using R = librados::ObjectOperation::rdma_delivery_result;
  crcs c;
  if (r.flags & R::COMBINABLE) {
    if (r.flags & R::CRC64_VALID) {
      c.crc64 = r.crc64;
    }
    if (r.flags & R::CRC32C_VALID) {
      c.crc32c = r.crc32c;
    }
  } else if (r.flags & R::RANGES) {
    std::vector<ceph::rdma::crc_range_t> ranges;
    ranges.reserve(r.ranges.size());
    uint64_t ranged = 0;
    for (const auto& x : r.ranges) {
      ranges.push_back({x.ofs, x.len, x.crc64, x.crc32c});
      ranged += x.len;
    }
    if (ranged == r.bytes) {
      if (r.flags & R::CRC64_VALID) {
        c.crc64 = ceph::rdma::fold_crc64_ranges(ranges);
      }
      if (r.flags & R::CRC32C_VALID) {
        c.crc32c = ceph::rdma::fold_crc32c_ranges(std::move(ranges));
      }
    }
  }
  return c;
}

void crc_fold::append(const crcs& c, uint64_t len)
{
  if (live.crc64nvme) {
    if (!c.crc64) {
      live.crc64nvme = false;
      folded.crc64.reset();
    } else {
      folded.crc64 = folded.crc64 ?
        ceph::crc64nvme_combine(*folded.crc64, *c.crc64, len) : *c.crc64;
    }
  }
  if (live.crc32c) {
    if (!c.crc32c) {
      live.crc32c = false;
      folded.crc32c.reset();
    } else {
      folded.crc32c = folded.crc32c ?
        ceph::rdma::crc32c_combine(*folded.crc32c, *c.crc32c, len) :
        *c.crc32c;
    }
  }
}

} // namespace rgw::rdma
