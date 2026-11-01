// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "common/rdma_token.h"
#include "include/rados/librados.hpp"

namespace librados {

/// the wire result, as a librados caller sees it. Its own header, so
/// that the unit tests fill a caller's result with this copy, not with
/// one of their own
inline void copy_rdma_result(const ceph::rdma::oob_result_t& r,
			     ObjectOperation::rdma_delivery_result* out)
{
  out->bytes = r.bytes;
  out->crc64 = r.crc64;
  out->crc32c = r.crc32c;
  out->flags = r.flags;
  out->ranges.clear();
  out->ranges.reserve(r.ranges.size());
  for (const auto& x : r.ranges) {
    out->ranges.push_back({x.ofs, x.len, x.crc64, x.crc32c});
  }
}

} // namespace librados
