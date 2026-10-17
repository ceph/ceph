// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <vector>

#include "common/rdma_token.h"

namespace ceph::osd::oob {

/// mark declined the result of each op that carries a descriptor in
/// deliveries: no transfer of it started, so nothing of it reached its
/// window, and its client need not fence the window. oob holds one
/// result for each op of the request
inline void decline_all(const std::vector<ceph::rdma::delivery_t>& deliveries,
			std::vector<ceph::rdma::oob_result_t>& oob)
{
  for (size_t i = 0; i < oob.size() && i < deliveries.size(); i++) {
    if (!deliveries[i].empty()) {
      oob[i].flags |= ceph::rdma::oob_result_t::FLAG_DECLINED;
    }
  }
}

/**
 * The results of a reply that runs none of the request's reads
 * (OSDService::reply_op_error()), with r its result, and dup when it
 * answers a resend as a dup of a write that the PG log has. No transfer
 * starts from this reply path, so an error declines each op that
 * carries a descriptor. A dup does too, whatever its result. oob is
 * aligned with deliveries.
 */
inline void reply_error_results(
  const std::vector<ceph::rdma::delivery_t>& deliveries, int r, bool dup,
  std::vector<ceph::rdma::oob_result_t>& oob)
{
  if (r < 0 || dup) {
    decline_all(deliveries, oob);
  }
}

} // namespace ceph::osd::oob
