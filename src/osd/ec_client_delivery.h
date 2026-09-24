// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <cstdint>
#include <set>
#include <string>
#include <vector>

#include "common/rdma_token.h"
#include "include/utime.h"

namespace ceph::osd {

/**
 * A client read the peers may finish themselves. When a primary reads
 * an erasure-coded object on a client's behalf and the client asked
 * for out-of-band delivery, every data shard's chunks would otherwise
 * travel to the primary and then to the client. With this attached to
 * the read, a peer holding a data shard RDMA-writes its chunks into
 * the client's window at their logical positions (the interleave a
 * shard-direct read uses) and only sends them to the primary if a
 * reconstruction needs them; the primary delivers its own chunks and
 * whatever it had to reconstruct, and reports the whole.
 */
struct ec_client_delivery_t {
  std::string token;        ///< the client's window
  uint64_t base_offset = 0; ///< window offset of ro_off
  uint64_t ro_off = 0;      ///< the read's logical range
  uint64_t ro_len = 0;
  bool want_crc = false;
  utime_t recv_stamp;       ///< of the client op, for the peers' lease fences
};

/**
 * Where, in raw shard raw's own offset space, the first chunk it owns
 * of the logical range starting at ro_off begins. A shard extent that
 * starts there holds that shard's chunks of the range in order, which
 * is what the interleave placement (ec_direct_plan) assumes.
 */
inline uint64_t ec_first_shard_offset(uint64_t ro_off, uint64_t chunk_size,
                                      uint32_t k, uint32_t raw)
{
  const uint64_t c0 = ro_off / chunk_size;
  const uint64_t c = c0 + (raw + k - (c0 % k)) % k;
  return (c / k) * chunk_size + (c == c0 ? ro_off % chunk_size : 0);
}

/// what the peers delivered on the client's behalf
struct ec_client_delivery_result_t {
  std::set<int> delivered_raw_shards; ///< their whole share of the range
  uint64_t bytes = 0;
  std::vector<ceph::rdma::crc_range_t> ranges;
};

} // namespace ceph::osd
