// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "common/rdma_dc_target.h"
#include "include/common_fwd.h"

/**
 * A cuObject Dynamically Connected target over one registered pool.
 *
 * When this OSD is the primary of an erasure-coded read gathered out of
 * band, its peers push their shard data with the cuObject server library,
 * which writes to DC targets. This exposes the pool as one: a DCT on the
 * mlx5 device, port and GID that ceph::oob::find_rdma_gid() finds for
 * the OSD's RDMA address, a RoCE v2 GID first, with the pool registered
 * for remote writes only. token() names a window of the pool in the
 * descriptor form the cuObject server parses,
 * "raddr:rsize:rkey:lid:dctn:has_gid:gid". The DCT itself is
 * ceph::rdma::dc_target.
 */
class OSDDcTarget {
public:
  OSDDcTarget() = default;
  ~OSDDcTarget();
  OSDDcTarget(const OSDDcTarget&) = delete;
  OSDDcTarget& operator=(const OSDDcTarget&) = delete;

  /// open the device carrying rdma_ip, IPv4 or IPv6, on the port and GID
  /// that carry it, register the pool_size bytes at pool, which must
  /// outlive the target, and bring the DCT to RTR; negative errno on
  /// failure
  int open(CephContext* cct, const std::string& rdma_ip, char* pool,
	   size_t pool_size, uint64_t dc_key);

  char* pool() const { return m_pool; }
  size_t pool_size() const { return m_pool_size; }

  /// descriptor for the window [ofs, ofs+len) of the pool
  std::string token(size_t ofs, size_t len) const;

private:
  void close();

  ibv_context* m_ctx = nullptr;
  ibv_pd* m_pd = nullptr;
  ibv_mr* m_mr = nullptr;
  ceph::rdma::dc_target m_dct;
  char* m_pool = nullptr;
  size_t m_pool_size = 0;
};
