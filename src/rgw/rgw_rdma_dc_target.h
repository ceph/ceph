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

#include <cstdint>
#include <string>

#include "common/rdma_dc_target.h"
#include "include/common_fwd.h"
#include "rgw_rdma_rc_transport.h"

/**
 * A cuObject Dynamically Connected target owned by the gateway.
 *
 * OSD-direct delivery (rgw_rdma_osd_direct) has each OSD push
 * its stripe with the cuObject server library, which speaks DC: the
 * writer needs only a token naming a DC target, an rkey and an address
 * window, with no per-pair handshake. When the S3 client speaks RC
 * instead, the gateway stands in as that target: it exposes a DCT on
 * the same device and protection domain the RC sessions use, hands
 * the OSDs a token for the session buffer, and the stripes land in
 * gateway memory for the RC push. Only mlx5 devices implement DC, so
 * this is compiled only with HAVE_MLX5DV and opened only when the
 * device supports it. The DCT itself is ceph::rdma::dc_target, which
 * the OSD's gather target (osd_dc_target) brings up too.
 */
namespace rgw::rdma::dc {

class Target {
 public:
  Target() = default;
  ~Target();
  Target(const Target&) = delete;
  Target& operator=(const Target&) = delete;

  /// create the DCT on dev; dc_key must match cuobj_dc_key
  int open(CephContext* cct, rc::Device& dev, uint64_t dc_key);
  void close();

  /// the cuObject descriptor for a registered window, in the form the
  /// OSDs' cuObject server library parses:
  /// "raddr:rsize:rkey:lid:dctn:has_gid:gid" (lowercase hex)
  std::string token(const void* addr, uint32_t size, uint32_t rkey) const {
    return dct.token(addr, size, rkey);
  }

 private:
  ceph::rdma::dc_target dct;
};

} // namespace rgw::rdma::dc
