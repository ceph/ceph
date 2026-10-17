// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

// Provenance: the DCT bring-up, and the descriptor format that
// format_cuobj_descriptor() writes, follow the host-memory DC client in
// versitygw's cuwrapper (Apache-2.0), which mirrors what NVIDIA's
// cuObject client library registers.

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

#include <infiniband/mlx5dv.h>
#include <infiniband/verbs.h>

#include <fmt/format.h>

#include "common/errno.h"
#include "common/rdma_token.h"

namespace ceph::rdma {

/**
 * A cuObject Dynamically Connected target (DCT).
 *
 * The cuObject server library writes to DC targets: the writer needs
 * only a descriptor naming the target, an rkey and an address window,
 * with no per-pair handshake. This creates a DCT on a device and
 * protection domain the owner opened, with a CQ and an SRQ of its own
 * that nothing polls, and brings it to RTR, which is all a target
 * needs. Peers may only write through it. The OSD's gather windows
 * (osd/osd_dc_target.h) sit behind one.
 *
 * Only mlx5 devices implement DC. This is header-only so that it builds
 * into the daemons that include it, which are the ones that link the
 * mlx5 direct verbs, and adds no library to anything else.
 */
class dc_target {
public:
  dc_target() = default;
  ~dc_target() { close(); }
  dc_target(const dc_target&) = delete;
  dc_target& operator=(const dc_target&) = delete;

  /// create the DCT on ctx and pd, for writers that know dc_key, on
  /// port and the GID at gid_index, and bring it to RTR. 0, or a
  /// negative errno with what failed in *err: -EOPNOTSUPP when the
  /// device cannot create a DCT, as only an mlx5 can
  int open(ibv_context* ctx, ibv_pd* pd, uint8_t port, int gid_index,
	   uint64_t dc_key, std::string* err);
  void close();

  uint32_t dctn() const { return m_dctn; }

  /// the descriptor for [addr, addr+size), registered on the target's
  /// protection domain with rkey for remote writes
  std::string token(const void* addr, uint32_t size, uint32_t rkey) const {
    return format_cuobj_descriptor(reinterpret_cast<uintptr_t>(addr), size,
				   rkey, m_lid, m_dctn, m_gid);
  }

private:
  ibv_cq* m_cq = nullptr;
  ibv_srq* m_srq = nullptr;
  ibv_qp* m_dct = nullptr;
  uint32_t m_dctn = 0;
  uint16_t m_lid = 0;
  uint8_t m_gid[16] = {};
};

inline int dc_target::open(ibv_context* ctx, ibv_pd* pd, uint8_t port,
			   int gid_index, uint64_t dc_key, std::string* err)
{
  // tear down what was created, say which step failed with errno e,
  // and return r, or -e
  auto fail = [&](std::string_view what, int e, int r = 0) {
    if (e <= 0) {
      e = EIO;
    }
    *err = fmt::format("{}: {}", what, cpp_strerror(e));
    close();
    return r ? r : -e;
  };

  ibv_port_attr pa;
  if (int r = ibv_query_port(ctx, port, &pa); r != 0) {
    return fail(fmt::format("querying port {}", port), r);
  }
  ibv_gid gid;
  if (ibv_query_gid(ctx, port, gid_index, &gid) != 0) {
    // it documents only -1, not errno: name what errno holds, read
    // before the message is formatted, but return -EIO
    int e = errno;
    return fail(fmt::format("querying GID {} of port {}", gid_index, port),
		e, -EIO);
  }
  m_lid = pa.lid;
  std::memcpy(m_gid, gid.raw, sizeof(m_gid));

  m_cq = ibv_create_cq(ctx, 1, nullptr, nullptr, 0);
  if (!m_cq) {
    return fail("ibv_create_cq", errno);
  }
  ibv_srq_init_attr srq_attr;
  std::memset(&srq_attr, 0, sizeof(srq_attr));
  srq_attr.attr.max_wr = 1;
  srq_attr.attr.max_sge = 1;
  m_srq = ibv_create_srq(pd, &srq_attr);
  if (!m_srq) {
    return fail("ibv_create_srq", errno);
  }

  ibv_qp_init_attr_ex attr_ex;
  std::memset(&attr_ex, 0, sizeof(attr_ex));
  attr_ex.pd = pd;
  attr_ex.send_cq = m_cq;
  attr_ex.recv_cq = m_cq;
  attr_ex.srq = m_srq;
  attr_ex.qp_type = IBV_QPT_DRIVER;
  attr_ex.comp_mask = IBV_QP_INIT_ATTR_PD;
  mlx5dv_qp_init_attr dv_attr;
  std::memset(&dv_attr, 0, sizeof(dv_attr));
  dv_attr.comp_mask = MLX5DV_QP_INIT_ATTR_MASK_DC;
  dv_attr.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCT;
  dv_attr.dc_init_attr.dct_access_key = dc_key;
  m_dct = mlx5dv_create_qp(ctx, &attr_ex, &dv_attr);
  if (!m_dct) {
    // expected on anything but mlx5: DC is a ConnectX feature
    return fail("mlx5dv_create_qp (DCT)", errno, -EOPNOTSUPP);
  }

  ibv_qp_attr qpa;
  std::memset(&qpa, 0, sizeof(qpa));
  qpa.qp_state = IBV_QPS_INIT;
  qpa.pkey_index = 0;
  qpa.port_num = port;
  // peers only write into the windows: none may read back what a
  // window holds, another client's data perhaps
  qpa.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;
  if (int r = ibv_modify_qp(m_dct, &qpa, IBV_QP_STATE | IBV_QP_PKEY_INDEX |
			    IBV_QP_PORT | IBV_QP_ACCESS_FLAGS); r != 0) {
    return fail("DCT -> INIT", r);
  }
  // a target only needs INIT -> RTR
  std::memset(&qpa, 0, sizeof(qpa));
  qpa.qp_state = IBV_QPS_RTR;
  // the port's own MTU, the largest packet a writer can send it
  qpa.path_mtu = pa.active_mtu;
  qpa.min_rnr_timer = 12;
  qpa.ah_attr.is_global = 1;
  qpa.ah_attr.port_num = port;
  // the DCT's ACKs carry this as their RoCE v2 TTL; leave room for a
  // routed multi-tier fabric
  qpa.ah_attr.grh.hop_limit = 64;
  qpa.ah_attr.grh.sgid_index = static_cast<uint8_t>(gid_index);
  if (int r = ibv_modify_qp(m_dct, &qpa, IBV_QP_STATE | IBV_QP_MIN_RNR_TIMER |
			    IBV_QP_AV | IBV_QP_PATH_MTU); r != 0) {
    return fail("DCT -> RTR", r);
  }
  m_dctn = m_dct->qp_num;
  if (m_dctn == 0) {
    return fail("DCT number is zero after RTR", EIO);
  }
  return 0;
}

inline void dc_target::close()
{
  if (m_dct) {
    ibv_destroy_qp(m_dct);
    m_dct = nullptr;
  }
  if (m_srq) {
    ibv_destroy_srq(m_srq);
    m_srq = nullptr;
  }
  if (m_cq) {
    ibv_destroy_cq(m_cq);
    m_cq = nullptr;
  }
  m_dctn = 0;
}

} // namespace ceph::rdma
