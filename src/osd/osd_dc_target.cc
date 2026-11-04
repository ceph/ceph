// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "osd/osd_dc_target.h"

#include <infiniband/verbs.h>

#include <cerrno>

#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/oob_config.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "osd_dc_target: "

OSDDcTarget::~OSDDcTarget()
{
  close();
}

void OSDDcTarget::close()
{
  m_dct.close();
  if (m_mr) ibv_dereg_mr(m_mr);
  if (m_pd) ibv_dealloc_pd(m_pd);
  if (m_ctx) ibv_close_device(m_ctx);
  m_mr = nullptr; m_pd = nullptr; m_ctx = nullptr; m_pool = nullptr;
}

int OSDDcTarget::open(CephContext* cct, const std::string& rdma_ip,
		      char* pool, size_t pool_size, uint64_t dc_key)
{
  // the device, port and GID that carry our RDMA address, as the OSD's
  // out-of-band location names them: a RoCE v2 GID when one holds the
  // address, since only v2 routes, and an IPv6 address's own GID
  std::string dev_name;
  unsigned port = 0;
  int gid_index = -1;
  if (!ceph::oob::find_rdma_gid("/sys", rdma_ip, &dev_name, &port,
				&gid_index)) {
    derr << "no RDMA device carries " << rdma_ip << dendl;
    return -ENODEV;
  }
  int num = 0;
  ibv_device** list = ibv_get_device_list(&num);
  for (int i = 0; list && i < num && !m_ctx; i++) {
    if (dev_name == ibv_get_device_name(list[i])) {
      m_ctx = ibv_open_device(list[i]);
    }
  }
  if (list) {
    ibv_free_device_list(list);
  }
  if (!m_ctx) {
    derr << "cannot open RDMA device " << dev_name << ", which carries "
	 << rdma_ip << dendl;
    return -ENODEV;
  }

  m_pool_size = pool_size;
  m_pool = pool;
  if (!m_pool || !(m_pd = ibv_alloc_pd(m_ctx))) {
    close();
    return -ENOMEM;
  }
  // peers only write into the windows: none may read back what a
  // window holds, another client's gathered data perhaps
  m_mr = ibv_reg_mr(m_pd, m_pool, pool_size,
		    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
  if (!m_mr) {
    derr << "registering the window pool: " << cpp_strerror(errno) << dendl;
    close();
    return -EIO;
  }
  std::string err;
  if (int r = m_dct.open(m_ctx, m_pd, static_cast<uint8_t>(port), gid_index,
			 dc_key, &err); r < 0) {
    derr << "DC target on " << dev_name << " port " << port << ": " << err
	 << dendl;
    close();
    return r;
  }
  dout(1) << "DC target dctn=" << m_dct.dctn() << " on " << rdma_ip << " ("
	  << dev_name << " port " << port << " GID " << gid_index
	  << "), pool " << pool_size << " bytes" << dendl;
  return 0;
}

std::string OSDDcTarget::token(size_t ofs, size_t len) const
{
  return m_dct.token(m_pool + ofs, static_cast<uint32_t>(len), m_mr->rkey);
}
