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

#include "rgw_rdma_dc_target.h"

#include <cerrno>

#include "common/ceph_context.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::rdma::dc {

Target::~Target()
{
  close();
}

int Target::open(CephContext* cct, rc::Device& dev, uint64_t dc_key)
{
  // on the device and protection domain of the RC sessions, so that
  // their buffers' rkeys hold behind it, and the port and GID they use
  std::string err;
  int r = dct.open(dev.context(), dev.protection_domain(), dev.port_num(),
                   dev.gid_idx(), dc_key, &err);
  if (r == -EOPNOTSUPP) {
    // expected on anything but mlx5: DC is a ConnectX feature
    ldout(cct, 1) << "rgw_rdma_dc: no DC target on " << dev.name() << ": "
                  << err << dendl;
    return r;
  } else if (r < 0) {
    lderr(cct) << "rgw_rdma_dc: DC target on " << dev.name() << ": " << err
               << dendl;
    return r;
  }
  ldout(cct, 1) << "rgw_rdma_dc: DC target dctn=" << dct.dctn() << " on "
                << dev.name() << dendl;
  return 0;
}

void Target::close()
{
  dct.close();
}

} // namespace rgw::rdma::dc
