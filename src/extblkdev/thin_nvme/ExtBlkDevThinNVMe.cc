// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include <errno.h>
#include <string>

#include <linux/nvme_ioctl.h>
#include <sys/ioctl.h>

#include "ExtBlkDevThinNVMe.h"
#include "common/blkdev.h"
#include "nvme/src/nvme/types.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_bdev
#define dout_context cct
#undef dout_prefix
#define dout_prefix *_dout << "ThinNVMe(" << this << ") "

int ExtBlkDevThinNVMe::init(const std::string& alogdevname)
{
  dout(3) << __func__ << " devname:" << alogdevname << dendl;
  logdevname = alogdevname;
  int _dev = -1;
  int _ns = -1;
  int n = -1;
  int r = sscanf(alogdevname.c_str(), "nvme%dn%d%n", &_dev, &_ns, &n);
  if (r == 2 && n == int(alogdevname.length())) {
    // FIXME: make additional checking for vendor/model or something
    ceph_assert(_dev >= 0);
    ceph_assert(_ns >= 0);
    std::string dev_name("/dev/nvme");
    dev_name += stringify(_dev);
    int _fd = open(dev_name.c_str(), O_RDONLY);
    if (_fd < 0) {
      r = -errno;
      derr << __func__ << " failed to open " << dev_name.c_str() << ": " << cpp_strerror(-r) << dendl;
      return r;
    }

    r = 0;
    dev = _dev;
    ns = _ns;
    fd = _fd;
    dout(3) << __func__ << " use dev:" << dev << " ns:" << ns << " as thin NVMe device" << dendl;
  } else {
    r = -EINVAL;
  }
  return r;
}

int ExtBlkDevThinNVMe::get_statfs(store_statfs_t& buf)
{
  if (fd < 0) {
    return -EBADF;
  }
  struct nvme_admin_cmd cmd = {};
  struct nvme_id_ns ns_data = {};

  cmd.opcode = nvme_admin_identify;
  cmd.nsid = ns;
  cmd.addr = (__u64)&ns_data;
  cmd.data_len = sizeof(ns_data);
  cmd.cdw10 = 0; // CNS value for Identify Namespace (0x00)


  int r = ioctl(fd, NVME_IOCTL_ADMIN_CMD, &cmd);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " ioctl failed: " << r << " " << cpp_strerror(-r) << dendl;
  } else {
    uint64_t lba_size = 1ULL << ns_data.lbaf[ns_data.flbas & 0x0F].ds;

    uint64_t nsze = ns_data.nsze * lba_size; // Namespace(logical) size: bytes available to the host
    uint64_t ncap = ns_data.ncap * lba_size; // Namespace capacity: the actual capacity (in bytes) provided
    uint64_t nuse = ns_data.nuse * lba_size; // Namespace usage: the current capacity utilization (in bytes)
                                             // of the namespace

    buf.total = ns_data.nsze * lba_size;
    buf.available = (ns_data.ncap - ns_data.nuse) * lba_size;
    buf.raw_use = ns_data.nuse * lba_size;
    dout(0) << __func__ << " stats (nsze/ncap/nuse):"
                        << nsze << "/"
                        << ncap << "/"
                        << nuse
                        << dendl;
  }
  return r;
}

int ExtBlkDevThinNVMe::collect_metadata(const std::string& prefix, std::map<std::string,std::string> *pm)
{
  (*pm)[prefix + "thin_nvme"] = "1";
  return 0;
}
