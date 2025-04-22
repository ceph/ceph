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

#ifndef CEPH_EXT_BLK_DEV_THIN_NVME_H
#define CEPH_EXT_BLK_DEV_THIN_NVME_H

#include "extblkdev/ExtBlkDevInterface.h"
#include "include/compat.h"

class ExtBlkDevThinNVMe final : public ceph::ExtBlkDevInterface
{
  std::string name;   // name of the underlying vdo device
  std::string logdevname; // name of the top level logical device
  int dev = -1;
  int ns = -1;
  int fd = -1;
  CephContext *cct;

public:
  explicit ExtBlkDevThinNVMe(CephContext *cct) : cct(cct) {}
  ~ExtBlkDevThinNVMe() {
    if (fd >= 0)
      VOID_TEMP_FAILURE_RETRY(::close(fd));
  }
  int init(const std::string& logdevname) override;
  const std::string& get_devname() const override {return name;}
  int get_statfs(store_statfs_t& statfs) override;
  int collect_metadata(const std::string& prefix, std::map<std::string,std::string> *pm) override;
};

#endif
