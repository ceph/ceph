// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef CEPH_EXT_BLK_DEV_PLUGIN_THIN_NVME_H
#define CEPH_EXT_BLK_DEV_PLUGIN_THIN_NVME_H

#include "ExtBlkDevThinNVMe.h"

class ExtBlkDevPluginThinNVMe : public ceph::ExtBlkDevPlugin {
public:
  explicit ExtBlkDevPluginThinNVMe(CephContext *cct) : ExtBlkDevPlugin(cct) {}
  int get_required_cap_set(cap_t caps) override;
  int factory(const std::string& logdevname,
              const std::string& device,
	      ceph::ExtBlkDevInterfaceRef& ext_blk_dev) override;
};

#endif
