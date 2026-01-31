// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include <set>
#include "ceph_ver.h"
#include "ExtBlkDevPluginThinNVMe.h"
#include "common/ceph_context.h"


// This plugin does not require any capabilities to be set
int ExtBlkDevPluginThinNVMe::get_required_cap_set(cap_t caps)
{
  cap_value_t arr[1];
  arr[0] = CAP_SYS_ADMIN;
  if (cap_set_flag(caps, CAP_PERMITTED, 1, arr, CAP_SET) < 0) {
    return -errno;
  }
  if (cap_set_flag(caps, CAP_EFFECTIVE, 1, arr, CAP_SET) < 0) {
    return -errno;
  }
  return 0;
}


int ExtBlkDevPluginThinNVMe::factory(const std::string& logdevname,
                                const std::string& device,
				ceph::ExtBlkDevInterfaceRef& ext_blk_dev)
{
  auto dev = new ExtBlkDevThinNVMe(cct);
  int r = dev->init(logdevname, device);
  if (r != 0) {
    delete dev;
    return r;
  }
  ext_blk_dev.reset(dev);
  return 0;
};

const char *__ceph_plugin_version() { return CEPH_GIT_NICE_VER; }

int __ceph_plugin_init(CephContext *cct,
		       const std::string& type,
		       const std::string& name)
{
  auto plg = new ExtBlkDevPluginThinNVMe(cct);
  if(plg == 0) return -ENOMEM;
  int rc = cct->get_plugin_registry()->add(type, name, plg);
  if(rc != 0){
    delete plg;
  }
  return rc;
}