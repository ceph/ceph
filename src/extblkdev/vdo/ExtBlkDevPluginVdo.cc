// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * (C) Copyright IBM Corporation 2022
 * Author: Martin Ohmacht <mohmacht@us.ibm.com>
 *
 * Based on the file src/erasure-code/clay/ErasureCodePluginClay.cc
 * Copyright (C) 2018 Indian Institute of Science <office.ece@iisc.ac.in>
 *
 * Author: Myna Vajha <mynaramana@gmail.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include "ceph_ver.h"
#include "ExtBlkDevPluginVdo.h"
#include "common/ceph_context.h"


// This plugin does not require any capabilities to be set
int ExtBlkDevPluginVdo::get_required_cap_set(cap_t caps)
{
  return 0;
}


int ExtBlkDevPluginVdo::factory(const std::string& logdevname,
				ceph::ExtBlkDevInterfaceRef& ext_blk_dev)
{
  auto vdo = new ExtBlkDevVdo(cct);
  int r = vdo->init(logdevname);
  if (r != 0) {
    delete vdo;
    return r;
  }
  ext_blk_dev.reset(vdo);
  return 0;
};

const char *__ceph_plugin_version() { return CEPH_GIT_NICE_VER; }

int __ceph_plugin_init(CephContext *cct,
		       const std::string& type,
		       const std::string& name)
{
  auto plg = new ExtBlkDevPluginVdo(cct);
  if(plg == 0) return -ENOMEM;
  int rc = cct->get_plugin_registry()->add(type, name, plg);
  if(rc != 0){
    delete plg;
  }
  return rc;
}
