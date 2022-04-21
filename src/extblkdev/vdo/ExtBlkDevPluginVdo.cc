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

int ExtBlkDevPluginVdo::factory(ceph::ExtBlkDevProfile &profile,
				ceph::ExtBlkDevInterfaceRef *ext_blk_dev,
				std::ostream *ss) {
  auto interface = std::make_unique<ExtBlkDevVdo>();
  int r = interface->init(profile, ss);
  if (r != 0) {
    return r;
  }
  *ext_blk_dev = ceph::ExtBlkDevInterfaceRef(interface.release());
  return 0;
};

const char *__ext_blk_dev_version() { return CEPH_GIT_NICE_VER; }

ceph::ExtBlkDevPlugin* __ext_blk_dev_init(char *plugin_name)
{
  return new ExtBlkDevPluginVdo();
}
