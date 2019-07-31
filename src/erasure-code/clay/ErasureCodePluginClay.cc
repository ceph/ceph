// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
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
#include "common/debug.h"
#include "ErasureCodePluginClay.h"
#include "ErasureCodeClay.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

int ErasureCodePluginClay::factory(const std::string &directory,
				   ceph::ErasureCodeProfile &profile,
				   ceph::ErasureCodeInterfaceRef *erasure_code,
				   std::ostream *ss) {
  auto interface = std::make_unique<ErasureCodeClay>(directory);
  if (int r = interface->init(profile, ss); r) {
    return r;
  }
  *erasure_code = ceph::ErasureCodeInterfaceRef(interface.release());
  return 0;
};

const char *__erasure_code_version() { return CEPH_GIT_NICE_VER; }

int __erasure_code_init(char *plugin_name, char *directory)
{
  auto& instance = ceph::ErasureCodePluginRegistry::instance();
  return instance.add(plugin_name, new ErasureCodePluginClay());
}
