// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include <unistd.h>

#include "ceph_ver.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "ErasureCodeExample.h"

using namespace std;

class ErasureCodePluginExample : public ErasureCodePlugin {
public:
  int factory(const std::string &directory,
		      ErasureCodeProfile &profile,
                      ErasureCodeInterfaceRef *erasure_code,
		      ostream *ss) override
  {
    auto ec = std::make_unique<ErasureCodeExample>();
    if (int r = ec->init(profile, ss); r) {
      return r;
    }
    erasure_code->reset(ec.release());
    return 0;
  }
};

const char *__erasure_code_version() { return CEPH_GIT_NICE_VER; }

int __erasure_code_init(char *plugin_name, char *directory)
{
  auto& instance = ErasureCodePluginRegistry::instance();
  auto plugin = std::make_unique<ErasureCodePluginExample>();
  int r = instance.add(plugin_name, plugin.get());
  if (r == 0) {
    std::ignore = plugin.release();
  }
  return r;
}
