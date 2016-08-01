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

class ErasureCodePluginExample : public ErasureCodePlugin {
public:
  virtual int factory(const std::string &directory,
		      ErasureCodeProfile &profile,
                      ErasureCodeInterfaceRef *erasure_code,
		      ostream *ss)
  {
    *erasure_code = ErasureCodeInterfaceRef(new ErasureCodeExample());
    (*erasure_code)->init(profile, ss);
    return 0;
  }
};

const char *__erasure_code_version() { return CEPH_GIT_NICE_VER; }

int __erasure_code_init(char *plugin_name, char *directory)
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  return instance.add(plugin_name, new ErasureCodePluginExample());
}
