// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include "common/debug.h"
#include "osd/ErasureCodePlugin.h"
#include "ErasureCodePyramid.h"

// re-include our assert
#include "include/assert.h" 

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodePluginPyramid: ";
}

class ErasureCodePluginPyramid : public ErasureCodePlugin {
public:
  virtual int factory(const map<std::string,std::string> &parameters,
		      ErasureCodeInterfaceRef *erasure_code) {
    ErasureCodePyramid *interface;
    interface = new ErasureCodePyramid();
    stringstream ass;
    assert(parameters.count("erasure-code-directory") != 0);
    int r = interface->init(parameters, &ass);
    if (r) {
      derr << ass.str() << dendl;
      delete interface;
      return r;
    }
    *erasure_code = ErasureCodeInterfaceRef(interface);
    return 0;
  }
};

int __erasure_code_init(char *plugin_name)
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  return instance.add(plugin_name, new ErasureCodePluginPyramid());
}
