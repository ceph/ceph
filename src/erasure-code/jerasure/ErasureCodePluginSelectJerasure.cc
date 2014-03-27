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
#include "arch/probe.h"
#include "arch/intel.h"
#include "erasure-code/ErasureCodePlugin.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodePluginSelectJerasure: ";
}

class ErasureCodePluginSelectJerasure : public ErasureCodePlugin {
public:
  virtual int factory(const map<std::string,std::string> &parameters,
		      ErasureCodeInterfaceRef *erasure_code) {
    ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
    stringstream ss;
    int ret;
    ceph_arch_probe();
    string name = "jerasure";
    if (parameters.count("jerasure-name"))
      name = parameters.find("jerasure-name")->second;
    if (ceph_arch_intel_pclmul &&
	ceph_arch_intel_sse42 &&
	ceph_arch_intel_sse41 &&
	ceph_arch_intel_ssse3 &&
	ceph_arch_intel_sse3 &&
	ceph_arch_intel_sse2) {
      dout(10) << "SSE4 plugin" << dendl;
      ret = instance.factory(name + "_sse4", parameters, erasure_code, ss);
    } else if (ceph_arch_intel_ssse3 &&
	       ceph_arch_intel_sse3 &&
	       ceph_arch_intel_sse2) {
      dout(10) << "SSE3 plugin" << dendl;
      ret = instance.factory(name + "_sse3", parameters, erasure_code, ss);
    } else {
      dout(10) << "generic plugin" << dendl;
      ret = instance.factory(name + "_generic", parameters, erasure_code, ss);
    }
    if (ret)
      derr << ss.str() << dendl;
    return ret;
  }
};

int __erasure_code_init(char *plugin_name)
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  return instance.add(plugin_name, new ErasureCodePluginSelectJerasure());
}
