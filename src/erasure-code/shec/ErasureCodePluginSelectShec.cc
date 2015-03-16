// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
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

#include "ceph_ver.h"
#include "common/debug.h"
#include "arch/probe.h"
#include "arch/intel.h"
#include "arch/arm.h"
#include "erasure-code/ErasureCodePlugin.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodePluginSelectShec: ";
}

static string get_variant() {
  ceph_arch_probe();

  if (ceph_arch_intel_pclmul &&
      ceph_arch_intel_sse42 &&
      ceph_arch_intel_sse41 &&
      ceph_arch_intel_ssse3 &&
      ceph_arch_intel_sse3 &&
      ceph_arch_intel_sse2) {
    return "sse4";
  } else if (ceph_arch_intel_ssse3 &&
	     ceph_arch_intel_sse3 &&
	     ceph_arch_intel_sse2) {
    return "sse3";
  } else if (ceph_arch_neon) {
    return "neon";
  } else {
    return "generic";
  }
}

class ErasureCodePluginSelectShec : public ErasureCodePlugin {
public:
  virtual int factory(const map<string,string> &parameters,
		      ErasureCodeInterfaceRef *erasure_code) {
    ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
    stringstream ss;
    int ret;
    string name = "shec";
    if (parameters.count("shec-name"))
      name = parameters.find("shec-name")->second;
    if (parameters.count("shec-variant")) {
      dout(10) << "shec-variant "
	       << parameters.find("shec-variant")->second << dendl;
      ret = instance.factory(name + "_" + parameters.find("shec-variant")->second,
			     parameters, erasure_code, ss);
    } else {
      string variant = get_variant();
      dout(10) << variant << " plugin" << dendl;
      ret = instance.factory(name + "_" + variant, parameters, erasure_code, ss);
    }
    if (ret)
      derr << ss.str() << dendl;
    return ret;
  }
};

const char *__erasure_code_version() { return CEPH_GIT_NICE_VER; }

int __erasure_code_init(char *plugin_name, char *directory = (char *)"")
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  string variant = get_variant();
  ErasureCodePlugin *plugin;
  stringstream ss;
  int r = instance.load(plugin_name + string("_") + variant,
			directory, &plugin, ss);
  if (r) {
    derr << ss.str() << dendl;
    return r;
  }
  dout(10) << ss.str() << dendl;
  return instance.add(plugin_name, new ErasureCodePluginSelectShec());
}
