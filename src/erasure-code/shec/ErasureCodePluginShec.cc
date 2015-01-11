// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 FUJITSU LIMITED
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Takanori Nakao <nakao.takanori@jp.fujitsu.com>
 * Author: Takeshi Miyamae <miyamae.takeshi@jp.fujitsu.com>
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
#include "erasure-code/ErasureCodePlugin.h"
#include "ErasureCodeShec.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodePluginShec: ";
}

class ErasureCodePluginShec : public ErasureCodePlugin {
public:
  virtual int factory(const map<std::string,std::string> &parameters,
		      ErasureCodeInterfaceRef *erasure_code) {
    ErasureCodeShec *interface;
    std::string t = "";

    if (parameters.find("technique") != parameters.end()){
      t = parameters.find("technique")->second;
    }

    if (t == "" || t == "single" || t == "multiple") {
      interface = new ErasureCodeShecReedSolomonVandermonde(t);
    } else {
      derr << "technique=" << t << " is not a valid coding technique. "
	   << " Choose one of the following: "
	   << "single, multiple"
	   << dendl;
      return -ENOENT;
    }
    int err = interface->init(parameters);
    if (err) {
      return err;
    }
    *erasure_code = ErasureCodeInterfaceRef(interface);

    dout(0) << "ErasureCodePluginShec: factory() completed" << dendl;

    return 0;
  }
};

const char *__erasure_code_version() { return CEPH_GIT_NICE_VER; }


int __erasure_code_init(char *plugin_name, char *directory)
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  return instance.add(plugin_name, new ErasureCodePluginShec());
}

int __erasure_code_init(char *plugin_name)
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  return instance.add(plugin_name, new ErasureCodePluginShec());
}
