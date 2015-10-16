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
#include "ErasureCodeShecTableCache.h"
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
  ErasureCodeShecTableCache tcache;

  virtual int factory(const std::string &directory,
		      ErasureCodeProfile &profile,
		      ErasureCodeInterfaceRef *erasure_code,
		      ostream *ss) {
    ErasureCodeShec *interface;

    if (profile.find("technique") == profile.end())
      profile["technique"] = "multiple";
    std::string t = profile.find("technique")->second;

    if (t == "single"){
      interface = new ErasureCodeShecReedSolomonVandermonde(tcache, ErasureCodeShec::SINGLE);
    } else if (t == "multiple"){
      interface = new ErasureCodeShecReedSolomonVandermonde(tcache, ErasureCodeShec::MULTIPLE);
    } else {
      *ss << "technique=" << t << " is not a valid coding technique. "
	  << "Choose one of the following: "
	  << "single, multiple ";
      return -ENOENT;
    }
    int r = interface->init(profile, ss);
    if (r) {
      delete interface;
      return r;
    }
    *erasure_code = ErasureCodeInterfaceRef(interface);

    dout(10) << "ErasureCodePluginShec: factory() completed" << dendl;

    return 0;
  }
};

extern "C" {
#include "jerasure/include/galois.h"

extern gf_t *gfp_array[];
extern int  gfp_is_composite[];
}

const char *__erasure_code_version() { return CEPH_GIT_NICE_VER; }

int __erasure_code_init(char *plugin_name, char *directory = (char *)"")
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  int w[] = { 8, 16, 32 };
  for(int i = 0; i < 3; i++) {
    int r = galois_init_default_field(w[i]);
    if (r) {
      derr << "failed to gf_init_easy(" << w[i] << ")" << dendl;
      return -r;
    }
  }
  return instance.add(plugin_name, new ErasureCodePluginShec());
}
