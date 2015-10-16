// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
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
#include "erasure-code/ErasureCodePlugin.h"
#include "ErasureCodeJerasure.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodePluginJerasure: ";
}

class ErasureCodePluginJerasure : public ErasureCodePlugin {
public:
  virtual int factory(const std::string& directory,
		      ErasureCodeProfile &profile,
		      ErasureCodeInterfaceRef *erasure_code,
		      ostream *ss) {
    ErasureCodeJerasure *interface;
    std::string t;
    if (profile.find("technique") != profile.end())
      t = profile.find("technique")->second;
    if (t == "reed_sol_van") {
      interface = new ErasureCodeJerasureReedSolomonVandermonde();
    } else if (t == "reed_sol_r6_op") {
      interface = new ErasureCodeJerasureReedSolomonRAID6();
    } else if (t == "cauchy_orig") {
      interface = new ErasureCodeJerasureCauchyOrig();
    } else if (t == "cauchy_good") {
      interface = new ErasureCodeJerasureCauchyGood();
    } else if (t == "liberation") {
      interface = new ErasureCodeJerasureLiberation();
    } else if (t == "blaum_roth") {
      interface = new ErasureCodeJerasureBlaumRoth();
    } else if (t == "liber8tion") {
      interface = new ErasureCodeJerasureLiber8tion();
    } else {
      derr << "technique=" << t << " is not a valid coding technique. "
	   << " Choose one of the following: "
	   << "reed_sol_van, reed_sol_r6_op, cauchy_orig, "
	   << "cauchy_good, liberation, blaum_roth, liber8tion"
	   << dendl;
      return -ENOENT;
    }
    dout(20) << __func__ << ": " << profile << dendl;
    int r = interface->init(profile, ss);
    if (r) {
      delete interface;
      return r;
    }
    *erasure_code = ErasureCodeInterfaceRef(interface);
    return 0;
  }
};

extern "C" {
#include "galois.h"

extern gf_t *gfp_array[];
extern int  gfp_is_composite[];
}

const char *__erasure_code_version() { return CEPH_GIT_NICE_VER; }

int __erasure_code_init(char *plugin_name, char *directory)
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  int w[] = { 4, 8, 16, 32 };
  for(int i = 0; i < 4; i++) {
    int r = galois_init_default_field(w[i]);
    if (r) {
      derr << "failed to gf_init_easy(" << w[i] << ")" << dendl;
      return -r;
    }
  }
  return instance.add(plugin_name, new ErasureCodePluginJerasure());
}
