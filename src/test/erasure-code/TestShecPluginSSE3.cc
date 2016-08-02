// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 * Copyright (C) 2015 FUJITSU LIMITED
 *
 * Author: Loic Dachary <loic@dachary.org>
 * Author: Shotaro Kawaguchi <kawaguchi.s@jp.fujitsu.com>
 * Author: Takanori Nakao <nakao.takanori@jp.fujitsu.com>
 * Author: Takeshi Miyamae <miyamae.takeshi@jp.fujitsu.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include "ceph_ver.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "ErasureCodeExample.h"
 
class ErasureCodePluginShecSse3 : public ErasureCodePlugin {
public:

  ErasureCodePluginShecSse3(CephContext* cct) : ErasureCodePlugin(cct)
  {}
  
  virtual int factory(ErasureCodeProfile &profile,
                      ErasureCodeInterfaceRef *erasure_code,
                      ostream *ss)
  {
    return -333;
  }
};

extern "C" const char *__ceph_plugin_version() { return CEPH_GIT_NICE_VER; }

extern "C" int __ceph_plugin_init(CephContext *cct,
                                  const std::string& type,
                                  const std::string& name)
{
  PluginRegistry *instance = cct->get_plugin_registry();
  return instance->add(type, name, new ErasureCodePluginShecSse3(cct));
}
