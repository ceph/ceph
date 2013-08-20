// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include "ErasureCodePlugin.h"
#include "ErasureCodeExample.h"

class ErasureCodePluginExample : public ErasureCodePlugin {
public:
  virtual int factory(ErasureCodeInterfaceRef *erasure_code,
                      const map<std::string,std::string> &parameters) {
    *erasure_code = ErasureCodeInterfaceRef(new ErasureCodeExample(parameters));
    return 0;
  }
};

extern "C" {
void __erasure_code_init(char *plugin_name);
}

void __erasure_code_init(char *plugin_name)
{
  ErasureCodePluginRegistry &registry = ErasureCodePluginRegistry::singleton();

  registry.plugins[plugin_name] = new ErasureCodePluginExample();
}
