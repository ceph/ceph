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

#ifndef CEPH_ERASURE_CODE_PLUGIN_H
#define CEPH_ERASURE_CODE_PLUGIN_H

#include "ErasureCodeInterface.h"

namespace ceph {

  class ErasureCodePlugin {
  public:
    void *library;

    ErasureCodePlugin() :
      library(0) {}
    virtual ~ErasureCodePlugin() {}
    virtual int factory(ErasureCodeInterfaceRef *erasure_code,
			const map<std::string,std::string> &parameters) = 0;

    static int load(const std::string &plugin_name,
		    const map<std::string,std::string> &parameters);
 
    static int factory(ErasureCodeInterfaceRef *erasure_code,
		       const std::string &plugin,
		       const map<std::string,std::string> &parameters);
  };

  class ErasureCodePluginRegistry {
  public:
    std::map<std::string,ErasureCodePlugin*> plugins;

    ~ErasureCodePluginRegistry();

    static ErasureCodePluginRegistry &singleton();
  };
}

#endif
