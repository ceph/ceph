// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
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

#include "common/Mutex.h"
#include "ErasureCodeInterface.h"

extern "C" {
  int __erasure_code_init(char *plugin_name);
}

namespace ceph {

  class ErasureCodePlugin {
  public:
    void *library;

    ErasureCodePlugin() :
      library(0) {}
    virtual ~ErasureCodePlugin() {}

    virtual int factory(const map<std::string,std::string> &parameters,
                        ErasureCodeInterfaceRef *erasure_code) = 0;
  };

  class ErasureCodePluginRegistry {
  public:
    Mutex lock;
    bool loading;
    bool disable_dlclose;
    std::map<std::string,ErasureCodePlugin*> plugins;

    static ErasureCodePluginRegistry singleton;

    ErasureCodePluginRegistry();
    ~ErasureCodePluginRegistry();

    static ErasureCodePluginRegistry &instance() {
      return singleton;
    }

    int factory(const std::string &plugin,
		const map<std::string,std::string> &parameters,
		ErasureCodeInterfaceRef *erasure_code,
		ostream &ss);

    int add(const std::string &name, ErasureCodePlugin *plugin);
    ErasureCodePlugin *get(const std::string &name);

    int load(const std::string &plugin_name,
	     const map<std::string,std::string> &parameters,
	     ErasureCodePlugin **plugin,
	     ostream &ss);

  };
}

#endif
