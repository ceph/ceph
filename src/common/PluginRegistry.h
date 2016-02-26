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

#ifndef CEPH_COMMON_PLUGINREGISTRY_H
#define CEPH_COMMON_PLUGINREGISTRY_H

#include <string>
#include <map>

#include "common/Mutex.h"

class CephContext;

extern "C" {
  const char *__ceph_plugin_version();
  int __ceph_plugin_init(CephContext *cct,
			 const std::string& type,
			 const std::string& name);
}

namespace ceph {

  class Plugin {
  public:
    void *library;
    CephContext *cct;

    explicit Plugin(CephContext *cct) : library(NULL), cct(cct) {}
    virtual ~Plugin() {}
  };

  class PluginRegistry {
  public:
    CephContext *cct;
    Mutex lock;
    bool loading;
    bool disable_dlclose;
    std::map<std::string,std::map<std::string,Plugin*> > plugins;

    explicit PluginRegistry(CephContext *cct);
    ~PluginRegistry();

    int add(const std::string& type, const std::string& name,
	    Plugin *factory);
    int remove(const std::string& type, const std::string& name);
    Plugin *get(const std::string& type, const std::string& name);
    Plugin *get_with_load(const std::string& type, const std::string& name);

    int load(const std::string& type,
	     const std::string& name);
    int preload();
    int preload(const std::string& type);
  };
}

#endif
