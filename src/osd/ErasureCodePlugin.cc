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

#include "common/debug.h"

#include <dlfcn.h>

#include "ErasureCodePlugin.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodePlugin: ";
}

#define PLUGIN_PREFIX "libec_"
#define PLUGIN_SUFFIX ".so"
#define PLUGIN_INIT_FUNCTION "__erasure_code_init"

int ErasureCodePlugin::factory(ErasureCodeInterfaceRef *erasure_code,
			       const std::string &plugin_name,
			       const map<std::string,std::string> &parameters) {
  ErasureCodePluginRegistry &registry = ErasureCodePluginRegistry::singleton();
  int r = 0;
  if (registry.plugins.count(plugin_name) == 0) {
    r = load(plugin_name, parameters);
    if (r != 0)
      return r;
  }

  ErasureCodePlugin *plugin = registry.plugins[plugin_name];
  return plugin->factory(erasure_code, parameters);
}

int ErasureCodePlugin::load(const std::string &plugin_name, const map<std::string,std::string> &parameters) {

  assert(parameters.count("directory") != 0);
  std::string fname = parameters.find("directory")->second + "/" PLUGIN_PREFIX
    + plugin_name + PLUGIN_SUFFIX;
  dout(10) << "load " << plugin_name << " from " << fname << dendl;

  void *library = dlopen(fname.c_str(), RTLD_NOW);
  if (!library) {
    derr << "load dlopen(" << fname
	    << "): " << dlerror() << dendl;
    return -EIO;
  }

  void (*erasure_code_init)(const char *) = (void (*)(const char *))dlsym(library, PLUGIN_INIT_FUNCTION);
  if (erasure_code_init) {
    std::string name = plugin_name;
    erasure_code_init(name.c_str());
  } else {
    derr << "load dlsym(" << fname
	    << ", " << PLUGIN_INIT_FUNCTION
	    << "): " << dlerror() << dendl;
    dlclose(library);
    return -ENOENT;
  }

  ErasureCodePluginRegistry &registry = ErasureCodePluginRegistry::singleton();

  if (registry.plugins.count(plugin_name) == 0) {
    derr << "load " << PLUGIN_INIT_FUNCTION << "()"
	 << "did not register " << plugin_name << dendl;
    dlclose(library);
    return -EBADF;
  }

  ErasureCodePlugin *plugin = registry.plugins[plugin_name];

  plugin->library = library;

  return 0;
}

static ErasureCodePluginRegistry registry;

ErasureCodePluginRegistry::~ErasureCodePluginRegistry() {
  for (std::map<std::string,ErasureCodePlugin*>::iterator i = plugins.begin();
       i != plugins.end();
       i++) {
    void *library = i->second->library;
    delete i->second;
    dlclose(library);
  }
}

ErasureCodePluginRegistry &ErasureCodePluginRegistry::singleton() {
  return registry;
}
