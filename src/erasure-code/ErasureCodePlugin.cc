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

#include <errno.h>
#include <dlfcn.h>

#include "ErasureCodePlugin.h"

#define PLUGIN_PREFIX "libec_"
#define PLUGIN_SUFFIX ".so"
#define PLUGIN_INIT_FUNCTION "__erasure_code_init"

ErasureCodePluginRegistry ErasureCodePluginRegistry::singleton;

ErasureCodePluginRegistry::ErasureCodePluginRegistry() :
  lock("ErasureCodePluginRegistry::lock"),
  loading(false),
  disable_dlclose(false)
{
}

ErasureCodePluginRegistry::~ErasureCodePluginRegistry()
{
  if (disable_dlclose)
    return;

  for (std::map<std::string,ErasureCodePlugin*>::iterator i = plugins.begin();
       i != plugins.end();
       ++i) {
    void *library = i->second->library;
    delete i->second;
    dlclose(library);
  }
}

int ErasureCodePluginRegistry::add(const std::string &name,
                                   ErasureCodePlugin* plugin)
{
  if (plugins.find(name) != plugins.end())
    return -EEXIST;
  plugins[name] = plugin;
  return 0;
}

ErasureCodePlugin *ErasureCodePluginRegistry::get(const std::string &name)
{
  if (plugins.find(name) != plugins.end())
    return plugins[name];
  else
    return 0;
}

int ErasureCodePluginRegistry::factory(const std::string &plugin_name,
				       const map<std::string,std::string> &parameters,
				       ErasureCodeInterfaceRef *erasure_code,
				       ostream &ss)
{
  ErasureCodePlugin *plugin;
  {
    Mutex::Locker l(lock);
    int r = 0;
    plugin = get(plugin_name);
    if (plugin == 0) {
      loading = true;
      r = load(plugin_name, parameters, &plugin, ss);
      loading = false;
      if (r != 0)
	return r;
    }
  }

  return plugin->factory(parameters, erasure_code);
}

int ErasureCodePluginRegistry::load(const std::string &plugin_name,
				    const map<std::string,std::string> &parameters,
				    ErasureCodePlugin **plugin,
				    ostream &ss)
{
  assert(parameters.count("directory") != 0);
  std::string fname = parameters.find("directory")->second
    + "/" PLUGIN_PREFIX
    + plugin_name + PLUGIN_SUFFIX;
  void *library = dlopen(fname.c_str(), RTLD_NOW);
  if (!library) {
    ss << "load dlopen(" << fname << "): " << dlerror();
    return -EIO;
  }

  int (*erasure_code_init)(const char *) =
    (int (*)(const char *))dlsym(library, PLUGIN_INIT_FUNCTION);
  if (erasure_code_init) {
    std::string name = plugin_name;
    int r = erasure_code_init(name.c_str());
    if (r != 0) {
      ss << "erasure_code_init(" << plugin_name
	 << "): " << strerror(-r);
      dlclose(library);
      return r;
    }
  } else {
    ss << "load dlsym(" << fname
       << ", " << PLUGIN_INIT_FUNCTION
       << "): " << dlerror();
    dlclose(library);
    return -ENOENT;
  }

  *plugin = get(plugin_name);
  if (*plugin == 0) {
    ss << "load " << PLUGIN_INIT_FUNCTION << "()"
       << "did not register " << plugin_name;
    dlclose(library);
    return -EBADF;
  }

  (*plugin)->library = library;

  return 0;
}

