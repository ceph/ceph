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

#include <cassert>
#include <errno.h>

#include "ceph_ver.h"
#include "ErasureCodePlugin.h"
#include "common/errno.h"
#include "include/dlfcn_compat.h"
#include "include/str_list.h"
#include "include/ceph_assert.h"

using namespace std;

#define PLUGIN_PREFIX "libec_"
#define PLUGIN_SUFFIX SHARED_LIB_SUFFIX
#define PLUGIN_INIT_FUNCTION "__erasure_code_init"
#define PLUGIN_VERSION_FUNCTION "__erasure_code_version"

namespace ceph {

ErasureCodePluginRegistry ErasureCodePluginRegistry::singleton;

ErasureCodePluginRegistry::ErasureCodePluginRegistry() = default;

ErasureCodePluginRegistry::~ErasureCodePluginRegistry()
{
  for (auto& name_plugin : plugins) {
    auto *plugin = name_plugin.second;
    assert(plugin);
    void *library = plugin->library;
    delete plugin;
    if (!disable_dlclose) {
      dlclose(library);
    }
  }
}

int ErasureCodePluginRegistry::remove(const std::string &name)
{
  ceph_assert(ceph_mutex_is_locked(lock));
  if (plugins.find(name) == plugins.end())
    return -ENOENT;
  std::map<std::string,ErasureCodePlugin*>::iterator plugin = plugins.find(name);
  void *library = plugin->second->library;
  delete plugin->second;
  dlclose(library);
  plugins.erase(plugin);
  return 0;
}

int ErasureCodePluginRegistry::add(const std::string &name,
                                   ErasureCodePlugin* plugin)
{
  ceph_assert(ceph_mutex_is_locked(lock));
  if (plugins.find(name) != plugins.end())
    return -EEXIST;
  plugins[name] = plugin;
  return 0;
}

ErasureCodePlugin *ErasureCodePluginRegistry::get(const std::string &name)
{
  ceph_assert(ceph_mutex_is_locked(lock));
  if (plugins.find(name) != plugins.end())
    return plugins[name];
  else
    return 0;
}

int ErasureCodePluginRegistry::factory(const std::string &plugin_name,
				       const std::string &directory,
				       ErasureCodeProfile &profile,
				       ErasureCodeInterfaceRef *erasure_code,
				       ostream *ss)
{
  ErasureCodePlugin *plugin;
  {
    std::lock_guard l{lock};
    plugin = get(plugin_name);
    if (plugin == 0) {
      loading = true;
      int r = load(plugin_name, directory, &plugin, ss);
      loading = false;
      if (r != 0)
	return r;
    }
  }

  int r = plugin->factory(directory, profile, erasure_code, ss);
  if (r)
    return r;
  if (profile != (*erasure_code)->get_profile()) {
    *ss << __func__ << " profile " << profile << " != get_profile() "
	<< (*erasure_code)->get_profile() << std::endl;
    return -EINVAL;
  }
  return 0;
}

static const char *an_older_version() {
  return "an older version";
}

int ErasureCodePluginRegistry::load(const std::string &plugin_name,
				    const std::string &directory,
				    ErasureCodePlugin **plugin,
				    ostream *ss)
{
  ceph_assert(ceph_mutex_is_locked(lock));
  std::string fname = directory + "/" PLUGIN_PREFIX
    + plugin_name + PLUGIN_SUFFIX;
  void *library = dlopen(fname.c_str(), RTLD_NOW);
  if (!library) {
    *ss << "load dlopen(" << fname << "): " << dlerror();
    return -EIO;
  }

  const char * (*erasure_code_version)() =
    (const char *(*)())dlsym(library, PLUGIN_VERSION_FUNCTION);
  if (erasure_code_version == NULL)
    erasure_code_version = an_older_version;
  if (erasure_code_version() != string(CEPH_GIT_NICE_VER)) {
    *ss << "expected plugin " << fname << " version " << CEPH_GIT_NICE_VER
	<< " but it claims to be " << erasure_code_version() << " instead";
    dlclose(library);
    return -EXDEV;
  }

  int (*erasure_code_init)(const char *, const char *) =
    (int (*)(const char *, const char *))dlsym(library, PLUGIN_INIT_FUNCTION);
  if (erasure_code_init) {
    std::string name = plugin_name;
    int r = erasure_code_init(name.c_str(), directory.c_str());
    if (r != 0) {
      *ss << "erasure_code_init(" << plugin_name
	  << "," << directory
	  << "): " << cpp_strerror(r);
      dlclose(library);
      return r;
    }
  } else {
    *ss << "load dlsym(" << fname
	<< ", " << PLUGIN_INIT_FUNCTION
	<< "): " << dlerror();
    dlclose(library);
    return -ENOENT;
  }

  *plugin = get(plugin_name);
  if (*plugin == 0) {
    *ss << "load " << PLUGIN_INIT_FUNCTION << "()"
	<< "did not register " << plugin_name;
    dlclose(library);
    return -EBADF;
  }

  (*plugin)->library = library;

  *ss << __func__ << ": " << plugin_name << " ";

  return 0;
}

int ErasureCodePluginRegistry::preload(const std::string &plugins,
				       const std::string &directory,
				       ostream *ss)
{
  std::lock_guard l{lock};
  list<string> plugins_list;
  get_str_list(plugins, plugins_list);
  for (list<string>::iterator i = plugins_list.begin();
       i != plugins_list.end();
       ++i) {
    ErasureCodePlugin *plugin;
    int r = load(*i, directory, &plugin, ss);
    if (r)
      return r;
  }
  return 0;
}
}
