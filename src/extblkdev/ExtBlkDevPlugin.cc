// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * (C) Copyright IBM Corporation 2022
 * Author: Martin Ohmacht <mohmacht@us.ibm.com>
 *
 * Based on the file ceph/src/erasure-code/ErasureCodePlugin.cc
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

#include <errno.h>

#include "ceph_ver.h"
#include "ExtBlkDevPlugin.h"
#include "common/errno.h"
#include "include/dlfcn_compat.h"
#include "include/str_list.h"
#include "include/ceph_assert.h"

using namespace std;

#define PLUGIN_PREFIX "libebd_"
#define PLUGIN_SUFFIX SHARED_LIB_SUFFIX
#define PLUGIN_INIT_FUNCTION "__ext_blk_dev_init"
#define PLUGIN_VERSION_FUNCTION "__ext_blk_dev_version"

namespace ceph {

ExtBlkDevPluginRegistry ExtBlkDevPluginRegistry::singleton;

ExtBlkDevPluginRegistry::ExtBlkDevPluginRegistry() = default;

ExtBlkDevPluginRegistry::~ExtBlkDevPluginRegistry()
{
  if (disable_dlclose)
    return;

  for (auto& i : plugins) {
    void *library = i.second->library;
    delete i.second;
    dlclose(library);
  }
}

int ExtBlkDevPluginRegistry::remove(const std::string &name)
{
  ceph_assert(ceph_mutex_is_locked(lock));
  auto plugin = plugins.find(name);
  if (plugin == plugins.end())
    return -ENOENT;
  void *library = plugin->second->library;
  delete plugin->second;
  dlclose(library);
  plugins.erase(plugin);
  return 0;
}

int ExtBlkDevPluginRegistry::add(const std::string &name,
                                   ExtBlkDevPlugin* plugin)
{
  ceph_assert(ceph_mutex_is_locked(lock));
  if (plugins.find(name) != plugins.end())
    return -EEXIST;
  plugins[name] = plugin;
  return 0;
}

ExtBlkDevPlugin *ExtBlkDevPluginRegistry::get(const std::string &name)
{
  ceph_assert(ceph_mutex_is_locked(lock));
  auto plugin = plugins.find(name);
  if (plugin == plugins.end())
    return 0;
  return plugin->second;
}

int ExtBlkDevPluginRegistry::factory(const std::string &plugin_name,
				       const std::string &directory,
				       ExtBlkDevProfile &profile,
				       ExtBlkDevInterfaceRef *ext_blk_dev,
				       ostream *ss)
{
  ExtBlkDevPlugin *plugin;
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

  int r = plugin->factory(profile, ext_blk_dev, ss);
  if (r)
    return r;
  if (profile != (*ext_blk_dev)->get_profile()) {
    *ss << __func__ << " profile " << profile << " != get_profile() "
	<< (*ext_blk_dev)->get_profile() << std::endl;
    return -EINVAL;
  }
  return 0;
}

static const char *an_older_version() {
  return "an older version";
}

int ExtBlkDevPluginRegistry::load(const std::string &plugin_name,
				    const std::string &directory,
				    ExtBlkDevPlugin **plugin,
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

  const char * (*extblkdev_version)() =
    (const char *(*)())dlsym(library, PLUGIN_VERSION_FUNCTION);
  if (extblkdev_version == NULL)
    extblkdev_version = an_older_version;
  if (extblkdev_version() != string(CEPH_GIT_NICE_VER)) {
    *ss << "expected plugin " << fname << " version " << CEPH_GIT_NICE_VER
	<< " but it claims to be " << extblkdev_version() << " instead";
    dlclose(library);
    return -EXDEV;
  }

  ExtBlkDevPlugin* (*extblkdev_init)(const char *) =
    (ExtBlkDevPlugin* (*)(const char *))dlsym(library, PLUGIN_INIT_FUNCTION);
  if (extblkdev_init) {
    std::string name = plugin_name;
    ExtBlkDevPlugin* plg = extblkdev_init(name.c_str());
    if (plg == 0) {
      *ss << "extblkdev_init(" << plugin_name
	  << ")";
      dlclose(library);
      return -ENOENT;
    }
    int r=add(plugin_name, plg);
    if (r != 0) {
      *ss << "add(" << plugin_name
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

int ExtBlkDevPluginRegistry::preload(const std::string &plugins,
				       const std::string &directory,
				       ostream *ss)
{
  std::lock_guard l{lock};
  list<string> plugins_list;
  get_str_list(plugins, plugins_list);
  for (auto& i : plugins_list) {
    ExtBlkDevPlugin *plugin;
    int r = load(i, directory, &plugin, ss);
    if (r)
      return r;
  }
  return 0;
}

int ExtBlkDevPluginRegistry::detect_device(const std::string &devname,
					   ExtBlkDevInterfaceRef& ebd_impl,
					   std::string &plg_name,
					   std::ostream *ss)
{
  ExtBlkDevProfile profile;
  profile.devname=devname;
  int rc=-ENOENT;
  for (auto& i : plugins) {
    if(ss){
      *ss << __func__ << " Trying to detect block device " << devname 
	  << " using plugin " << i.first << std::endl;
    }
    rc=i.second->factory(profile, &ebd_impl, ss);
    if(rc==0){
      plg_name=i.first;
      break;
    }
  }
  return rc;
}

}
