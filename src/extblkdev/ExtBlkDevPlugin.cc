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
#include "common/ceph_context.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_bdev
#define dout_context cct

using namespace std;

namespace ceph {

  namespace extblkdev {


#ifdef __linux__
    // iterate across plugins and determine each capability's reqirement
    // merge requirements into merge_caps set
    int get_required_caps(CephContext *cct, cap_t &merge_caps)
    {
      cap_t plugin_caps = nullptr;
      auto close_caps_on_return = make_scope_guard([&] {
	if (plugin_caps != nullptr) {
	  cap_free(plugin_caps);
	}
      });

      // plugin-private cap set to populate by a plugin
      plugin_caps = cap_init();
      if (plugin_caps == nullptr) {
	return -errno;
      }
      auto registry = cct->get_plugin_registry();
      std::lock_guard l(registry->lock);
      // did we preload any extblkdev type plugins?
      auto ptype = registry->plugins.find("extblkdev");
      if (ptype != registry->plugins.end()) {
	// iterate over all extblkdev plugins
	for (auto& it : ptype->second) {
	  // clear cap set before passing to plugin
	  if (cap_clear(plugin_caps) < 0) {
	    return -errno;
	  }
	  // let plugin populate set with required caps
	  auto ebdplugin = dynamic_cast<ExtBlkDevPlugin*>(it.second);
	  if (ebdplugin == nullptr) {
	    derr << __func__ << " Is not an extblkdev plugin: " << it.first << dendl;
	    return -ENOENT;
	  }
	  int rc = ebdplugin->get_required_cap_set(plugin_caps);
	  if (rc != 0)
	    return rc;
	  // iterate over capabilities and check for active bits
	  for (int i = 0; i <= CAP_LAST_CAP; ++i) {
	    cap_flag_value_t val;
	    if (cap_get_flag(plugin_caps, i, CAP_PERMITTED, &val) < 0) {
	      return -errno;
	    }
	    if (val != CAP_CLEAR) {
	      cap_value_t arr[1];
	      arr[0] = i;
	      // set capability in merged set
	      if (cap_set_flag(merge_caps, CAP_PERMITTED, 1, arr, CAP_SET) < 0) {
		return -errno;
	      }
	    }
	  }
	}
      }
      return 0;
    }

    // trim away all capabilities of this process that are not explicitly set in merge_set
    int trim_caps(CephContext *cct, cap_t &merge_caps)
    {
      cap_t proc_caps = nullptr;
      auto close_caps_on_return = make_scope_guard([&] {
	if (proc_caps != nullptr) {
	  cap_free(proc_caps);
	}
      });
      bool changed = false;
      // get process capability set
      proc_caps = cap_get_proc(); 
      if (proc_caps == nullptr) {
	dout(1) << " cap_get_proc failed with errno: " << errno << dendl;
	return -errno;
      }
      {
	char *cap_str = cap_to_text(proc_caps, 0);
	if (cap_str != nullptr){
	  dout(10) << " cap_get_proc yields: " << cap_str << dendl;
	  cap_free(cap_str);
	}
      }
      // iterate over capabilities
      for (int i = 0; i <= CAP_LAST_CAP; ++i) {
	cap_flag_value_t val;
	if (cap_get_flag(merge_caps, i, CAP_PERMITTED, &val) < 0) {
	  return -errno;
	}
	if (val == CAP_CLEAR) {
	  if (cap_get_flag(proc_caps, i, CAP_PERMITTED, &val) < 0) {
	    return -errno;
	  }
	  if (val != CAP_CLEAR) {
	    // if bit clear in merged set, but set in process set, clear in process set
	    changed = true;
	    cap_value_t arr[1];
	    arr[0] = i;
	    if (cap_set_flag(proc_caps, CAP_PERMITTED, 1, arr, CAP_CLEAR) < 0) {
	      return -errno;
	    }
	    if (cap_set_flag(proc_caps, CAP_EFFECTIVE, 1, arr, CAP_CLEAR) < 0) {
	      return -errno;
	    }
	  }
	}
      }
      // apply reduced capability set to process
      if (changed) {
	char *cap_str = cap_to_text(proc_caps, 0);
	if (cap_str != nullptr){
	  dout(10) << " new caps for cap_set_proc: " << cap_str << dendl;
	  cap_free(cap_str);
	}
	if (cap_set_proc(proc_caps) < 0) {
	  dout(1) << " cap_set_proc failed with errno: " << errno << dendl;
	  return -errno;
	}
      }
      return 0;
    }

    int limit_caps(CephContext *cct)
    {
      cap_t merge_caps = nullptr;
      auto close_caps_on_return = make_scope_guard([&] {
	if (merge_caps != nullptr) {
	  cap_free(merge_caps);
	}
      });
      // collect required caps in merge_caps
      merge_caps = cap_init();
      if (merge_caps == nullptr) {
	return -errno;
      }
      int rc = get_required_caps(cct, merge_caps);
      if (rc != 0) {
	return rc;
      }
      return trim_caps(cct, merge_caps);
    }
#endif

    // preload set of extblkdev plugins defined in config
    int preload(CephContext *cct)
    {
      const auto& conf = cct->_conf;
      string plugins = conf.get_val<std::string>("osd_extblkdev_plugins");
      dout(10) << "starting preload of extblkdev plugins: " << plugins << dendl;

      list<string> plugins_list;
      get_str_list(plugins, plugins_list);

      auto registry = cct->get_plugin_registry();
      {
	std::lock_guard l(registry->lock);
	for (auto& plg : plugins_list) {
	  dout(10) << "starting load of extblkdev plugin: " << plg << dendl;
	  int rc = registry->load("extblkdev", std::string("ebd_") + plg);
	  if (rc) {
	    derr << __func__ << " failed preloading extblkdev plugin: " << plg << dendl;
	    return rc;
	  }else{
	    dout(10) << "successful load of extblkdev plugin: " << plg << dendl;
	  }
	}
      }
#ifdef __linux__
      // if we are still running as root, we do not need to trim capabilities
      // as we are intended to use the privileges
      if (geteuid() == 0) {
	return 0;
      }
      return limit_caps(cct);
#else
      return 0;
#endif
    }


    // scan extblkdev plugins for support of this device
    int detect_device(CephContext *cct,
		      const std::string &logdevname,
		      ExtBlkDevInterfaceRef& ebd_impl)
    {
      int rc = -ENOENT;
      std::string plg_name;
      auto registry = cct->get_plugin_registry();
      std::lock_guard l(registry->lock);
      auto ptype = registry->plugins.find("extblkdev");
      if (ptype == registry->plugins.end()) {
	return -ENOENT;
      }

      for (auto& it : ptype->second) {

	dout(10) << __func__ << " Trying to detect block device " << logdevname 
		      << " using plugin " << it.first << dendl;
	auto ebdplugin = dynamic_cast<ExtBlkDevPlugin*>(it.second);
	if (ebdplugin == nullptr) {
	  derr << __func__ << " Is not an extblkdev plugin: " << it.first << dendl;
	  return -ENOENT;
	}
	rc = ebdplugin->factory(logdevname, ebd_impl);
	if (rc == 0) {
	  plg_name = it.first;
	  break;
	}
      }
      if (rc == 0) {
	dout(1) << __func__ << " using plugin " << plg_name << ", volume " << ebd_impl->get_devname()
		      << " maps to " << logdevname << dendl;
      } else {
	dout(10) << __func__ << " no plugin volume maps to " << logdevname << dendl;
      }
      return rc;
    }

    // release device object
    int release_device(ExtBlkDevInterfaceRef& ebd_impl)
    {
      if (ebd_impl) {
	ebd_impl.reset();
      }
      return 0;
    }

  }
}
