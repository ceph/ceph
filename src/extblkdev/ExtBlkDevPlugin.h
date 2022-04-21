// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * (C) Copyright IBM Corporation 2022
 * Author: Martin Ohmacht <mohmacht@us.ibm.com>
 *
 * Based on the file ceph/src/erasure-code/ErasureCodePlugin.h
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

#ifndef CEPH_EXT_BLK_DEV_PLUGIN_H
#define CEPH_EXT_BLK_DEV_PLUGIN_H

#include "common/ceph_mutex.h"
#include "ExtBlkDevInterface.h"

namespace ceph {

  class ExtBlkDevPluginRegistry {
  public:
    ceph::mutex lock = ceph::make_mutex("ExtBlkDevPluginRegistry::lock");
    bool loading = false;
    bool disable_dlclose = false;
    std::map<std::string,ExtBlkDevPlugin*> plugins;

    static ExtBlkDevPluginRegistry singleton;

    ExtBlkDevPluginRegistry();
    ~ExtBlkDevPluginRegistry();

    static ExtBlkDevPluginRegistry &instance() {
      return singleton;
    }

    int factory(const std::string &plugin,
		const std::string &directory,
		ExtBlkDevProfile &profile,
		ExtBlkDevInterfaceRef *ext_blk_dev,
		std::ostream *ss);

    int add(const std::string &name, ExtBlkDevPlugin *plugin);
    int remove(const std::string &name);
    ExtBlkDevPlugin *get(const std::string &name);

    int load(const std::string &plugin_name,
	     const std::string &directory,
	     ExtBlkDevPlugin **plugin,
	     std::ostream *ss);

    int preload(const std::string &plugins,
		const std::string &directory,
		std::ostream *ss);
    int detect_device(const std::string &devname,
		      ExtBlkDevInterfaceRef& ebd_impl,
		      std::string &plg_name,
		      std::ostream *ss);
  };
}

#endif
