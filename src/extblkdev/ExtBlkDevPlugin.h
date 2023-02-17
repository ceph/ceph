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

#include "ExtBlkDevInterface.h"

namespace ceph {

  namespace extblkdev {
    int preload(CephContext *cct);
    int detect_device(CephContext *cct,
			  const std::string &logdevname,
			  ExtBlkDevInterfaceRef& ebd_impl);
    int release_device(ExtBlkDevInterfaceRef& ebd_impl);
  }
}

#endif
