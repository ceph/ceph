/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 XSKY Inc.
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include "acconfig.h"
#include "ceph_ver.h"
#include "CompressionPluginLZ4.h"

#ifndef BUILDING_FOR_EMBEDDED

// -----------------------------------------------------------------------------

const char *__ceph_plugin_version()
{
  return CEPH_GIT_NICE_VER;
}

// -----------------------------------------------------------------------------

int __ceph_plugin_init(CephContext *cct,
                       const std::string& type,
                       const std::string& name)
{
  PluginRegistry *instance = cct->get_plugin_registry();

  return instance->add(type, name, new CompressionPluginLZ4(cct));
}

#endif // !BUILDING_FOR_EMBEDDED
