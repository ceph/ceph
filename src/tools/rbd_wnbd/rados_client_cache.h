/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 Cloudbase Solutions
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "common/debug.h"
#include "common/dout.h"

#include "global/global_init.h"

#include "include/rados/librados.hpp"

// In order to re-use OSD connections, we're caching one rados client
// per cluster.
class RadosClientCache
{
private:
  std::map<std::string, std::weak_ptr<librados::Rados>> cache;
  ceph::mutex cache_lock = ceph::make_mutex("RadosClientCache::MapLock");

  // Remove deleted objects from the map.
  void remove_expired();

  std::shared_ptr<librados::Rados> init_client(
    std::string& entity_name, std::string& cluster_name);

public:
  std::shared_ptr<librados::Rados> get_client(
    std::string& entity_name, std::string& cluster_name);
};
