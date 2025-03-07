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

#include "rados_client_cache.h"

#include "common/errno.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "rbd-wnbd: "

std::shared_ptr<librados::Rados> RadosClientCache::init_client(
  std::string& entity_name, std::string& cluster_name)
{
  auto rados = std::make_shared<librados::Rados>();

  int r = rados->init2(entity_name.c_str(), cluster_name.c_str(), 0);
  if (r < 0) {
    derr << "couldn't initialize rados: " << cpp_strerror(r)
         << dendl;
    return std::shared_ptr<librados::Rados>();
  }

  r = rados->conf_read_file(nullptr);
  if (r < 0) {
    derr << "couldn't read conf file: " << cpp_strerror(r)
         << dendl;
    return std::shared_ptr<librados::Rados>();
  }

  r = rados->connect();
  if (r < 0) {
    derr << "couldn't establish rados connection: "
         << cpp_strerror(r) << dendl;
    return std::shared_ptr<librados::Rados>();
  } else {
    dout(1) << "successfully initialized rados connection" << dendl;
  }

  return rados;
}

std::shared_ptr<librados::Rados> RadosClientCache::get_client(
  std::string& entity_name, std::string& cluster_name)
{
  std::unique_lock l{cache_lock};

  remove_expired();

  std::string key = entity_name + "@" + cluster_name;
  auto cached_client_weak = cache.find(key);
  if (cached_client_weak != cache.end()) {
    if (auto cached_client = cached_client_weak->second.lock()) {
      dout(1) << "reusing cached rados client: " << key << dendl;
      return cached_client;
    } else {
      dout(5) << "cleaning up expired rados ref: "
              << cached_client_weak->first << dendl;
      cache.erase(cached_client_weak);
    }
  }

  dout(1) << "creating new rados client: " << key << dendl;
  auto client = init_client(entity_name, cluster_name);
  cache.insert(std::pair{key, client});
  return client;
}

void RadosClientCache::remove_expired()
{
  auto i = cache.begin();
  while (i != cache.end()) {
    if (i->second.expired()) {
      dout(5) << "removing expired rados ref: "
              << i->first << dendl;
      i = cache.erase(i);
      continue;
    }
    i++;
  }
}
