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

#include "rados_client_cache.h"
#include "rbd_mapping_config.h"
#include "wnbd_handler.h"

class WNBDWatchCtx : public librbd::UpdateWatchCtx
{
private:
  librados::IoCtx &io_ctx;
  WnbdHandler* handler;
  librbd::Image &image;
  uint64_t size;
public:
  WNBDWatchCtx(librados::IoCtx& io_ctx, WnbdHandler* handler,
               librbd::Image& image, uint64_t size)
    : io_ctx(io_ctx)
    , handler(handler)
    , image(image)
    , size(size)
  {
  }

  ~WNBDWatchCtx() override {}

  void handle_notify() override
  {
    uint64_t new_size;

    if (image.size(&new_size) == 0 && new_size != size &&
        handler->resize(new_size) == 0) {
      size = new_size;
    }
  }
};

typedef std::function<void(std::string devpath, int ret)> disconnect_cbk_t;

class RbdMapping
{
private:
  Config cfg;
  // We're sharing the rados object across mappings in order to
  // reuse the OSD connections.
  RadosClientCache& client_cache;
  std::shared_ptr<librados::Rados> rados;

  librbd::RBD rbd;
  librados::IoCtx io_ctx;
  librbd::Image image;
  uint64_t initial_image_size;

  WnbdHandler* handler = nullptr;
  uint64_t watch_handle;
  WNBDWatchCtx* watch_ctx = nullptr;
  bool saved_cfg_to_registry = false;
  disconnect_cbk_t disconnect_cbk;

  ceph::mutex shutdown_lock = ceph::make_mutex("RbdMapping::ShutdownLock");
  std::thread monitor_thread;

  int init();

public:
  RbdMapping(Config& _cfg,
             RadosClientCache& _client_cache)
    : cfg(_cfg)
    , client_cache(_client_cache)
  {}

  RbdMapping(Config& _cfg,
             RadosClientCache& _client_cache,
             disconnect_cbk_t _disconnect_cbk)
    : cfg(_cfg)
    , client_cache(_client_cache)
    , disconnect_cbk(_disconnect_cbk)
  {}

  ~RbdMapping();

  int start();
  // Wait until the image gets disconnected.
  int wait();
  void shutdown();
};

// Wait for the mapped disk to become available.
int wait_mapped_disk(Config& cfg);

class RbdMappingDispatcher
{
private:
  RadosClientCache& client_cache;

  std::map<std::string, std::shared_ptr<RbdMapping>> mappings;
  ceph::mutex map_mutex = ceph::make_mutex("RbdMappingDispatcher::MapMutex");

  void disconnect_cbk(std::string devpath, int ret);

public:
  RbdMappingDispatcher(RadosClientCache& _client_cache)
    : client_cache(_client_cache)
  {}

  int create(Config& cfg);
  std::shared_ptr<RbdMapping> get_mapping(std::string& devpath);
};
