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


class RbdMapping
{
private:
  Config cfg;
  // We're sharing the rados object across mappings in order to
  // reuse the OSD connections.
  librados::Rados &rados;
  std::string command_line;

  librbd::RBD rbd;
  librados::IoCtx io_ctx;
  librbd::Image image;
  uint64_t initial_image_size;

  WnbdHandler* handler = nullptr;
  uint64_t watch_handle;
  WNBDWatchCtx* watch_ctx = nullptr;

  ceph::mutex shutdown_lock = ceph::make_mutex("RbdMapping::ShutdownLock");

  int init();
  void shutdown();

public:
  RbdMapping(Config& _cfg, librados::Rados& _rados,
             std::string _command_line)
    : cfg(_cfg)
    , rados(_rados)
    , command_line(_command_line)
  {
  }

  ~RbdMapping()
  {
      shutdown();
  }

  int start();
  int wait();
};
