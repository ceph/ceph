// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#ifndef __LIBRBD_HPP
#define __LIBRBD_HPP

#include <stdbool.h>
#include <string>
#include <list>
#include <map>
#include <vector>
#include "buffer.h"
#include "librados.hpp"
#include "librbd.h"

namespace librbd {
  class RBDClient;
  typedef void *pool_t;
#if 0 // for IO
  typedef void *completion_t;
  typedef void (*callback_t)(completion_t cb, void *arg);
#endif

  typedef struct {
    uint64_t id;
    uint64_t size;
    std::string name;
  } snap_info_t;

  typedef struct {
    uint64_t size;
    uint64_t obj_size;
    uint64_t num_objs;
    int order;
  } image_info_t;

class RBD
{
  RBDClient *client;

public:
  RBD() {}
  ~RBD() {}

  /* We don't allow assignment or copying */
  RBD(const RBD& rhs);
  const RBD& operator=(const RBD& rhs);

  int initialize(int argc, const char *argv[]);
  void shutdown();

  void version(int *major, int *minor, int *extra);

  int open_pool(const char *pool_name, pool_t *pool);
  int close_pool(pool_t pool);
  int create(pool_t pool, const char *name, size_t size);
  int remove(pool_t pool, const char *name);
  int resize(pool_t pool, const char *name, size_t size);
  int stat(pool_t pool, const char *name, image_info_t& info);
  int list(pool_t pool, std::vector<string>& names);

  int create_snap(pool_t pool, const char *image_name, const char *snapname);
  int remove_snap(pool_t pool, const char *image_name, const char *snapname);
  int rollback_snap(pool_t pool, const char *image_name, const char *snapname);
  int list_snaps(pool_t pool, const char *image_name, std::vector<snap_info_t>& snaps);
};

}

#endif
