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
#include "../rados/buffer.h"
#include "../rados/librados.hpp"
#include "librbd.h"

namespace librbd {

  using librados::pool_t;

  class RBDClient;
  typedef void *image_t;
  typedef void *completion_t;
  typedef void (*callback_t)(completion_t cb, void *arg);

  typedef struct {
    uint64_t id;
    uint64_t size;
    std::string name;
  } snap_info_t;

  typedef rbd_image_info_t image_info_t;

class RBD
{
public:
  RBD();
  ~RBD();

  struct AioCompletion {
    void *pc;
    AioCompletion(void *_pc) : pc(_pc) {}
    int wait_for_complete();
    int get_return_value();
    void release();
  };

  /* We don't allow assignment or copying */
  RBD(const RBD& rhs);
  const RBD& operator=(const RBD& rhs);

  void version(int *major, int *minor, int *extra);

  int list(pool_t pool, std::vector<std::string>& names);
  int create(pool_t pool, const char *name, size_t size, int *order);
  int remove(pool_t pool, const char *name);
  int copy(pool_t src_pool, const char *srcname, pool_t dest_pool, const char *destname);
  int rename(pool_t src_pool, const char *srcname, const char *destname);

  int open(pool_t pool, const char *name, image_t *image, const char *snap_name = NULL);
  int close(image_t image);
  int resize(image_t image, size_t size);
  int stat(image_t image, image_info_t& info, size_t infosize);

  /* snapshots */
  int snap_list(image_t image, std::vector<snap_info_t>& snaps);
  int snap_create(image_t image, const char *snap_name);
  int snap_remove(image_t image, const char *snap_name);
  int snap_rollback(image_t image, const char *snap_name);
  int snap_set(image_t image, const char *snap_name);

  /* I/O */
  int read(image_t image, off_t ofs, size_t len, ceph::bufferlist& bl);
  int read_iterate(image_t image, off_t ofs, size_t len,
                   int (*cb)(off_t, size_t, const char *, void *), void *arg);
  int write(image_t image, off_t ofs, size_t len, ceph::bufferlist& bl);

  AioCompletion *aio_create_completion(void *cb_arg, callback_t complete_cb);
  int aio_write(image_t image, off_t off, size_t len, ceph::bufferlist& bl,
                AioCompletion *c);
  int aio_read(image_t image, off_t off, size_t len, ceph::bufferlist& bl, AioCompletion *c);
};

}

#endif
