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

  using librados::IoCtx;

  class Image;
  typedef void *image_ctx_t;
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
    AioCompletion(void *cb_arg, callback_t complete_cb);
    int wait_for_complete();
    int get_return_value();
    void release();
  };

  void version(int *major, int *minor, int *extra);

  int open(IoCtx& io_ctx, Image& image, const char *name);
  int open(IoCtx& io_ctx, Image& image, const char *name, const char *snapname);
  int list(IoCtx& io_ctx, std::vector<std::string>& names);
  int create(IoCtx& io_ctx, const char *name, size_t size, int *order);
  int remove(IoCtx& io_ctx, const char *name);
  int copy(IoCtx& src_io_ctx, const char *srcname, IoCtx& dest_io_ctx, const char *destname);
  int rename(IoCtx& src_io_ctx, const char *srcname, const char *destname);

private:
  /* We don't allow assignment or copying */
  RBD(const RBD& rhs);
  const RBD& operator=(const RBD& rhs);
};

class Image
{
public:
  Image();
  ~Image();

  int resize(size_t size);
  int stat(image_info_t &info, size_t infosize);

  /* snapshots */
  int snap_list(std::vector<snap_info_t>& snaps);
  int snap_create(const char *snapname);
  int snap_remove(const char *snapname);
  int snap_rollback(const char *snap_name);
  int snap_set(const char *snap_name);

  /* I/O */
  int read(off_t ofs, size_t len, ceph::bufferlist& bl);
  int read_iterate(off_t ofs, size_t len,
                   int (*cb)(off_t, size_t, const char *, void *), void *arg);
  int write(off_t ofs, size_t len, ceph::bufferlist& bl);

  int aio_write(off_t off, size_t len, ceph::bufferlist& bl, RBD::AioCompletion *c);
  int aio_read(off_t off, size_t len, ceph::bufferlist& bl, RBD::AioCompletion *c);

private:
  friend class RBD;

  Image(const Image& rhs);
  const Image& operator=(const Image& rhs);

  image_ctx_t ctx;
};

}

#endif
