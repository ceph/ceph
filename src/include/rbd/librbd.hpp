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

  class ProgressContext
  {
  public:
    virtual ~ProgressContext();
    virtual int update_progress(uint64_t offset, uint64_t total) = 0;
  };

class RBD
{
public:
  RBD();
  ~RBD();

  struct AioCompletion {
    void *pc;
    AioCompletion(void *cb_arg, callback_t complete_cb);
    int wait_for_complete();
    ssize_t get_return_value();
    void release();
  };

  void version(int *major, int *minor, int *extra);

  int open(IoCtx& io_ctx, Image& image, const char *name);
  int open(IoCtx& io_ctx, Image& image, const char *name, const char *snapname);
  int list(IoCtx& io_ctx, std::vector<std::string>& names);
  int create(IoCtx& io_ctx, const char *name, uint64_t size, int *order);
  int create2(IoCtx& io_ctx, const char *name, uint64_t size,
	      uint64_t features, int *order);
  int clone(IoCtx& p_ioctx, const char *p_name, const char *p_snapname,
	       IoCtx& c_ioctx, const char *c_name, uint64_t features,
	       int *c_order);
  int remove(IoCtx& io_ctx, const char *name);
  int remove_with_progress(IoCtx& io_ctx, const char *name, ProgressContext& pctx);
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

  int resize(uint64_t size);
  int resize_with_progress(uint64_t size, ProgressContext& pctx);
  int stat(image_info_t &info, size_t infosize);
  int parent_info(std::string *parent_poolname, std::string *parent_name,
		      std::string *parent_snapname);
  int old_format(uint8_t *old);
  int features(uint64_t *features);
  int overlap(uint64_t *overlap);
  int copy(IoCtx& dest_io_ctx, const char *destname);
  int copy_with_progress(IoCtx& dest_io_ctx, const char *destname,
			 ProgressContext &prog_ctx);

  /* cooperative locking */
  int list_locks(std::set<std::pair<std::string, std::string> > &locks,
                 bool &exclusive);
  int lock_exclusive(const std::string& cookie);
  int lock_shared(const std::string& cookie);
  int unlock(const std::string& cookie);
  int break_lock(const std::string& other_locker, const std::string& cookie);

  /* snapshots */
  int snap_list(std::vector<snap_info_t>& snaps);
  int snap_create(const char *snapname);
  int snap_remove(const char *snapname);
  int snap_rollback(const char *snap_name);
  int snap_rollback_with_progress(const char *snap_name, ProgressContext& pctx);
  int snap_set(const char *snap_name);

  /* I/O */
  ssize_t read(uint64_t ofs, size_t len, ceph::bufferlist& bl);
  int64_t read_iterate(uint64_t ofs, size_t len,
		       int (*cb)(uint64_t, size_t, const char *, void *), void *arg);
  ssize_t write(uint64_t ofs, size_t len, ceph::bufferlist& bl);
  int discard(uint64_t ofs, uint64_t len);

  int aio_write(uint64_t off, size_t len, ceph::bufferlist& bl, RBD::AioCompletion *c);
  int aio_read(uint64_t off, size_t len, ceph::bufferlist& bl, RBD::AioCompletion *c);
  int aio_discard(uint64_t off, uint64_t len, RBD::AioCompletion *c);

  int flush();

private:
  friend class RBD;

  Image(const Image& rhs);
  const Image& operator=(const Image& rhs);

  image_ctx_t ctx;
};

}

#endif
