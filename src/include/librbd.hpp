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

class RBD;

namespace librbd {

  using ceph::bufferlist;

  typedef void *list_ctx_t;
  typedef void *pool_t;
  typedef uint64_t snap_t;

  typedef void *completion_t;
  typedef void (*callback_t)(completion_t cb, void *arg);

  struct SnapContext {
    snap_t seq;
    std::vector<snap_t> snaps;
  };

  struct pools {
    pool_t md;
    pool_t data;
    pool_t dest;
  };

  typedef struct pools pools_t;

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
  librados::Rados rados;

  int do_list(pools_t& pp, const char *poolname, std::vector<string>& names);
  int do_create(pool_t pool, string& md_oid, const char *imgname, uint64_t size, int *order);
  int do_rename(pools_t& pp, string& md_oid, const char *imgname, const char *dstname);
  int do_info(pools_t& pp, string& md_oid, image_info_t& info);
  int do_delete(pools_t& pp, string& md_oid, const char *imgname);
  int do_resize(pools_t& pp, string& md_oid, const char *imgname, uint64_t size);
  int do_list_snaps(pools_t& pp, string& md_oid, std::vector<snap_info_t>& snaps);
  int do_add_snap(pools_t& pp, string& md_oid, const char *snapname);
  int do_rm_snap(pools_t& pp, string& md_oid, const char *snapname);
  int do_get_snapc(pools_t& pp, string& md_oid, const char *snapname,
		   ::SnapContext& snapc, vector<snap_t>& snaps, uint64_t& snapid);
  int do_rollback_snap(pools_t& pp, string& md_oid, ::SnapContext& snapc, uint64_t snapid);
  int do_remove_snap(pools_t& pp, string& md_oid, const char *snapname, uint64_t snapid);
  int do_copy(pools_t& pp, const char *imgname, const char *destname);

  int open_pools(const char *poolname, pools_t& pp);
  void close_pools(pools_t& pp);

  void trim_image(pools_t& pp, const char *imgname, rbd_obj_header_ondisk *header, uint64_t newsize);
  int read_rbd_info(pools_t& pp, string& info_oid, struct rbd_info *info);

  int touch_rbd_info(librados::pool_t pool, string& info_oid);
  int rbd_assign_bid(librados::pool_t pool, string& info_oid, uint64_t *id);
  int read_header_bl(librados::pool_t pool, string& md_oid, bufferlist& header, uint64_t *ver);
  int notify_change(librados::pool_t pool, string& oid, uint64_t *pver);
  int read_header(librados::pool_t pool, string& md_oid, struct rbd_obj_header_ondisk *header, uint64_t *ver);
  int write_header(pools_t& pp, string& md_oid, bufferlist& header);
  int tmap_set(pools_t& pp, string& imgname);
  int tmap_rm(pools_t& pp, string& imgname);
  int rollback_image(pools_t& pp, struct rbd_obj_header_ondisk *header,
		     ::SnapContext& snapc, uint64_t snapid);
  static void image_info(rbd_obj_header_ondisk& header, librbd::image_info_t& info);
  static string get_block_oid(rbd_obj_header_ondisk *header, uint64_t num);
  static uint64_t get_max_block(rbd_obj_header_ondisk *header);
  static uint64_t get_block_size(rbd_obj_header_ondisk *header);
  static uint64_t get_block_num(rbd_obj_header_ondisk *header, uint64_t ofs);
  static int init_rbd_info(struct rbd_info *info);
  static void init_rbd_header(struct rbd_obj_header_ondisk& ondisk,
			      size_t size, int *order, uint64_t bid);


public:
  RBD() {}
  ~RBD() {}

  /* We don't allow assignment or copying */
  RBD(const RBD& rhs);
  const RBD& operator=(const RBD& rhs);

  int initialize(int argc, const char *argv[]);
  void shutdown();

  void version(int *major, int *minor, int *extra);

  int create_image(const char *pool, const char *name, size_t size);
  int remove_image(const char *pool, const char *name);
  int resize_image(const char *pool, const char *name, size_t size);
  int stat_image(const char *pool, const char *name, image_info_t& info);
  int list_images(const char *pool, std::vector<string>& names);

  int create_snap(const char *pool, const char *image_name, const char *snapname);
  int remove_snap(const char *pool, const char *image_name, const char *snapname);
  int rollback_snap(const char *pool, const char *image_name, const char *snapname);
  int list_snaps(const char *pool, const char *image_name, std::vector<snap_info_t>& snaps);
};

}

#endif
