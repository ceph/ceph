// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/errno.h"
#include "osdc/Striper.h"

#include "PgFiles.h"


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "pgeffects." << __func__ << ": "

int PgFiles::init()
{
  int r = ceph_create_with_context(&cmount, g_ceph_context);
  if (r != 0) {
    return r;
  }

  return ceph_init(cmount);
}

PgFiles::PgFiles(Objecter *o, std::set<pg_t> pgs_)
  : objecter(o), pgs(pgs_)
{
  for (const auto &i : pgs) {
    pools.insert(i.m_pool);
  }
}

PgFiles::~PgFiles()
{
  ceph_release(cmount);
}

void PgFiles::hit_dir(std::string const &path)
{
  dout(10) << "entering " << path << dendl;

  ceph_dir_result *dr = nullptr;
  int r = ceph_opendir(cmount, path.c_str(), &dr);
  if (r != 0) {
    derr << "Failed to open path: " << cpp_strerror(r) << dendl;
    return;
  }

  struct dirent de;
  while((r = ceph_readdir_r(cmount, dr, &de)) != 0) {
    if (r < 0) {
      derr << "Error reading path " << path << ": " << cpp_strerror(r)
           << dendl;
      ceph_closedir(cmount, dr); // best effort, ignore r
      return;
    }

    if (std::string(de.d_name) == "." || std::string(de.d_name) == "..") {
      continue;
    }

    struct ceph_statx stx;
    std::string de_path = (path + std::string("/") + de.d_name);
    r = ceph_statx(cmount, de_path.c_str(), &stx,
		    CEPH_STATX_INO|CEPH_STATX_SIZE, 0);
    if (r != 0) {
      derr << "Failed to stat path " << de_path << ": "
            << cpp_strerror(r) << dendl;
      // Don't hold up the whole process for one bad inode
      continue;
    }

    if (S_ISREG(stx.stx_mode)) {
      hit_file(de_path, stx);
    } else if (S_ISDIR(stx.stx_mode)) {
      hit_dir(de_path);
    } else {
      dout(20) << "Skipping non reg/dir file: " << de_path << dendl;
    }
  }

  r = ceph_closedir(cmount, dr);
  if (r != 0) {
    derr << "Error closing path " << path << ": " << cpp_strerror(r) << dendl;
    return;
  }
}

void PgFiles::hit_file(std::string const &path, const struct ceph_statx &stx)
{
  assert(S_ISREG(stx.stx_mode));

  dout(20) << "Hitting file '" << path << "'" << dendl;

  int l_stripe_unit = 0;
  int l_stripe_count = 0;
  int l_object_size = 0;
  int l_pool_id = 0;
  int r = ceph_get_path_layout(cmount, path.c_str(), &l_stripe_unit,
                               &l_stripe_count, &l_object_size,
                               &l_pool_id);
  if (r != 0) {
    derr << "Error reading layout on " << path << ": " << cpp_strerror(r)
         << dendl;
    return;
  }

  struct file_layout_t layout;
  layout.stripe_unit = l_stripe_unit;
  layout.stripe_count = l_stripe_count;
  layout.object_size = l_object_size;
  layout.pool_id = l_pool_id;

  // Avoid calculating PG if the layout targeted a completely different pool
  if (pools.count(layout.pool_id) == 0) {
    dout(20) << "Fast check missed: pool " << layout.pool_id << " not in "
                "target set" << dendl;
    return;
  }

  auto num_objects = Striper::get_num_objects(layout, stx.stx_size);

  for (uint64_t i = 0; i < num_objects; ++i) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%llx.%08llx", (long long unsigned)stx.stx_ino,
                                              (long long unsigned int)i);
    dout(20) << "  object " << std::string(buf) << dendl;

    pg_t target;
    object_t oid;
    object_locator_t loc;
    loc.pool = layout.pool_id;
    loc.key = std::string(buf);

    unsigned pg_num_mask = 0;
    unsigned pg_num = 0;

    int r = 0;
    objecter->with_osdmap([&r, oid, loc, &target, &pg_num_mask, &pg_num]
                          (const OSDMap &osd_map) {
      r = osd_map.object_locator_to_pg(oid, loc, target);
      if (r == 0) {
        auto pool = osd_map.get_pg_pool(loc.pool);
        pg_num_mask = pool->get_pg_num_mask();
        pg_num = pool->get_pg_num();
      }
    });
    if (r != 0) {
      // Can happen if layout pointed to pool not in osdmap, for example
      continue;
    }

    target.m_seed = ceph_stable_mod(target.ps(), pg_num, pg_num_mask);

    dout(20) << "  target " << target << dendl;

    if (pgs.count(target)) {
      std::cout << path << std::endl;
      return;
    }
  }
  
}

int PgFiles::scan_path(std::string const &path)
{
  int r = ceph_mount(cmount, "/");
  if (r != 0) {
    derr << "Failed to mount: " << cpp_strerror(r) << dendl;
    return r;
  }

  hit_dir(path);

  r = ceph_unmount(cmount);
  if (r != 0) {
    derr << "Failed to unmount: " << cpp_strerror(r) << dendl;
    return r;
  }

  return r;
}

