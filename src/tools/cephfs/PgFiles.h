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

#ifndef PG_EFFECTS_H_
#define PG_EFFECTS_H_

#include "include/cephfs/libcephfs.h"
#include "osd/osd_types.h"
#include <set>
#include "osdc/Objecter.h"

/**
 * This utility scans the files (via an online MDS) and works out
 * which ones rely on named PGs.  For use when someone has
 * some bad/damaged PGs and wants to see which files might be
 * affected.
 */
class PgFiles
{
private:
  Objecter *objecter;
  struct ceph_mount_info *cmount = nullptr;

  std::set<pg_t> pgs;
  std::set<uint64_t> pools;

  void hit_file(std::string const &path, const struct ceph_statx &stx);
  void hit_dir(std::string const &path);


public:
  PgFiles(Objecter *o, std::set<pg_t> pgs_);
  ~PgFiles();

  int init();
  int scan_path(std::string const &path);
};

#endif

