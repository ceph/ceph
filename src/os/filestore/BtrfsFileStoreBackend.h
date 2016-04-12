// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_BTRFSFILESTOREBACKEDN_H
#define CEPH_BTRFSFILESTOREBACKEDN_H

#if defined(__linux__)
#include "GenericFileStoreBackend.h"

class BtrfsFileStoreBackend : public GenericFileStoreBackend {
private:
  bool has_clone_range;       ///< clone range ioctl is supported
  bool has_snap_create;       ///< snap create ioctl is supported
  bool has_snap_destroy;      ///< snap destroy ioctl is supported
  bool has_snap_create_v2;    ///< snap create v2 ioctl (async!) is supported
  bool has_wait_sync;         ///< wait sync ioctl is supported
  bool stable_commits;
  bool m_filestore_btrfs_clone_range;
  bool m_filestore_btrfs_snap;
public:
  explicit BtrfsFileStoreBackend(FileStore *fs);
  ~BtrfsFileStoreBackend() {}
  const char *get_name() {
    return "btrfs";
  }
  int detect_features();
  bool can_checkpoint();
  int create_current();
  int list_checkpoints(list<string>& ls);
  int create_checkpoint(const string& name, uint64_t *cid);
  int sync_checkpoint(uint64_t cid);
  int rollback_to(const string& name);
  int destroy_checkpoint(const string& name);
  int syncfs();
  int clone_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff);
};
#endif
#endif
