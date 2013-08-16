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

#ifndef CEPH_GENERICFILESTOREBACKEDN_H
#define CEPH_GENERICFILESTOREBACKEDN_H

#include "FileStore.h"

class GenericFileStoreBackend : public FileStoreBackend {
private:
  bool ioctl_fiemap;
  bool m_filestore_fiemap;
  bool m_filestore_fsync_flushes_journal_data;
public:
  GenericFileStoreBackend(FileStore *fs);
  virtual ~GenericFileStoreBackend() {};
  virtual int detect_features();
  virtual int create_current();
  virtual bool can_checkpoint() { return false; };
  virtual int list_checkpoints(list<string>& ls) { return 0; }
  virtual int create_checkpoint(const string& name, uint64_t *cid) { return -EOPNOTSUPP; }
  virtual int sync_checkpoint(uint64_t id) { return -EOPNOTSUPP; }
  virtual int rollback_to(const string& name) { return -EOPNOTSUPP; }
  virtual int destroy_checkpoint(const string& name) { return -EOPNOTSUPP; }
  virtual int syncfs();
  virtual bool has_fiemap() { return ioctl_fiemap; }
  virtual int do_fiemap(int fd, off_t start, size_t len, struct fiemap **pfiemap);
  virtual int clone_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff) {
    return _copy_range(from, to, srcoff, len, dstoff);
  }
};
#endif
