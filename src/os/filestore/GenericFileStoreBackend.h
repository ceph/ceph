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

class SloppyCRCMap;

class GenericFileStoreBackend : public FileStoreBackend {
private:
  bool ioctl_fiemap;
  bool seek_data_hole;
  bool use_splice;
  bool m_filestore_fiemap;
  bool m_filestore_seek_data_hole;
  bool m_filestore_fsync_flushes_journal_data;
  bool m_filestore_splice;
  bool m_rotational = true;
  bool m_journal_rotational = true;
public:
  explicit GenericFileStoreBackend(FileStore *fs);
  ~GenericFileStoreBackend() override {}

  const char *get_name() override {
    return "generic";
  }
  int detect_features() override;
  int create_current() override;
  bool can_checkpoint() override { return false; }
  bool is_rotational() override {
    return m_rotational;
  }
  bool is_journal_rotational() override {
    return m_journal_rotational;
  }
  int list_checkpoints(list<string>& ls) override { return 0; }
  int create_checkpoint(const string& name, uint64_t *cid) override { return -EOPNOTSUPP; }
  int sync_checkpoint(uint64_t id) override { return -EOPNOTSUPP; }
  int rollback_to(const string& name) override { return -EOPNOTSUPP; }
  int destroy_checkpoint(const string& name) override { return -EOPNOTSUPP; }
  int syncfs() override;
  bool has_fiemap() override { return ioctl_fiemap; }
  bool has_seek_data_hole() override { return seek_data_hole; }
  int do_fiemap(int fd, off_t start, size_t len, struct fiemap **pfiemap) override;
  int clone_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff) override {
    return _copy_range(from, to, srcoff, len, dstoff);
  }
  int set_alloc_hint(int fd, uint64_t hint) override { return -EOPNOTSUPP; }
  bool has_splice() const override { return use_splice; }
private:
  int _crc_load_or_init(int fd, SloppyCRCMap *cm);
  int _crc_save(int fd, SloppyCRCMap *cm);
public:
  int _crc_update_write(int fd, loff_t off, size_t len, const bufferlist& bl) override;
  int _crc_update_truncate(int fd, loff_t off) override;
  int _crc_update_zero(int fd, loff_t off, size_t len) override;
  int _crc_update_clone_range(int srcfd, int destfd,
				      loff_t srcoff, size_t len, loff_t dstoff) override;
  int _crc_verify_read(int fd, loff_t off, size_t len, const bufferlist& bl,
			       ostream *out) override;
};
#endif
