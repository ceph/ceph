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
  bool m_filestore_fiemap;
  bool m_filestore_seek_data_hole;
  bool m_filestore_fsync_flushes_journal_data;
  bool m_filestore_splice;
public:
  explicit GenericFileStoreBackend(FileStore *fs);
  virtual ~GenericFileStoreBackend() {}

  virtual const char *get_name() {
    return "generic";
  }
  virtual int detect_features();
  virtual int create_current();
  virtual bool can_checkpoint() { return false; }
  virtual int list_checkpoints(list<string>& ls) { return 0; }
  virtual int create_checkpoint(const string& name, uint64_t *cid) { return -EOPNOTSUPP; }
  virtual int sync_checkpoint(uint64_t id) { return -EOPNOTSUPP; }
  virtual int rollback_to(const string& name) { return -EOPNOTSUPP; }
  virtual int destroy_checkpoint(const string& name) { return -EOPNOTSUPP; }
  virtual int syncfs();
  virtual bool has_fiemap() { return ioctl_fiemap; }
  virtual bool has_seek_data_hole() { return seek_data_hole; }
  virtual int do_fiemap(int fd, off_t start, size_t len, struct fiemap **pfiemap);
  virtual int clone_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff) {
    return _copy_range(from, to, srcoff, len, dstoff);
  }
  virtual int set_alloc_hint(int fd, uint64_t hint) { return -EOPNOTSUPP; }
  virtual bool has_splice() const { return m_filestore_splice; }
private:
  int _crc_load_or_init(int fd, SloppyCRCMap *cm);
  int _crc_save(int fd, SloppyCRCMap *cm);
public:
  virtual int _crc_update_write(int fd, loff_t off, size_t len, const bufferlist& bl);
  virtual int _crc_update_truncate(int fd, loff_t off);
  virtual int _crc_update_zero(int fd, loff_t off, size_t len);
  virtual int _crc_update_clone_range(int srcfd, int destfd,
				      loff_t srcoff, size_t len, loff_t dstoff);
  virtual int _crc_verify_read(int fd, loff_t off, size_t len, const bufferlist& bl,
			       ostream *out);
};
#endif
