// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_ZFSFILESTOREBACKEND_H
#define CEPH_ZFSFILESTOREBACKEND_H

#ifdef HAVE_LIBZFS
#include "GenericFileStoreBackend.h"
#include "os/fs/ZFS.h"

class ZFSFileStoreBackend : public GenericFileStoreBackend {
private:
  ZFS zfs;
  ZFS::Handle *base_zh;
  ZFS::Handle *current_zh;
  bool m_filestore_zfs_snap;
  int update_current_zh();
public:
  explicit ZFSFileStoreBackend(FileStore *fs);
  ~ZFSFileStoreBackend();
  int detect_features();
  bool can_checkpoint();
  int create_current();
  int list_checkpoints(list<string>& ls);
  int create_checkpoint(const string& name, uint64_t *cid);
  int rollback_to(const string& name);
  int destroy_checkpoint(const string& name);
};
#endif
#endif
