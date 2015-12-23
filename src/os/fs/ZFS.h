// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_ZFS_H
#define CEPH_ZFS_H

// Simple wrapper to hide libzfs.h. (it conflicts with standard linux headers)
class ZFS {
  void *g_zfs;
public:

  static const int TYPE_FILESYSTEM;
  static const int TYPE_SNAPSHOT;
  static const int TYPE_VOLUME;
  static const int TYPE_POOL;
  static const int TYPE_DATASET;

  typedef void Handle;
  typedef int (*iter_func)(Handle *, void *);

  static const char *get_name(Handle *);

  ZFS() : g_zfs(NULL) {}
  ~ZFS();
  int init();
  Handle *open(const char *, int);
  void close(Handle *);
  Handle *path_to_zhandle(const char *, int);
  int create(const char *, int);
  int snapshot(const char *, bool);
  int rollback(Handle *, Handle *, bool);
  int destroy_snaps(Handle *, const char *, bool);
  int iter_snapshots_sorted(Handle *, iter_func, void *);
  int mount(Handle *, const char *, int);
  int umount(Handle *, const char *, int);
  bool is_mounted(Handle *, char **);
};

#endif
