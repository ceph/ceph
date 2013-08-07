// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#define HAVE_IOCTL_IN_SYS_IOCTL_H
#include <libzfs.h>
#include "ZFS.h"

const int ZFS::TYPE_FILESYSTEM 	= ZFS_TYPE_FILESYSTEM;
const int ZFS::TYPE_SNAPSHOT	= ZFS_TYPE_SNAPSHOT;
const int ZFS::TYPE_VOLUME	= ZFS_TYPE_VOLUME;
const int ZFS::TYPE_DATASET	= ZFS_TYPE_DATASET;

ZFS::~ZFS()
{
  if (g_zfs)
    ::libzfs_fini((libzfs_handle_t*)g_zfs);
}

int ZFS::init()
{
  g_zfs = ::libzfs_init();
  return g_zfs ? 0 : -EINVAL;
}

ZFS::Handle *ZFS::open(const char *n, int t)
{
  return (ZFS::Handle*)::zfs_open((libzfs_handle_t*)g_zfs, n, (zfs_type_t)t);
}

void ZFS::close(ZFS::Handle *h)
{
  ::zfs_close((zfs_handle_t*)h);
}

const char *ZFS::get_name(ZFS::Handle *h)
{
  return ::zfs_get_name((zfs_handle_t*)h);
}

ZFS::Handle *ZFS::path_to_zhandle(const char *p, int t)
{
  return ::zfs_path_to_zhandle((libzfs_handle_t*)g_zfs, (char *)p, (zfs_type_t)t);
}

int ZFS::create(const char *n, int t)
{
  return ::zfs_create((libzfs_handle_t*)g_zfs, n, (zfs_type_t)t, NULL);
}

int ZFS::snapshot(const char *n, bool r)
{
  return ::zfs_snapshot((libzfs_handle_t*)g_zfs, n, (boolean_t)r, NULL);
}

int ZFS::rollback(ZFS::Handle *h, ZFS::Handle *snap, bool f)
{
  return ::zfs_rollback((zfs_handle_t*)h, (zfs_handle_t*)snap, (boolean_t)f);
}

int ZFS::destroy_snaps(ZFS::Handle *h, const char *n, bool d)
{
  return ::zfs_destroy_snaps((zfs_handle_t*)h, (char *)n, (boolean_t)d);
}

bool ZFS::is_mounted(ZFS::Handle *h, char **p)
{
  return (bool)::zfs_is_mounted((zfs_handle_t*)h, p);
}

int ZFS::mount(ZFS::Handle *h, const char *o, int f)
{
  return ::zfs_mount((zfs_handle_t*)h, o, f);
}

int ZFS::umount(ZFS::Handle *h, const char *o, int f)
{
  return ::zfs_unmount((zfs_handle_t*)h, o, f);
}

int ZFS::iter_snapshots_sorted(ZFS::Handle *h, ZFS::iter_func f, void *d)
{
  return ::zfs_iter_snapshots_sorted((zfs_handle_t*)h, (zfs_iter_f)f, d);
}
