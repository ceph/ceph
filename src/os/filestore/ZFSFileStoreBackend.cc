// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/int_types.h"
#include "include/types.h"

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>

#include "include/compat.h"
#include "include/linux_fiemap.h"
#include "include/color.h"
#include "include/buffer.h"
#include "include/assert.h"

#include <iostream>
#include <fstream>
#include <sstream>

#include "common/errno.h"
#include "common/config.h"
#include "common/sync_filesystem.h"

#ifdef HAVE_LIBZFS

#include "ZFSFileStoreBackend.h"

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "zfsfilestorebackend(" << get_basedir_path() << ") "

ZFSFileStoreBackend::ZFSFileStoreBackend(FileStore *fs) :
  GenericFileStoreBackend(fs), base_zh(NULL), current_zh(NULL),
  m_filestore_zfs_snap(g_conf->filestore_zfs_snap)
{
  int ret = zfs.init();
  if (ret < 0) {
    dout(0) << "ZFSFileStoreBackend: failed to init libzfs" << dendl;
    return;
  }

  base_zh = zfs.path_to_zhandle(get_basedir_path().c_str(), ZFS::TYPE_FILESYSTEM);
  if (!base_zh) {
    dout(0) << "ZFSFileStoreBackend: failed to get zfs handler for basedir" << dendl;
    return;
  }

  update_current_zh();
}

ZFSFileStoreBackend::~ZFSFileStoreBackend()
{
  if (base_zh)
    zfs.close(base_zh);
  if (current_zh)
    zfs.close(current_zh);
}

int ZFSFileStoreBackend::update_current_zh()
{
  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s/current", zfs.get_name(base_zh));
  ZFS::Handle *zh = zfs.open(path, ZFS::TYPE_FILESYSTEM);
  if (zh) {
    char *mnt;
    if (zfs.is_mounted(zh, &mnt)) {
      int ret = get_current_path() == mnt;
      free(mnt);
      if (ret) {
	current_zh = zh;
	return 0;
      }
    } else {
      int ret = zfs.mount(zh, NULL, 0);
      if (ret < 0) {
	ret = -errno;
	dout(0) << "update_current_zh: zfs_mount '" << zfs.get_name(zh)
		<< "' got " << cpp_strerror(ret) << dendl;
	return ret;
      }
    }
    zfs.close(zh);
  } else {
    dout(0) << "update_current_zh: zfs_open '" << path << "' got NULL" << dendl;
    return -ENOENT;
  }

  zh = zfs.path_to_zhandle(get_current_path().c_str(), ZFS::TYPE_FILESYSTEM);
  if (zh) {
    if (strcmp(zfs.get_name(base_zh), zfs.get_name(zh))) {
      current_zh = zh;
      return 0;
    }
    zfs.close(zh);
    dout(0) << "update_current_zh: basedir and current/ on the same filesystem" << dendl;
  } else {
    dout(0) << "update_current_zh: current/ not exist" << dendl;
  }
  return -ENOENT;
}

int ZFSFileStoreBackend::detect_features()
{
  if (!current_zh)
    dout(0) << "detect_features: null zfs handle for current/" << dendl;
  return 0;
}

bool ZFSFileStoreBackend::can_checkpoint()
{
  return m_filestore_zfs_snap && current_zh != NULL;
}

int ZFSFileStoreBackend::create_current()
{
  struct stat st;
  int ret = ::stat(get_current_path().c_str(), &st);
  if (ret == 0) {
    // current/ exists
    if (!S_ISDIR(st.st_mode)) {
      dout(0) << "create_current: current/ exists but is not a directory" << dendl;
      return -ENOTDIR;
    }
    return 0;
  } else if (errno != ENOENT) {
    ret = -errno;
    dout(0) << "create_current: cannot stat current/ " << cpp_strerror(ret) << dendl;
    return ret;
  }

  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s/current", zfs.get_name(base_zh));
  ret = zfs.create(path, ZFS::TYPE_FILESYSTEM);
  if (ret < 0 && errno != EEXIST) {
    ret = -errno;
    dout(0) << "create_current: zfs_create '" << path << "' got " << cpp_strerror(ret) << dendl;
    return ret;
  }

  ret = update_current_zh();
  return ret;
}

static int list_checkpoints_callback(ZFS::Handle *zh, void *data)
{
  list<string> *ls = static_cast<list<string> *>(data);
  string str = ZFS::get_name(zh);
  size_t pos = str.find('@');
  assert(pos != string::npos && pos + 1 != str.length());
  ls->push_back(str.substr(pos + 1));
  return 0;
}

int ZFSFileStoreBackend::list_checkpoints(list<string>& ls)
{
  dout(10) << "list_checkpoints:" << dendl;
  if (!current_zh)
    return -EINVAL;

  list<string> snaps;
  int ret = zfs.iter_snapshots_sorted(current_zh, list_checkpoints_callback, &snaps);
  if (ret < 0) {
    ret = -errno;
    dout(0) << "list_checkpoints: zfs_iter_snapshots_sorted got" << cpp_strerror(ret) << dendl;
    return ret;
  }
  ls.swap(snaps);
  return 0;
}

int ZFSFileStoreBackend::create_checkpoint(const string& name, uint64_t *cid)
{
  dout(10) << "create_checkpoint: '" << name << "'" << dendl;
  if (!current_zh)
    return -EINVAL;

  // looks like zfsonlinux doesn't flush dirty data when taking snapshot
  int ret = sync_filesystem(get_current_fd());
  if (ret < 0) {
    ret = -errno;
    dout(0) << "create_checkpoint: sync_filesystem got" << cpp_strerror(ret) << dendl;
    return ret;
  }

  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s@%s", zfs.get_name(current_zh), name.c_str());
  ret = zfs.snapshot(path, false);
  if (ret < 0) {
    ret = -errno;
    dout(0) << "create_checkpoint: zfs_snapshot '" << path << "' got" << cpp_strerror(ret) << dendl;
    return ret;
  }
  if (cid)
    *cid = 0;
  return 0;
}

int ZFSFileStoreBackend::rollback_to(const string& name)
{
  dout(10) << "rollback_to: '" << name << "'" << dendl;
  if (!current_zh)
    return -EINVAL;

  // umount current to avoid triggering online rollback deadlock
  int ret;
  if (zfs.is_mounted(current_zh, NULL)) {
    ret = zfs.umount(current_zh, NULL, 0);
    if (ret < 0) {
      ret = -errno;
      dout(0) << "rollback_to: zfs_umount '" << zfs.get_name(current_zh) << "' got" << cpp_strerror(ret) << dendl;
    }
  }

  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s@%s", zfs.get_name(current_zh), name.c_str());

  ZFS::Handle *snap_zh = zfs.open(path, ZFS::TYPE_SNAPSHOT);
  if (!snap_zh) {
    dout(0) << "rollback_to: zfs_open '" << path << "' got NULL" << dendl;
    return -ENOENT;
  }

  ret = zfs.rollback(current_zh, snap_zh, false);
  if (ret < 0) {
    ret = -errno;
    dout(0) << "rollback_to: zfs_rollback '" << zfs.get_name(snap_zh) << "' got" << cpp_strerror(ret) << dendl;
  }

  if (!zfs.is_mounted(current_zh, NULL)) {
    int ret = zfs.mount(current_zh, NULL, 0);
    if (ret < 0) {
      ret = -errno;
      dout(0) << "update_current_zh: zfs_mount '" << zfs.get_name(current_zh) << "' got " << cpp_strerror(ret) << dendl;
      return ret;
    }
  }

  zfs.close(snap_zh);
  return ret;
}

int ZFSFileStoreBackend::destroy_checkpoint(const string& name)
{
  dout(10) << "destroy_checkpoint: '" << name << "'" << dendl;
  if (!current_zh)
    return -EINVAL;

  int ret = zfs.destroy_snaps(current_zh, name.c_str(), true);
  if (ret < 0) {
    ret = -errno;
    dout(0) << "destroy_checkpoint: zfs_destroy_snaps '" << name << "' got" << cpp_strerror(ret) << dendl;
  }
  return ret;
}
#endif
