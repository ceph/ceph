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

#ifndef __CYGWIN__
#include "os/fs/btrfs_ioctl.h"
#endif

#include <iostream>
#include <fstream>
#include <sstream>

#include "BtrfsFileStoreBackend.h"

#include "common/errno.h"
#include "common/config.h"

#if defined(__linux__)

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "btrfsfilestorebackend(" << get_basedir_path() << ") "

#define ALIGN_DOWN(x, by) ((x) - ((x) % (by)))
#define ALIGNED(x, by) (!((x) % (by)))
#define ALIGN_UP(x, by) (ALIGNED((x), (by)) ? (x) : (ALIGN_DOWN((x), (by)) + (by)))

BtrfsFileStoreBackend::BtrfsFileStoreBackend(FileStore *fs):
    GenericFileStoreBackend(fs), has_clone_range(false),
    has_snap_create(false), has_snap_destroy(false),
    has_snap_create_v2(false), has_wait_sync(false), stable_commits(false),
    m_filestore_btrfs_clone_range(g_conf->filestore_btrfs_clone_range),
    m_filestore_btrfs_snap (g_conf->filestore_btrfs_snap) { }

int BtrfsFileStoreBackend::detect_features()
{
  int r;

  r = GenericFileStoreBackend::detect_features();
  if (r < 0)
    return r;

  // clone_range?
  if (m_filestore_btrfs_clone_range) {
    int fd = ::openat(get_basedir_fd(), "clone_range_test", O_CREAT|O_WRONLY, 0600);
    if (fd >= 0) {
      if (::unlinkat(get_basedir_fd(), "clone_range_test", 0) < 0) {
	r = -errno;
	dout(0) << "detect_feature: failed to unlink test file for CLONE_RANGE ioctl: "
		<< cpp_strerror(r) << dendl;
      }
      btrfs_ioctl_clone_range_args clone_args;
      memset(&clone_args, 0, sizeof(clone_args));
      clone_args.src_fd = -1;
      r = ::ioctl(fd, BTRFS_IOC_CLONE_RANGE, &clone_args);
      if (r < 0 && errno == EBADF) {
	dout(0) << "detect_feature: CLONE_RANGE ioctl is supported" << dendl;
	has_clone_range = true;
      } else {
	r = -errno;
	dout(0) << "detect_feature: CLONE_RANGE ioctl is NOT supported: " << cpp_strerror(r) << dendl;
      }
      TEMP_FAILURE_RETRY(::close(fd));
    } else {
      r = -errno;
      dout(0) << "detect_feature: failed to create test file for CLONE_RANGE ioctl: "
	      << cpp_strerror(r) << dendl;
    }
  } else {
    dout(0) << "detect_feature: CLONE_RANGE ioctl is DISABLED via 'filestore btrfs clone range' option" << dendl;
  }

  struct btrfs_ioctl_vol_args vol_args;
  memset(&vol_args, 0, sizeof(vol_args));

  // create test source volume
  vol_args.fd = 0;
  strcpy(vol_args.name, "test_subvol");
  r = ::ioctl(get_basedir_fd(), BTRFS_IOC_SUBVOL_CREATE, &vol_args);
  if (r != 0) {
    r = -errno;
    dout(0) << "detect_feature: failed to create simple subvolume " << vol_args.name << ": " << cpp_strerror(r) << dendl;
  }
  int srcfd = ::openat(get_basedir_fd(), vol_args.name, O_RDONLY);
  if (srcfd < 0) {
    r = -errno;
    dout(0) << "detect_feature: failed to open " << vol_args.name << ": " << cpp_strerror(r) << dendl;
  }

  // snap_create and snap_destroy?
  vol_args.fd = srcfd;
  strcpy(vol_args.name, "sync_snap_test");
  r = ::ioctl(get_basedir_fd(), BTRFS_IOC_SNAP_CREATE, &vol_args);
  int err = errno;
  if (r == 0 || errno == EEXIST) {
    dout(0) << "detect_feature: SNAP_CREATE is supported" << dendl;
    has_snap_create = true;

    r = ::ioctl(get_basedir_fd(), BTRFS_IOC_SNAP_DESTROY, &vol_args);
    if (r == 0) {
      dout(0) << "detect_feature: SNAP_DESTROY is supported" << dendl;
      has_snap_destroy = true;
    } else {
      err = -errno;
      dout(0) << "detect_feature: SNAP_DESTROY failed: " << cpp_strerror(err) << dendl;

      if (err == -EPERM && getuid() != 0) {
	dout(0) << "detect_feature: failed with EPERM as non-root; remount with -o user_subvol_rm_allowed" << dendl;
	cerr << TEXT_YELLOW
	     << "btrfs SNAP_DESTROY failed as non-root; remount with -o user_subvol_rm_allowed"
	     << TEXT_NORMAL << std::endl;
      } else if (err == -EOPNOTSUPP) {
	derr << "btrfs SNAP_DESTROY ioctl not supported; you need a kernel newer than 2.6.32" << dendl;
      }
    }
  } else {
    dout(0) << "detect_feature: SNAP_CREATE failed: " << cpp_strerror(err) << dendl;
  }

  if (m_filestore_btrfs_snap) {
    if (has_snap_destroy)
      stable_commits = true;
    else
      dout(0) << "detect_feature: snaps enabled, but no SNAP_DESTROY ioctl; DISABLING" << dendl;
  }

  // start_sync?
  __u64 transid = 0;
  r = ::ioctl(get_basedir_fd(), BTRFS_IOC_START_SYNC, &transid);
  if (r < 0) {
    int err = errno;
    dout(0) << "detect_feature: START_SYNC got " << cpp_strerror(err) << dendl;
  }
  if (r == 0 && transid > 0) {
    dout(0) << "detect_feature: START_SYNC is supported (transid " << transid << ")" << dendl;

    // do we have wait_sync too?
    r = ::ioctl(get_basedir_fd(), BTRFS_IOC_WAIT_SYNC, &transid);
    if (r == 0 || errno == ERANGE) {
      dout(0) << "detect_feature: WAIT_SYNC is supported" << dendl;
      has_wait_sync = true;
    } else {
      int err = errno;
      dout(0) << "detect_feature: WAIT_SYNC is NOT supported: " << cpp_strerror(err) << dendl;
    }
  } else {
    int err = errno;
    dout(0) << "detect_feature: START_SYNC is NOT supported: " << cpp_strerror(err) << dendl;
  }

  if (has_wait_sync) {
    // async snap creation?
    struct btrfs_ioctl_vol_args_v2 async_args;
    memset(&async_args, 0, sizeof(async_args));
    async_args.fd = srcfd;
    async_args.flags = BTRFS_SUBVOL_CREATE_ASYNC;
    strcpy(async_args.name, "async_snap_test");

    // remove old one, first
    struct stat st;
    strcpy(vol_args.name, async_args.name);
    if (::fstatat(get_basedir_fd(), vol_args.name, &st, 0) == 0) {
      dout(0) << "detect_feature: removing old async_snap_test" << dendl;
      r = ::ioctl(get_basedir_fd(), BTRFS_IOC_SNAP_DESTROY, &vol_args);
      if (r != 0) {
	int err = errno;
	dout(0) << "detect_feature: failed to remove old async_snap_test: " << cpp_strerror(err) << dendl;
      }
    }

    r = ::ioctl(get_basedir_fd(), BTRFS_IOC_SNAP_CREATE_V2, &async_args);
    if (r == 0 || errno == EEXIST) {
      dout(0) << "detect_feature: SNAP_CREATE_V2 is supported" << dendl;
      has_snap_create_v2 = true;

      // clean up
      strcpy(vol_args.name, "async_snap_test");
      r = ::ioctl(get_basedir_fd(), BTRFS_IOC_SNAP_DESTROY, &vol_args);
      if (r != 0) {
	int err = errno;
	dout(0) << "detect_feature: SNAP_DESTROY failed: " << cpp_strerror(err) << dendl;
      }
    } else {
      int err = errno;
      dout(0) << "detect_feature: SNAP_CREATE_V2 is NOT supported: " << cpp_strerror(err) << dendl;
    }
  }

  // clean up test subvol
  if (srcfd >= 0)
    TEMP_FAILURE_RETRY(::close(srcfd));

  strcpy(vol_args.name, "test_subvol");
  r = ::ioctl(get_basedir_fd(), BTRFS_IOC_SNAP_DESTROY, &vol_args);
  if (r < 0) {
    r = -errno;
    dout(0) << "detect_feature: failed to remove " << vol_args.name << ": " << cpp_strerror(r) << dendl;
  }

  if (m_filestore_btrfs_snap && !has_snap_create_v2) {
    dout(0) << "mount WARNING: btrfs snaps enabled, but no SNAP_CREATE_V2 ioctl (from kernel 2.6.37+)" << dendl;
    cerr << TEXT_YELLOW
      << " ** WARNING: 'filestore btrfs snap' is enabled (for safe transactions,\n"
      << "             rollback), but btrfs does not support the SNAP_CREATE_V2 ioctl\n"
      << "             (added in Linux 2.6.37).  Expect slow btrfs sync/commit\n"
      << "             performance.\n"
      << TEXT_NORMAL;
  }

  return 0;
}

bool BtrfsFileStoreBackend::can_checkpoint()
{
  return stable_commits;
}

int BtrfsFileStoreBackend::create_current()
{
  struct stat st;
  int ret = ::stat(get_current_path().c_str(), &st);
  if (ret == 0) {
    // current/ exists
    if (!S_ISDIR(st.st_mode)) {
      dout(0) << "create_current: current/ exists but is not a directory" << dendl;
      return -EINVAL;
    }

    struct stat basest;
    struct statfs currentfs;
    ret = ::fstat(get_basedir_fd(), &basest);
    if (ret < 0) {
      ret = -errno;
      dout(0) << "create_current: cannot fstat basedir " << cpp_strerror(ret) << dendl;
      return ret;
    }
    ret = ::statfs(get_current_path().c_str(), &currentfs);
    if (ret < 0) {
      ret = -errno;
      dout(0) << "create_current: cannot statsf basedir " << cpp_strerror(ret) << dendl;
      return ret;
    }
    if (currentfs.f_type == BTRFS_SUPER_MAGIC && basest.st_dev != st.st_dev) {
      dout(2) << "create_current: current appears to be a btrfs subvolume" << dendl;
      stable_commits = true;
    }
    return 0;
  }

  struct btrfs_ioctl_vol_args volargs;
  memset(&volargs, 0, sizeof(volargs));

  volargs.fd = 0;
  strcpy(volargs.name, "current");
  if (::ioctl(get_basedir_fd(), BTRFS_IOC_SUBVOL_CREATE, (unsigned long int)&volargs) < 0) {
    ret = -errno;
    dout(0) << "create_current: BTRFS_IOC_SUBVOL_CREATE failed with error "
	    << cpp_strerror(ret) << dendl;
    return ret;
  }

  dout(2) << "create_current: created btrfs subvol " << get_current_path() << dendl;
  if (::chmod(get_current_path().c_str(), 0755) < 0) {
    ret = -errno;
    dout(0) << "create_current: failed to chmod " << get_current_path() << " to 0755: "
	    << cpp_strerror(ret) << dendl;
    return ret;
  }

  stable_commits = true;
  return 0;
}

int BtrfsFileStoreBackend::list_checkpoints(list<string>& ls)
{
  int ret, err = 0;

  struct stat basest;
  ret = ::fstat(get_basedir_fd(), &basest);
  if (ret < 0) {
    ret = -errno;
    dout(0) << "list_checkpoints: cannot fstat basedir " << cpp_strerror(ret) << dendl;
    return ret;
  }

  // get snap list
  DIR *dir = ::opendir(get_basedir_path().c_str());
  if (!dir) {
    ret = -errno;
    dout(0) << "list_checkpoints: opendir '" << get_basedir_path() << "' failed: "
	    << cpp_strerror(ret) << dendl;
    return ret;
  }

  list<string> snaps;
  char path[PATH_MAX];
  char buf[offsetof(struct dirent, d_name) + PATH_MAX + 1];
  struct dirent *de;
  while (::readdir_r(dir, (struct dirent *)&buf, &de) == 0) {
    if (!de)
      break;

    snprintf(path, sizeof(path), "%s/%s", get_basedir_path().c_str(), de->d_name);

    struct stat st;
    ret = ::stat(path, &st);
    if (ret < 0) {
      err = -errno;
      dout(0) << "list_checkpoints: stat '" << path << "' failed: "
	      << cpp_strerror(err) << dendl;
      break;
    }

    if (!S_ISDIR(st.st_mode))
      continue;

    struct statfs fs;
    ret = ::statfs(path, &fs);
    if (ret < 0) {
      err = -errno;
      dout(0) << "list_checkpoints: statfs '" << path << "' failed: "
	      << cpp_strerror(err) << dendl;
      break;
    }

    if (fs.f_type == BTRFS_SUPER_MAGIC && basest.st_dev != st.st_dev)
      snaps.push_back(string(de->d_name));
  }

  if (::closedir(dir) < 0) {
      ret = -errno;
      dout(0) << "list_checkpoints: closedir failed: " << cpp_strerror(ret) << dendl;
      if (!err)
	err = ret;
  }

  if (err)
    return err;

  ls.swap(snaps);
  return 0;
}

int BtrfsFileStoreBackend::create_checkpoint(const string& name, uint64_t *transid)
{
  dout(10) << "create_checkpoint: '" << name << "'" << dendl;
  if (has_snap_create_v2 && transid) {
    struct btrfs_ioctl_vol_args_v2 async_args;
    memset(&async_args, 0, sizeof(async_args));
    async_args.fd = get_current_fd();
    async_args.flags = BTRFS_SUBVOL_CREATE_ASYNC;

    size_t name_size = sizeof(async_args.name);
    strncpy(async_args.name, name.c_str(), name_size);
    async_args.name[name_size-1] = '\0';

    int r = ::ioctl(get_basedir_fd(), BTRFS_IOC_SNAP_CREATE_V2, &async_args);
    if (r < 0) {
      r = -errno;
      dout(0) << "create_checkpoint: async snap create '" << name << "' got " << cpp_strerror(r) << dendl;
      return r;
    }
    dout(20) << "create_checkpoint: async snap create '" << name << "' transid " << async_args.transid << dendl;
    *transid = async_args.transid;
  } else {
    struct btrfs_ioctl_vol_args vol_args;
    memset(&vol_args, 0, sizeof(vol_args));
    vol_args.fd = get_current_fd();

    size_t name_size = sizeof(vol_args.name);
    strncpy(vol_args.name, name.c_str(), name_size);
    vol_args.name[name_size-1] = '\0';

    int r = ::ioctl(get_basedir_fd(), BTRFS_IOC_SNAP_CREATE, &vol_args);
    if (r < 0) {
      r = -errno;
      dout(0) << "create_checkpoint: snap create '" << name << "' got " << cpp_strerror(r) << dendl;
      return r;
    }
    if (transid)
      *transid = 0;
  }
  return 0;
}

int BtrfsFileStoreBackend::sync_checkpoint(uint64_t transid)
{
  // wait for commit
  dout(10) << "sync_checkpoint: transid " << transid << " to complete" << dendl;
  int ret = ::ioctl(get_op_fd(), BTRFS_IOC_WAIT_SYNC, &transid);
  if (ret < 0) {
    ret = -errno;
    dout(0) << "sync_checkpoint: ioctl WAIT_SYNC got " << cpp_strerror(ret) << dendl;
    return -errno;
  }
  dout(20) << "sync_checkpoint: done waiting for transid " << transid << dendl;
  return 0;
}

int BtrfsFileStoreBackend::rollback_to(const string& name)
{
  dout(10) << "rollback_to: to '" << name << "'" << dendl;
  char s[PATH_MAX];
  btrfs_ioctl_vol_args vol_args;

  memset(&vol_args, 0, sizeof(vol_args));
  vol_args.fd = 0;
  strcpy(vol_args.name, "current");

  int ret = ::ioctl(get_basedir_fd(), BTRFS_IOC_SNAP_DESTROY, &vol_args);
  if (ret && errno != ENOENT) {
    dout(0) << "rollback_to: error removing old current subvol: " << cpp_strerror(ret) << dendl;
    snprintf(s, sizeof(s), "%s/current.remove.me.%d", get_basedir_path().c_str(), rand());
    if (::rename(get_current_path().c_str(), s)) {
      ret = -errno;
      dout(0) << "rollback_to: error renaming old current subvol: "
	      << cpp_strerror(ret) << dendl;
      return ret;
    }
  }

  snprintf(s, sizeof(s), "%s/%s", get_basedir_path().c_str(), name.c_str());

  // roll back
  vol_args.fd = ::open(s, O_RDONLY);
  if (vol_args.fd < 0) {
    ret = -errno;
    dout(0) << "rollback_to: error opening '" << s << "': " << cpp_strerror(ret) << dendl;
    return ret;
  }
  ret = ::ioctl(get_basedir_fd(), BTRFS_IOC_SNAP_CREATE, &vol_args);
  if (ret < 0 ) {
    ret = -errno;
    dout(0) << "rollback_to: ioctl SNAP_CREATE got " << cpp_strerror(ret) << dendl;
  }
  TEMP_FAILURE_RETRY(::close(vol_args.fd));
  return ret;
}

int BtrfsFileStoreBackend::destroy_checkpoint(const string& name)
{
  dout(10) << "destroy_checkpoint: '" << name << "'" << dendl;
  btrfs_ioctl_vol_args vol_args;
  memset(&vol_args, 0, sizeof(vol_args));
  vol_args.fd = 0;
  strncpy(vol_args.name, name.c_str(), sizeof(vol_args.name));

  int ret = ::ioctl(get_basedir_fd(), BTRFS_IOC_SNAP_DESTROY, &vol_args);
  if (ret) {
    ret = -errno;
    dout(0) << "destroy_checkpoint: ioctl SNAP_DESTROY got " << cpp_strerror(ret) << dendl;
    return ret;
  }
  return 0;
}

int BtrfsFileStoreBackend::syncfs()
{
  dout(15) << "syncfs" << dendl;
  // do a full btrfs commit
  int ret = ::ioctl(get_op_fd(), BTRFS_IOC_SYNC);
  if (ret < 0) {
    ret = -errno;
    dout(0) << "syncfs: btrfs IOC_SYNC got " << cpp_strerror(ret) << dendl;
  }
  return ret;
}

int BtrfsFileStoreBackend::clone_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff)
{
  dout(20) << "clone_range: " << srcoff << "~" << len << " to " << dstoff << dendl;
  size_t blk_size = get_blksize();
  if (!has_clone_range ||
      srcoff % blk_size != dstoff % blk_size) {
    dout(20) << "clone_range: using copy" << dendl;
    return _copy_range(from, to, srcoff, len, dstoff);
  }

  int err = 0;
  int r = 0;

  uint64_t srcoffclone = ALIGN_UP(srcoff, blk_size);
  uint64_t dstoffclone = ALIGN_UP(dstoff, blk_size);
  if (srcoffclone >= srcoff + len) {
    dout(20) << "clone_range: using copy, extent too short to align srcoff" << dendl;
    return _copy_range(from, to, srcoff, len, dstoff);
  }

  uint64_t lenclone = len - (srcoffclone - srcoff);
  if (!ALIGNED(lenclone, blk_size)) {
    struct stat from_stat, to_stat;
    err = ::fstat(from, &from_stat);
    if (err) return -errno;
    err = ::fstat(to , &to_stat);
    if (err) return -errno;

    if (srcoff + len != (uint64_t)from_stat.st_size ||
	dstoff + len < (uint64_t)to_stat.st_size) {
      // Not to the end of the file, need to align length as well
      lenclone = ALIGN_DOWN(lenclone, blk_size);
    }
  }
  if (lenclone == 0) {
    // too short
    return _copy_range(from, to, srcoff, len, dstoff);
  }

  dout(20) << "clone_range: cloning " << srcoffclone << "~" << lenclone
	   << " to " << dstoffclone << " = " << r << dendl;
  btrfs_ioctl_clone_range_args a;
  a.src_fd = from;
  a.src_offset = srcoffclone;
  a.src_length = lenclone;
  a.dest_offset = dstoffclone;
  err = ::ioctl(to, BTRFS_IOC_CLONE_RANGE, &a);
  if (err >= 0) {
    r += err;
  } else if (errno == EINVAL) {
    // Still failed, might be compressed
    dout(20) << "clone_range: failed CLONE_RANGE call with -EINVAL, using copy" << dendl;
    return _copy_range(from, to, srcoff, len, dstoff);
  } else {
    return -errno;
  }

  // Take care any trimmed from front
  if (srcoffclone != srcoff) {
    err = _copy_range(from, to, srcoff, srcoffclone - srcoff, dstoff);
    if (err >= 0) {
      r += err;
    } else {
      return err;
    }
  }

  // Copy end
  if (srcoffclone + lenclone != srcoff + len) {
    err = _copy_range(from, to,
			 srcoffclone + lenclone,
			 (srcoff + len) - (srcoffclone + lenclone),
			 dstoffclone + lenclone);
    if (err >= 0) {
      r += err;
    } else {
      return err;
    }
  }
  dout(20) << "clone_range: finished " << srcoff << "~" << len
	   << " to " << dstoff << " = " << r << dendl;
  return r;
}
#endif
