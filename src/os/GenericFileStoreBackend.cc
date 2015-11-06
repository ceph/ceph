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

#if defined(__linux__)
#include <linux/fs.h>
#endif

#include "include/compat.h"
#include "include/linux_fiemap.h"

#include <iostream>
#include <fstream>
#include <sstream>

#include "GenericFileStoreBackend.h"

#include "common/errno.h"
#include "common/config.h"
#include "common/sync_filesystem.h"

#include "common/SloppyCRCMap.h"
#include "os/chain_xattr.h"

#define SLOPPY_CRC_XATTR "user.cephos.scrc"


#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "genericfilestorebackend(" << get_basedir_path() << ") "

#define ALIGN_DOWN(x, by) ((x) - ((x) % (by)))
#define ALIGNED(x, by) (!((x) % (by)))
#define ALIGN_UP(x, by) (ALIGNED((x), (by)) ? (x) : (ALIGN_DOWN((x), (by)) + (by)))

GenericFileStoreBackend::GenericFileStoreBackend(FileStore *fs):
  FileStoreBackend(fs),
  ioctl_fiemap(false),
  seek_data_hole(false),
  m_filestore_fiemap(g_conf->filestore_fiemap),
  m_filestore_seek_data_hole(g_conf->filestore_seek_data_hole),
  m_filestore_fsync_flushes_journal_data(g_conf->filestore_fsync_flushes_journal_data),
  m_filestore_splice(false) {}

int GenericFileStoreBackend::detect_features()
{
  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/fiemap_test", get_basedir_path().c_str());

  int fd = ::open(fn, O_CREAT|O_RDWR|O_TRUNC, 0644);
  if (fd < 0) {
    fd = -errno;
    derr << "detect_features: unable to create " << fn << ": " << cpp_strerror(fd) << dendl;
    return fd;
  }

  // ext4 has a bug in older kernels where fiemap will return an empty
  // result in some cases.  this is a file layout that triggers the bug
  // on 2.6.34-rc5.
  int v[] = {
    0x0000000000016000, 0x0000000000007000,
    0x000000000004a000, 0x0000000000007000,
    0x0000000000060000, 0x0000000000001000,
    0x0000000000061000, 0x0000000000008000,
    0x0000000000069000, 0x0000000000007000,
    0x00000000000a3000, 0x000000000000c000,
    0x000000000024e000, 0x000000000000c000,
    0x000000000028b000, 0x0000000000009000,
    0x00000000002b1000, 0x0000000000003000,
    0, 0
  };
  for (int i=0; v[i]; i++) {
    int off = v[i++];
    int len = v[i];

    // write a large extent
    char buf[len];
    memset(buf, 1, sizeof(buf));
    int r = ::lseek(fd, off, SEEK_SET);
    if (r < 0) {
      r = -errno;
      derr << "detect_features: failed to lseek " << fn << ": " << cpp_strerror(r) << dendl;
      VOID_TEMP_FAILURE_RETRY(::close(fd));
      return r;
    }
    r = write(fd, buf, sizeof(buf));
    if (r < 0) {
      derr << "detect_features: failed to write to " << fn << ": " << cpp_strerror(r) << dendl;
      VOID_TEMP_FAILURE_RETRY(::close(fd));
      return r;
    }
  }

  // fiemap an extent inside that
  if (!m_filestore_fiemap) {
    dout(0) << "detect_features: FIEMAP ioctl is disabled via 'filestore fiemap' config option" << dendl;
    ioctl_fiemap = false;
  } else {
    struct fiemap *fiemap;
    int r = do_fiemap(fd, 2430421, 59284, &fiemap);
    if (r < 0) {
      dout(0) << "detect_features: FIEMAP ioctl is NOT supported" << dendl;
      ioctl_fiemap = false;
    } else {
      if (fiemap->fm_mapped_extents == 0) {
        dout(0) << "detect_features: FIEMAP ioctl is supported, but buggy -- upgrade your kernel" << dendl;
        ioctl_fiemap = false;
      } else {
        dout(0) << "detect_features: FIEMAP ioctl is supported and appears to work" << dendl;
        ioctl_fiemap = true;
      }
      free(fiemap);
    }
  }

  // SEEK_DATA/SEEK_HOLE detection
  if (!m_filestore_seek_data_hole) {
    dout(0) << "detect_features: SEEK_DATA/SEEK_HOLE is disabled via 'filestore seek data hole' config option" << dendl;
    seek_data_hole = false;
  } else {
#if defined(__linux__) && defined(SEEK_HOLE) && defined(SEEK_DATA)
    // If compiled on an OS with SEEK_HOLE/SEEK_DATA support, but running
    // on an OS that doesn't support SEEK_HOLE/SEEK_DATA, EINVAL is returned.
    // Fall back to use fiemap.
    off_t hole_pos;

    hole_pos = lseek(fd, 0, SEEK_HOLE);
    if (hole_pos < 0) {
      if (errno == EINVAL) {
        dout(0) << "detect_features: lseek SEEK_DATA/SEEK_HOLE is NOT supported" << dendl;
        seek_data_hole = false;
      } else {
        derr << "detect_features: failed to lseek " << fn << ": " << cpp_strerror(-errno) << dendl;
        VOID_TEMP_FAILURE_RETRY(::close(fd));
        return -errno;
      }
    } else {
      dout(0) << "detect_features: lseek SEEK_DATA/SEEK_HOLE is supported" << dendl;
      seek_data_hole = true;
    }
#endif
  }

  //splice detection
#ifdef CEPH_HAVE_SPLICE
  if (!m_filestore_splice) {
    int pipefd[2];
    loff_t off_in = 0;
    int r;
    if ((r = pipe(pipefd)) < 0)
      dout(0) << "detect_features: splice  pipe met error " << cpp_strerror(errno) << dendl;
    else {
      lseek(fd, 0, SEEK_SET);
      r = splice(fd, &off_in, pipefd[1], NULL, 10, 0);
      if (!(r < 0 && errno == EINVAL)) {
	m_filestore_splice = true;
	dout(0) << "detect_features: splice is supported" << dendl;
      } else
	dout(0) << "detect_features: splice is NOT supported" << dendl;
      close(pipefd[0]);
      close(pipefd[1]);
    }
  }
#endif
  ::unlink(fn);
  VOID_TEMP_FAILURE_RETRY(::close(fd));


  bool have_syncfs = false;
#ifdef HAVE_SYS_SYNCFS
  if (::syncfs(get_basedir_fd()) == 0) {
    dout(0) << "detect_features: syncfs(2) syscall fully supported (by glibc and kernel)" << dendl;
    have_syncfs = true;
  } else {
    dout(0) << "detect_features: syncfs(2) syscall supported by glibc BUT NOT the kernel" << dendl;
  }
#elif defined(SYS_syncfs)
  if (syscall(SYS_syncfs, get_basedir_fd()) == 0) {
    dout(0) << "detect_features: syscall(SYS_syncfs, fd) fully supported" << dendl;
    have_syncfs = true;
  } else {
    dout(0) << "detect_features: syscall(SYS_syncfs, fd) supported by libc BUT NOT the kernel" << dendl;
  }
#elif defined(__NR_syncfs)
  if (syscall(__NR_syncfs, get_basedir_fd()) == 0) {
    dout(0) << "detect_features: syscall(__NR_syncfs, fd) fully supported" << dendl;
    have_syncfs = true;
  } else {
    dout(0) << "detect_features: syscall(__NR_syncfs, fd) supported by libc BUT NOT the kernel" << dendl;
  }
#endif
  if (!have_syncfs) {
    dout(0) << "detect_features: syncfs(2) syscall not supported" << dendl;
    if (m_filestore_fsync_flushes_journal_data) {
      dout(0) << "detect_features: no syncfs(2), but 'filestore fsync flushes journal data = true', so fsync will suffice." << dendl;
    } else {
      dout(0) << "detect_features: no syncfs(2), must use sync(2)." << dendl;
      dout(0) << "detect_features: WARNING: multiple ceph-osd daemons on the same host will be slow" << dendl;
    }
  }

  return 0;
}

int GenericFileStoreBackend::create_current()
{
  struct stat st;
  int ret = ::stat(get_current_path().c_str(), &st);
  if (ret == 0) {
    // current/ exists
    if (!S_ISDIR(st.st_mode)) {
      dout(0) << "_create_current: current/ exists but is not a directory" << dendl;
      ret = -EINVAL;
    }
  } else {
    ret = ::mkdir(get_current_path().c_str(), 0755);
    if (ret < 0) {
      ret = -errno;
      dout(0) << "_create_current: mkdir " << get_current_path() << " failed: "<< cpp_strerror(ret) << dendl;
    }
  }
  return ret;
}

int GenericFileStoreBackend::syncfs()
{
  int ret;
  if (m_filestore_fsync_flushes_journal_data) {
    dout(15) << "syncfs: doing fsync on " << get_op_fd() << dendl;
    // make the file system's journal commit.
    //  this works with ext3, but NOT ext4
    ret = ::fsync(get_op_fd());
    if (ret < 0)
      ret = -errno;
  } else {
    dout(15) << "syncfs: doing a full sync (syncfs(2) if possible)" << dendl;
    ret = sync_filesystem(get_current_fd());
  }
  return ret;
}

int GenericFileStoreBackend::do_fiemap(int fd, off_t start, size_t len, struct fiemap **pfiemap)
{
  struct fiemap *fiemap = NULL;
  struct fiemap *_realloc_fiemap = NULL;
  int size;
  int ret;

  fiemap = (struct fiemap*)calloc(sizeof(struct fiemap), 1);
  if (!fiemap)
    return -ENOMEM;
  /*
   * There is a bug on xfs about fiemap. Suppose(offset=3990, len=4096),
   * the result is (logical=4096, len=4096). It leak the [3990, 4096).
   * Commit:"xfs: fix rounding error of fiemap length parameter
   * (eedf32bfcace7d8e20cc66757d74fc68f3439ff7)" fix this bug.
   * Here, we make offset aligned with CEPH_PAGE_SIZE to avoid this bug.
   */
  fiemap->fm_start = start - start % CEPH_PAGE_SIZE;
  fiemap->fm_length = len + start % CEPH_PAGE_SIZE;
  fiemap->fm_flags = FIEMAP_FLAG_SYNC; /* flush extents to disk if needed */

#if defined(DARWIN) || defined(__FreeBSD__)
  ret = -ENOTSUP;
  goto done_err;
#else
  if (ioctl(fd, FS_IOC_FIEMAP, fiemap) < 0) {
    ret = -errno;
    goto done_err;
  }
#endif
  size = sizeof(struct fiemap_extent) * (fiemap->fm_mapped_extents);

  _realloc_fiemap = (struct fiemap *)realloc(fiemap, sizeof(struct fiemap) + size);
  if (!_realloc_fiemap) {
    ret = -ENOMEM;
    goto done_err;
  } else {
    fiemap = _realloc_fiemap;
  }

  memset(fiemap->fm_extents, 0, size);

  fiemap->fm_extent_count = fiemap->fm_mapped_extents;
  fiemap->fm_mapped_extents = 0;

#if defined(DARWIN) || defined(__FreeBSD__)
  ret = -ENOTSUP;
  goto done_err;
#else
  if (ioctl(fd, FS_IOC_FIEMAP, fiemap) < 0) {
    ret = -errno;
    goto done_err;
  }
  *pfiemap = fiemap;
#endif
  return 0;

done_err:
  *pfiemap = NULL;
  free(fiemap);
  return ret;
}


int GenericFileStoreBackend::_crc_load_or_init(int fd, SloppyCRCMap *cm)
{
  char buf[100];
  bufferptr bp;
  int r = 0;
  int l = chain_fgetxattr(fd, SLOPPY_CRC_XATTR, buf, sizeof(buf));
  if (l == -ENODATA) {
    return 0;
  }
  if (l >= 0) {
    bp = buffer::create(l);
    memcpy(bp.c_str(), buf, l);
  } else if (l == -ERANGE) {
    l = chain_fgetxattr(fd, SLOPPY_CRC_XATTR, 0, 0);
    if (l > 0) {
      bp = buffer::create(l);
      l = chain_fgetxattr(fd, SLOPPY_CRC_XATTR, bp.c_str(), l);
    }
  }
  bufferlist bl;
  bl.append(bp);
  bufferlist::iterator p = bl.begin();
  try {
    ::decode(*cm, p);
  }
  catch (buffer::error &e) {
    r = -EIO;
  }
  if (r < 0)
    derr << __func__ << " got " << cpp_strerror(r) << dendl;
  return r;
}

int GenericFileStoreBackend::_crc_save(int fd, SloppyCRCMap *cm)
{
  bufferlist bl;
  ::encode(*cm, bl);
  int r = chain_fsetxattr(fd, SLOPPY_CRC_XATTR, bl.c_str(), bl.length());
  if (r < 0)
    derr << __func__ << " got " << cpp_strerror(r) << dendl;
  return r;
}

int GenericFileStoreBackend::_crc_update_write(int fd, loff_t off, size_t len, const bufferlist& bl)
{
  SloppyCRCMap scm(get_crc_block_size());
  int r = _crc_load_or_init(fd, &scm);
  if (r < 0)
    return r;
  ostringstream ss;
  scm.write(off, len, bl, &ss);
  dout(30) << __func__ << "\n" << ss.str() << dendl;
  r = _crc_save(fd, &scm);
  return r;
}

int GenericFileStoreBackend::_crc_update_truncate(int fd, loff_t off)
{
  SloppyCRCMap scm(get_crc_block_size());
  int r = _crc_load_or_init(fd, &scm);
  if (r < 0)
    return r;
  scm.truncate(off);
  r = _crc_save(fd, &scm);
  return r;
}

int GenericFileStoreBackend::_crc_update_zero(int fd, loff_t off, size_t len)
{
  SloppyCRCMap scm(get_crc_block_size());
  int r = _crc_load_or_init(fd, &scm);
  if (r < 0)
    return r;
  scm.zero(off, len);
  r = _crc_save(fd, &scm);
  return r;
}

int GenericFileStoreBackend::_crc_update_clone_range(int srcfd, int destfd,
						     loff_t srcoff, size_t len, loff_t dstoff)
{
  SloppyCRCMap scm_src(get_crc_block_size());
  SloppyCRCMap scm_dst(get_crc_block_size());
  int r = _crc_load_or_init(srcfd, &scm_src);
  if (r < 0)
    return r;
  r = _crc_load_or_init(destfd, &scm_dst);
  if (r < 0)
    return r;
  ostringstream ss;
  scm_dst.clone_range(srcoff, len, dstoff, scm_src, &ss);
  dout(30) << __func__ << "\n" << ss.str() << dendl;
  r = _crc_save(destfd, &scm_dst);
  return r;
}

int GenericFileStoreBackend::_crc_verify_read(int fd, loff_t off, size_t len, const bufferlist& bl,
					      ostream *out)
{
  SloppyCRCMap scm(get_crc_block_size());
  int r = _crc_load_or_init(fd, &scm);
  if (r < 0)
    return r;
  return scm.read(off, len, bl, out);
}
