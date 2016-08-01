// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Inktank, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "XfsFileStoreBackend.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/utsname.h>

#include <xfs/xfs.h>

#include "common/errno.h"
#include "common/linux_version.h"
#include "include/assert.h"
#include "include/compat.h"

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "xfsfilestorebackend(" << get_basedir_path() << ") "

XfsFileStoreBackend::XfsFileStoreBackend(FileStore *fs):
  GenericFileStoreBackend(fs), m_has_extsize(false) { }

/*
 * Set extsize attr on a file to val.  Should be a free-standing
 * function, but dout_prefix expanding to a call to get_basedir_path()
 * protected member function won't let it.
 */
int XfsFileStoreBackend::set_extsize(int fd, unsigned int val)
{
  struct fsxattr fsx;
  struct stat sb;
  int ret;

  if (fstat(fd, &sb) < 0) {
    ret = -errno;
    dout(0) << "set_extsize: fstat: " << cpp_strerror(ret) << dendl;
    return ret;
  }
  if (!S_ISREG(sb.st_mode)) {
    dout(0) << "set_extsize: invalid target file type" << dendl;
    return -EINVAL;
  }

  if (ioctl(fd, XFS_IOC_FSGETXATTR, &fsx) < 0) {
    ret = -errno;
    dout(0) << "set_extsize: FSGETXATTR: " << cpp_strerror(ret) << dendl;
    return ret;
  }

  // already set?
  if ((fsx.fsx_xflags & XFS_XFLAG_EXTSIZE) && fsx.fsx_extsize == val)
    return 0;

  // xfs won't change extent size if any extents are allocated
  if (fsx.fsx_nextents != 0)
    return 0;

  fsx.fsx_xflags |= XFS_XFLAG_EXTSIZE;
  fsx.fsx_extsize = val;

  if (ioctl(fd, XFS_IOC_FSSETXATTR, &fsx) < 0) {
    ret = -errno;
    dout(0) << "set_extsize: FSSETXATTR: " << cpp_strerror(ret) << dendl;
    return ret;
  }

  return 0;
}

int XfsFileStoreBackend::detect_features()
{
  int ret;

  ret = GenericFileStoreBackend::detect_features();
  if (ret < 0)
    return ret;

  // extsize?
  int fd = ::openat(get_basedir_fd(), "extsize_test", O_CREAT|O_WRONLY, 0600);
  if (fd < 0) {
    ret = -errno;
    dout(0) << "detect_feature: failed to create test file for extsize attr: "
            << cpp_strerror(ret) << dendl;
    goto out;
  }
  if (::unlinkat(get_basedir_fd(), "extsize_test", 0) < 0) {
    ret = -errno;
    dout(0) << "detect_feature: failed to unlink test file for extsize attr: "
            << cpp_strerror(ret) << dendl;
    goto out_close;
  }

  if (g_conf->filestore_xfs_extsize) {
    ret = set_extsize(fd, 1U << 15); // a few pages
    if (ret) {
      ret = 0;
      dout(0) << "detect_feature: failed to set test file extsize, assuming extsize is NOT supported" << dendl;
      goto out_close;
    }

    // make sure we have 3.5 or newer, which includes this fix
    //   aff3a9edb7080f69f07fe76a8bd089b3dfa4cb5d
    // for this set_extsize bug
    //   http://oss.sgi.com/bugzilla/show_bug.cgi?id=874
    int ver = get_linux_version();
    if (ver == 0) {
      dout(0) << __func__ << ": couldn't verify extsize not buggy, disabling extsize" << dendl;
      m_has_extsize = false;
    } else if (ver < KERNEL_VERSION(3, 5, 0)) {
      dout(0) << __func__ << ": disabling extsize, your kernel < 3.5 and has buggy extsize ioctl" << dendl;
      m_has_extsize = false;
    } else {
      dout(0) << __func__ << ": extsize is supported and your kernel >= 3.5" << dendl;
      m_has_extsize = true;
    }
  } else {
    dout(0) << "detect_feature: extsize is disabled by conf" << dendl;
  }

out_close:
  TEMP_FAILURE_RETRY(::close(fd));
out:
  return ret;
}

int XfsFileStoreBackend::set_alloc_hint(int fd, uint64_t hint)
{
  if (!m_has_extsize)
    return -EOPNOTSUPP;

  assert(hint < UINT_MAX);
  return set_extsize(fd, hint);
}
