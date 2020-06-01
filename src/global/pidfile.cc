// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/debug.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "global/pidfile.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#if defined(__FreeBSD__)
#include <sys/param.h>
#endif

#include "include/compat.h"

using std::string;

//
// derr can be used for functions exclusively called from pidfile_write
//
// cerr must be used for functions called by pidfile_remove because
// logging is not functional when it is called. cerr output is lost
// when the caller is daemonized but it will show if not (-f)
//
#define dout_context g_ceph_context
#define dout_prefix *_dout
#define dout_subsys ceph_subsys_

struct pidfh {
  int pf_fd;
  string pf_path;
  dev_t pf_dev;
  ino_t pf_ino;

  pidfh() {
    reset();
  }
  ~pidfh() {
    remove();
  }

  bool is_open() const {
    return !pf_path.empty() && pf_fd != -1;
  }
  void reset() {
    pf_fd = -1;
    pf_path.clear();
    pf_dev = 0;
    pf_ino = 0;
  }
  int verify();
  int remove();
  int open(std::string_view pid_file);
  int write();
};

static pidfh *pfh = nullptr;

int pidfh::verify() {
  // check that the file we opened still is the same
  if (pf_fd == -1)
    return -EINVAL;
  struct stat st;
  if (stat(pf_path.c_str(), &st) == -1)
    return -errno;
  if (st.st_dev != pf_dev || st.st_ino != pf_ino)
    return -ESTALE;
  return 0;
}

int pidfh::remove()
{
  if (pf_path.empty())
    return 0;

  int ret;
  if ((ret = verify()) < 0) {
    if (pf_fd != -1) {
      ::close(pf_fd);
      reset();
    }
    return ret;
  }

  // seek to the beginning of the file before reading
  ret = ::lseek(pf_fd, 0, SEEK_SET);
  if (ret < 0) {
    std::cerr << __func__ << " lseek failed "
	      << cpp_strerror(errno) << std::endl;
    return -errno;
  }

  // check that the pid file still has our pid in it
  char buf[32];
  memset(buf, 0, sizeof(buf));
  ssize_t res = safe_read(pf_fd, buf, sizeof(buf));
  ::close(pf_fd);
  if (res < 0) {
    std::cerr << __func__ << " safe_read failed "
	      << cpp_strerror(-res) << std::endl;
    return res;
  }

  int a = atoi(buf);
  if (a != getpid()) {
    std::cerr << __func__ << " the pid found in the file is "
	      << a << " which is different from getpid() "
	      << getpid() << std::endl;
    return -EDOM;
  }
  ret = ::unlink(pf_path.c_str());
  if (ret < 0) {
    std::cerr << __func__ << " unlink " << pf_path.c_str() << " failed "
	      << cpp_strerror(errno) << std::endl;
    return -errno;
  }
  reset();
  return 0;
}

int pidfh::open(std::string_view pid_file)
{
  pf_path = pid_file;

  int fd;
  fd = ::open(pf_path.c_str(), O_CREAT|O_RDWR|O_CLOEXEC, 0644);
  if (fd < 0) {
    int err = errno;
    derr << __func__ << ": failed to open pid file '"
	 << pf_path << "': " << cpp_strerror(err) << dendl;
    reset();
    return -err;
  }
  struct stat st;
  if (fstat(fd, &st) == -1) {
    int err = errno;
    derr << __func__ << ": failed to fstat pid file '"
	 << pf_path << "': " << cpp_strerror(err) << dendl;
    ::close(fd);
    reset();
    return -err;
  }

  pf_fd = fd;
  pf_dev = st.st_dev;
  pf_ino = st.st_ino;

  struct flock l = {
    .l_type = F_WRLCK,
    .l_whence = SEEK_SET,
    .l_start = 0,
    .l_len = 0
  };
  int r = ::fcntl(pf_fd, F_SETLK, &l);
  if (r < 0) {
    if (errno == EAGAIN || errno == EACCES) {
      derr << __func__ << ": failed to lock pidfile "
	   << pf_path << " because another process locked it" 
	   << "': " << cpp_strerror(errno) << dendl;
    } else {
      derr << __func__ << ": failed to lock pidfile "
	   << pf_path << "': " << cpp_strerror(errno) << dendl;
    }
    const auto lock_errno = errno;
    ::close(pf_fd);
    reset();
    return -lock_errno;
  }
  return 0;
}

int pidfh::write()
{
  if (!is_open())
    return 0;

  char buf[32];
  int len = snprintf(buf, sizeof(buf), "%d\n", getpid());
  if (::ftruncate(pf_fd, 0) < 0) {
    int err = errno;
    derr << __func__ << ": failed to ftruncate the pid file '"
	 << pf_path << "': " << cpp_strerror(err) << dendl;
    return -err;
  }
  ssize_t res = safe_write(pf_fd, buf, len);
  if (res < 0) {
    derr << __func__ << ": failed to write to pid file '"
	 << pf_path << "': " << cpp_strerror(-res) << dendl;
    return res;
  }
  return 0;
}

void pidfile_remove()
{
  if (pfh != nullptr)
    delete pfh;
  pfh = nullptr;
}

int pidfile_write(std::string_view pid_file)
{
  if (pid_file.empty()) {
    dout(0) << __func__ << ": ignore empty --pid-file" << dendl;
    return 0;
  }

  ceph_assert(pfh == nullptr);

  pfh = new pidfh();
  if (atexit(pidfile_remove)) {
    derr << __func__ << ": failed to set pidfile_remove function "
	 << "to run at exit." << dendl;
    return -EINVAL;
  }

  int r = pfh->open(pid_file);
  if (r != 0) {
    pidfile_remove();
    return r;
  }

  r = pfh->write();
  if (r != 0) {
    pidfile_remove();
    return r;
  }

  return 0;
}
