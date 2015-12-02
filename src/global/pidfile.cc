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

#define dout_prefix *_dout

struct pidfh {
  int pf_fd;
  char pf_path[PATH_MAX + 1];
  dev_t pf_dev;
  ino_t pf_ino;
 
  pidfh() : pf_fd(-1), pf_dev(0), pf_ino(0) {
    memset(pf_path, 0, sizeof(pf_path));
  }
  
  void close() {
    pf_fd = -1;
    pf_path[0] = '\0';
    pf_dev = 0;
    pf_ino = 0;
  }
};
static pidfh pfh;

static int pidfile_verify() {
  struct stat st;

  if (pfh.pf_fd == -1) {
    return -EINVAL;
  }
  /*
   * Check remembered descriptor
   */ 
  if (fstat(pfh.pf_fd, &st) == -1) {
    return -errno;
  }

  if (st.st_dev != pfh.pf_dev || st.st_ino != pfh.pf_ino) {
    return -ESTALE;
  }
  return 0;
}

int pidfile_write() 
{
  if (!pfh.pf_path[0]) {
    return 0;
  }

  int ret;
  if ((ret = pidfile_verify()) < 0) {
    return ret;
  }
  
  char buf[32];
  int len = snprintf(buf, sizeof(buf), "%d\n", getpid());
  ret = safe_write(pfh.pf_fd, buf, len);
  if (ret < 0) {
    derr << "write_pid_file: failed to write to pid file '"
	 << pfh.pf_path << "': " << cpp_strerror(ret) << dendl;
    pfh.close(); 
    return ret;
  }

  return 0;
}

int pidfile_remove()
{
  if (!pfh.pf_path[0]) {
    return 0;
  }

  int ret;
  if ((ret = pidfile_verify()) < 0) {
    if (pfh.pf_fd != -1) {
      ::close(pfh.pf_fd);
    }
    return ret;
  }

  // only remove it if it has OUR pid in it!
  char buf[32];
  memset(buf, 0, sizeof(buf));
  ssize_t res = safe_read(pfh.pf_fd, buf, sizeof(buf));
  if (pfh.pf_fd != -1) {
    ::close(pfh.pf_fd);
  }

  if (res < 0) {
    return res;
  }
  
  int a = atoi(buf);
  if (a != getpid()) {
    return -EDOM;
  }
  res = ::unlink(pfh.pf_path);
  if (res) {
    return res;
  }
  pfh.close();
  return 0;
}

int pidfile_open(const md_config_t *conf) 
{
  if (conf->pid_file.empty()) {
    return 0;
  }
  int len = snprintf(pfh.pf_path, sizeof(pfh.pf_path),
                    "%s", conf->pid_file.c_str());

  if (len >= (int)sizeof(pfh.pf_path)) { 
    return -ENAMETOOLONG;
  }

  int fd;
  fd = ::open(pfh.pf_path, O_CREAT|O_WRONLY, 0644);
  if (fd < 0) {
    int err = errno;
    derr << "write_pid_file: failed to open pid file '"
         << pfh.pf_path << "': " << cpp_strerror(err) << dendl;
    pfh.close();
    return -errno;
  }
  struct stat st;   
  if (fstat(fd, &st) == -1) {
    close(fd);
    pfh.close();  
    return -errno;
  }
  pfh.pf_fd = fd;
  pfh.pf_dev = st.st_dev;
  pfh.pf_ino = st.st_ino;
  
  struct flock l = { F_WRLCK, SEEK_SET, 0, 0, 0 };
  int r = ::fcntl(pfh.pf_fd, F_SETLK, &l);
  if (r < 0) {
    derr << "failed to lock pidfile - " << pfh.pf_path << ". is there another process in use?" << dendl;
    close(pfh.pf_fd);
    pfh.close();
    return -errno;
  }
  return 0;
}
