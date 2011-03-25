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

#include "common/errno.h"
#include "common/pidfile.h"
#include "common/safe_io.h"
#include "debug.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define dout_prefix *_dout

static char pid_file[PATH_MAX] = "";

int pidfile_write(const md_config_t *conf)
{
  int ret, fd;

  const char *pfile = conf->pid_file;
  if ((pfile == NULL) || (!pfile[0])) {
    return pidfile_remove();
  }
  strncpy(pid_file, pfile, PATH_MAX);

  fd = TEMP_FAILURE_RETRY(::open(pid_file,
				 O_CREAT|O_TRUNC|O_WRONLY, 0644));
  if (fd < 0) {
    int err = errno;
    derr << "write_pid_file: failed to open pid file '"
	 << pid_file << "': " << cpp_strerror(err) << dendl;
    return err;
  }

  char buf[20];
  int len = snprintf(buf, sizeof(buf), "%d\n", getpid());
  ret = safe_write(fd, buf, len);
  if (ret < 0) {
    derr << "write_pid_file: failed to write to pid file '"
	 << pid_file << "': " << cpp_strerror(ret) << dendl;
    TEMP_FAILURE_RETRY(::close(fd));
    return ret;
  }
  if (TEMP_FAILURE_RETRY(::close(fd))) {
    ret = errno;
    derr << "SimpleMessenger::write_pid_file: failed to close to pid file '"
	 << pid_file << "': " << cpp_strerror(ret) << dendl;
    return -ret;
  }

  return 0;
}

int pidfile_remove(void)
{
  if (!pid_file[0])
    return 0;

  // only remove it if it has OUR pid in it!
  int fd = TEMP_FAILURE_RETRY(::open(pid_file, O_RDONLY));
  if (fd < 0)
    return -errno;
  char buf[32];
  memset(buf, 0, sizeof(buf));
  ssize_t res = safe_read(fd, buf, sizeof(buf));
  if (res < 0)
    return res;
  TEMP_FAILURE_RETRY(::close(fd));
  int a = atoi(buf);
  if (a != getpid())
    return -EDOM;

  res = ::unlink(pid_file);
  if (res)
    return res;

  pid_file[0] = '\0';
  return 0;
}
