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

#include "gtest/gtest.h"
#include "include/cephfs/libcephfs.h"
#include <linux/types.h>
#include <inttypes.h>
#include "include/ceph_fs.h"
#include <errno.h>
#include <sys/fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/xattr.h>
#include <signal.h>

void do_sigusr1(int s) {}

// wait_and_suspend() forks the process, waits for the
// child to signal SIGUSR1, suspends the child with SIGSTOP
// sleeps for s seconds, and then unsuspends the child,
// waits for the child to exit, and then returns the exit code
// of the child
static int _wait_and_suspend(int s) {

  int fpid = fork();
  if (fpid != 0) {
    // wait for child to signal
    signal(SIGUSR1, &do_sigusr1);
    sigset_t set;
    sigaddset(&set, SIGUSR1);
    int sig;
    sigwait(&set, &sig);

    // fork and suspend child, sleep for 20 secs, and resume
    kill(fpid, SIGSTOP);
    sleep(s);
    kill(fpid, SIGCONT);
    int status;
    wait(&status);
    if (WIFEXITED(status))
      return WEXITSTATUS(status);
    return 1;
  }
  return -1;
}

// signal_for_suspend sends the parent the SIGUSR1 signal
// and sleeps for 1 second so that it can be suspended at the
// point of the call
static void _signal_for_suspend() {
  kill(getppid(), SIGUSR1);
}

TEST(Caps, ReadZero) {

  int w = _wait_and_suspend(20);
  if (w >= 0) {
    ASSERT_EQ(0, w);
    return;
  }

  pid_t mypid = getpid();
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(0, ceph_conf_set(cmount, "client_cache_size", "10"));

  int i = 0;
  for(; i < 30; ++i) {

    char c_path[1024];
    sprintf(c_path, "/caps_rzfile_%d_%d", mypid, i);
    int fd = ceph_open(cmount, c_path, O_CREAT|O_TRUNC|O_WRONLY, 0644);
    ASSERT_LT(0, fd);

    int expect = CEPH_CAP_FILE_EXCL | CEPH_CAP_FILE_WR | CEPH_CAP_FILE_BUFFER;
    int caps = ceph_debug_get_fd_caps(cmount, fd);

    ASSERT_EQ(expect, caps & expect);
    ASSERT_EQ(0, ceph_close(cmount, fd));

    caps = ceph_debug_get_file_caps(cmount, c_path);
    ASSERT_EQ(expect, caps & expect);

    char cw_path[1024];
    sprintf(cw_path, "/caps_wzfile_%d_%d", mypid, i);
    int wfd = ceph_open(cmount, cw_path, O_CREAT|O_TRUNC|O_WRONLY, 0644);
    ASSERT_LT(0, wfd);

    char wbuf[4096];
    ASSERT_EQ(4096, ceph_write(cmount, wfd, wbuf, 4096, 0));

    ASSERT_EQ(0, ceph_close(cmount, wfd));

    struct stat st;
    ASSERT_EQ(0, ceph_stat(cmount, c_path, &st));

    caps = ceph_debug_get_file_caps(cmount, c_path);
    ASSERT_EQ(expect, caps & expect);
  }

  _signal_for_suspend();

  for(i = 0; i < 30; ++i) {

    char c_path[1024];
    sprintf(c_path, "/caps_rzfile_%d_%d", mypid, i);

    int fd = ceph_open(cmount, c_path, O_RDONLY, 0);
    ASSERT_LT(0, fd);
    char buf[256];

    int expect = CEPH_CAP_FILE_RD | CEPH_STAT_CAP_SIZE | CEPH_CAP_FILE_CACHE;
    int caps = ceph_debug_get_fd_caps(cmount, fd);
    ASSERT_EQ(expect, caps & expect);
    ASSERT_EQ(0, ceph_read(cmount, fd, buf, 256, 0));

    caps = ceph_debug_get_fd_caps(cmount, fd);
    ASSERT_EQ(expect, caps & expect);
    ASSERT_EQ(0, ceph_close(cmount, fd));

  }
  ceph_shutdown(cmount);
}
