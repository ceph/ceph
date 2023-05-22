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
#include "include/types.h"
#include "gtest/gtest.h"
#include "include/cephfs/libcephfs.h"
#include "include/fs_types.h"
#include "include/ceph_fs.h"
#include "client/posix_acl.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#ifdef __linux__
#include <sys/xattr.h>
#endif

struct ceph_mount_info *cmount = nullptr;

// close the file will flush the dirty buffer and caps
void write_file(char *file_path, std::string buf, int flags=0)
{
  ASSERT_NE(cmount, nullptr);

  int fd = ceph_open(cmount, file_path, O_RDWR|O_CREAT|flags, 0666);
  ASSERT_LT(0, fd);

  int len = buf.length();
  ASSERT_EQ(len, ceph_write(cmount, fd, buf.c_str(), len, 0));
  std::cout << " write " << len << " bytes to " << file_path << std::endl;
  ceph_close(cmount, fd);
}

TEST(SnapSize, Rstats) {
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  char dir_path[64];

  sprintf(dir_path, "/snapsize_dir_%d", getpid());
  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path, 0755));

  struct ceph_statx stx;
  char file_path[128];
  char c_snapdir_path[256];
  char c_snapshot_path[512];
  sprintf(file_path, "%s/test_file", dir_path);
  sprintf(c_snapdir_path, "%s/.snap", dir_path);

  // ****** Test 'snapA' ******
  write_file(file_path, "abc", O_TRUNC);
  // snapdir '.snap/' size
  ASSERT_EQ(ceph_statx(cmount, c_snapdir_path, &stx, CEPH_STATX_SIZE, 0), 0);
  ASSERT_EQ(stx.stx_size, 0);
  ASSERT_EQ(0, ceph_mksnap(cmount, c_snapdir_path, "snapA", 0755, nullptr, 0));
  std::cout << " mksnap 'snapA'" << std::endl;
  // snapdir '.snap/' size
  ASSERT_EQ(ceph_statx(cmount, c_snapdir_path, &stx, CEPH_STATX_SIZE, 0), 0);
  std::cout << " path " << c_snapdir_path << " size " << stx.stx_size << std::endl;
  ASSERT_EQ(stx.stx_size, 3);
  // snapshot 'snapA' size
  sprintf(c_snapshot_path, "%s/snapA", c_snapdir_path);
  ASSERT_EQ(ceph_statx(cmount, c_snapshot_path, &stx, CEPH_STATX_SIZE, 0), 0);
  std::cout << " path " << c_snapshot_path << " size " << stx.stx_size << std::endl;
  ASSERT_EQ(stx.stx_size, 3);

  // ****** Test 'snapB' ******
  write_file(file_path, "defg");
  ASSERT_EQ(0, ceph_mksnap(cmount, c_snapdir_path, "snapB", 0755, nullptr, 0));
  std::cout << " mksnap 'snapB'" << std::endl;
  // snapdir '.snap/' size
  ASSERT_EQ(ceph_statx(cmount, c_snapdir_path, &stx, CEPH_STATX_SIZE, 0), 0);
  std::cout << " path " << c_snapdir_path << " size " << stx.stx_size << std::endl;
  ASSERT_EQ(stx.stx_size, 7);
  // snapshot 'snapB' size
  sprintf(c_snapshot_path, "%s/snapB", c_snapdir_path);
  ASSERT_EQ(ceph_statx(cmount, c_snapshot_path, &stx, CEPH_STATX_SIZE, 0), 0);
  std::cout << " path " << c_snapshot_path << " size " << stx.stx_size << std::endl;
  ASSERT_EQ(stx.stx_size, 4);

  // Write to the file and test the snapdir '.snap/' and the size shouldn't
  // change
  write_file(file_path, "hijkl");
  // snapdir '.snap/' size
  ASSERT_EQ(ceph_statx(cmount, c_snapdir_path, &stx, CEPH_STATX_SIZE, 0), 0);
  std::cout << " path " << c_snapdir_path << " size " << stx.stx_size << std::endl;
  ASSERT_EQ(stx.stx_size, 7);

  // ****** Test 'snapC' ******
  ASSERT_EQ(0, ceph_mksnap(cmount, c_snapdir_path, "snapC", 0755, nullptr, 0));
  std::cout << " mksnap 'snapC'" << std::endl;
  // snapdir '.snap/' size
  ASSERT_EQ(ceph_statx(cmount, c_snapdir_path, &stx, CEPH_STATX_SIZE, 0), 0);
  std::cout << " path " << c_snapdir_path << " size " << stx.stx_size << std::endl;
  ASSERT_EQ(stx.stx_size, 12);
  // snapshot 'snapC' size
  sprintf(c_snapshot_path, "%s/snapC", c_snapdir_path);
  ASSERT_EQ(ceph_statx(cmount, c_snapshot_path, &stx, CEPH_STATX_SIZE, 0), 0);
  std::cout << " path " << c_snapshot_path << " size " << stx.stx_size << std::endl;
  ASSERT_EQ(stx.stx_size, 5);

  // ****** Test removing 'snapA' ******
  ASSERT_EQ(0, ceph_rmsnap(cmount, c_snapdir_path, "snapA"));
  std::cout << " rmsnap 'snapC'" << std::endl;
  // snapdir '.snap/' size
  ASSERT_EQ(ceph_statx(cmount, c_snapdir_path, &stx, CEPH_STATX_SIZE, 0), 0);
  std::cout << " path " << c_snapdir_path << " size " << stx.stx_size << std::endl;
  ASSERT_EQ(stx.stx_size, 9);

  ASSERT_EQ(0, ceph_rmsnap(cmount, c_snapdir_path, "snapB"));
  ASSERT_EQ(0, ceph_rmsnap(cmount, c_snapdir_path, "snapC"));
  ceph_unlink(cmount, file_path);
  ASSERT_EQ(ceph_rmdir(cmount, dir_path), 0);
  ceph_shutdown(cmount);
}

TEST(SnapSize, NonRstats) {
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(ceph_conf_set(cmount, "client_dirsize_rbytes", "0"), 0);

  char dir_path[64];

  sprintf(dir_path, "/snapsize_nonrstats_dir_%d", getpid());
  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path, 0755));

  struct ceph_statx stx;
  char c_snapdir_path[256];
  char c_snapshot_path[512];
  sprintf(c_snapdir_path, "%s/.snap", dir_path);

  // ****** Test 'snapA' ******
  char file_pathA[128];
  sprintf(file_pathA, "%s/test_fileA", dir_path);
  write_file(file_pathA, "abc");
  // snapdir '.snap/' size
  ASSERT_EQ(ceph_statx(cmount, c_snapdir_path, &stx, CEPH_STATX_SIZE, 0), 0);
  ASSERT_EQ(stx.stx_size, 0);
  ASSERT_EQ(0, ceph_mksnap(cmount, c_snapdir_path, "snapA", 0755, nullptr, 0));
  std::cout << " mksnap 'snapA'" << std::endl;
  // snapdir '.snap/' size
  ASSERT_EQ(ceph_statx(cmount, c_snapdir_path, &stx, CEPH_STATX_SIZE, 0), 0);
  std::cout << " path " << c_snapdir_path << " size " << stx.stx_size << std::endl;
  ASSERT_EQ(stx.stx_size, 1);
  // snapshot 'snapA' size
  sprintf(c_snapshot_path, "%s/snapA", c_snapdir_path);
  ASSERT_EQ(ceph_statx(cmount, c_snapshot_path, &stx, CEPH_STATX_SIZE, 0), 0);
  std::cout << " path " << c_snapshot_path << " size " << stx.stx_size << std::endl;
  ASSERT_EQ(stx.stx_size, 1);

  // ****** Test 'snapB' ******
  char file_pathB[128];
  sprintf(file_pathB, "%s/test_fileB", dir_path);
  write_file(file_pathB, "defg");
  ASSERT_EQ(0, ceph_mksnap(cmount, c_snapdir_path, "snapB", 0755, nullptr, 0));
  std::cout << " mksnap 'snapB'" << std::endl;
  // snapdir '.snap/' size
  ASSERT_EQ(ceph_statx(cmount, c_snapdir_path, &stx, CEPH_STATX_SIZE, 0), 0);
  std::cout << " path " << c_snapdir_path << " size " << stx.stx_size << std::endl;
  ASSERT_EQ(stx.stx_size, 2);
  // snapshot 'snapB' size
  sprintf(c_snapshot_path, "%s/snapB", c_snapdir_path);
  ASSERT_EQ(ceph_statx(cmount, c_snapshot_path, &stx, CEPH_STATX_SIZE, 0), 0);
  std::cout << " path " << c_snapshot_path << " size " << stx.stx_size << std::endl;
  ASSERT_EQ(stx.stx_size, 2);

  // Write to the file and test the snapdir '.snap/' and the size shouldn't
  // change
  char file_pathC[128];
  sprintf(file_pathC, "%s/test_fileC", dir_path);
  write_file(file_pathC, "hijkl");
  // snapdir '.snap/' size
  ASSERT_EQ(ceph_statx(cmount, c_snapdir_path, &stx, CEPH_STATX_SIZE, 0), 0);
  std::cout << " path " << c_snapdir_path << " size " << stx.stx_size << std::endl;
  ASSERT_EQ(stx.stx_size, 2);

  // ****** Test 'snapC' ******
  ASSERT_EQ(0, ceph_mksnap(cmount, c_snapdir_path, "snapC", 0755, nullptr, 0));
  std::cout << " mksnap 'snapC'" << std::endl;
  // snapdir '.snap/' size
  ASSERT_EQ(ceph_statx(cmount, c_snapdir_path, &stx, CEPH_STATX_SIZE, 0), 0);
  std::cout << " path " << c_snapdir_path << " size " << stx.stx_size << std::endl;
  ASSERT_EQ(stx.stx_size, 3);
  // snapshot 'snapC' size
  sprintf(c_snapshot_path, "%s/snapC", c_snapdir_path);
  ASSERT_EQ(ceph_statx(cmount, c_snapshot_path, &stx, CEPH_STATX_SIZE, 0), 0);
  std::cout << " path " << c_snapshot_path << " size " << stx.stx_size << std::endl;
  ASSERT_EQ(stx.stx_size, 3);

  // ****** Test removing 'snapA' ******
  ASSERT_EQ(0, ceph_rmsnap(cmount, c_snapdir_path, "snapA"));
  std::cout << " rmsnap 'snapC'" << std::endl;
  // snapdir '.snap/' size
  ASSERT_EQ(ceph_statx(cmount, c_snapdir_path, &stx, CEPH_STATX_SIZE, 0), 0);
  std::cout << " path " << c_snapdir_path << " size " << stx.stx_size << std::endl;
  ASSERT_EQ(stx.stx_size, 2);

  ASSERT_EQ(0, ceph_rmsnap(cmount, c_snapdir_path, "snapB"));
  ASSERT_EQ(0, ceph_rmsnap(cmount, c_snapdir_path, "snapC"));
  ceph_unlink(cmount, file_pathA);
  ceph_unlink(cmount, file_pathB);
  ceph_unlink(cmount, file_pathC);
  ASSERT_EQ(ceph_rmdir(cmount, dir_path), 0);
  ceph_shutdown(cmount);
}
