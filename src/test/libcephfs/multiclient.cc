// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
#include "include/compat.h"
#include "include/cephfs/libcephfs.h"
#include "include/ceph_fs.h"
#include <atomic>
#include <cstdlib>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <memory>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <thread>
#ifdef __linux__
#include <sys/xattr.h>
#endif

TEST(LibCephFS, MulticlientSimple) {
  struct ceph_mount_info *ca, *cb;
  ASSERT_EQ(ceph_create(&ca, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(ca, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(ca, NULL));
  ASSERT_EQ(ceph_mount(ca, NULL), 0);

  ASSERT_EQ(ceph_create(&cb, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cb, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cb, NULL));
  ASSERT_EQ(ceph_mount(cb, NULL), 0);

  char name[20];
  snprintf(name, sizeof(name), "foo.%d", getpid());
  int fda = ceph_open(ca, name, O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fda);
  int fdb = ceph_open(cb, name, O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fdb);

  char bufa[4] = "foo";
  char bufb[4];

  for (int i=0; i<10; i++) {
    strcpy(bufa, "foo");
    ASSERT_EQ((int)sizeof(bufa), ceph_write(ca, fda, bufa, sizeof(bufa), i*6));
    ASSERT_EQ((int)sizeof(bufa), ceph_read(cb, fdb, bufb, sizeof(bufa), i*6));
    ASSERT_EQ(0, memcmp(bufa, bufb, sizeof(bufa)));
    strcpy(bufb, "bar");
    ASSERT_EQ((int)sizeof(bufb), ceph_write(cb, fdb, bufb, sizeof(bufb), i*6+3));
    ASSERT_EQ((int)sizeof(bufb), ceph_read(ca, fda, bufa, sizeof(bufb), i*6+3));
    ASSERT_EQ(0, memcmp(bufa, bufb, sizeof(bufa)));
  }

  ceph_close(ca, fda);
  ceph_close(cb, fdb);

  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

TEST(LibCephFS, MulticlientHoleEOF) {
  struct ceph_mount_info *ca, *cb;
  ASSERT_EQ(ceph_create(&ca, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(ca, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(ca, NULL));
  ASSERT_EQ(ceph_mount(ca, NULL), 0);

  ASSERT_EQ(ceph_create(&cb, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cb, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cb, NULL));
  ASSERT_EQ(ceph_mount(cb, NULL), 0);

  char name[20];
  snprintf(name, sizeof(name), "foo.%d", getpid());
  int fda = ceph_open(ca, name, O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fda);
  int fdb = ceph_open(cb, name, O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fdb);

  ASSERT_EQ(3, ceph_write(ca, fda, "foo", 3, 0));
  ASSERT_EQ(0, ceph_ftruncate(ca, fda, 1000000));

  char buf[4];
  ASSERT_EQ(2, ceph_read(cb, fdb, buf, sizeof(buf), 1000000-2));
  ASSERT_EQ(0, buf[0]);
  ASSERT_EQ(0, buf[1]);

  ceph_close(ca, fda);
  ceph_close(cb, fdb);

  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

static void write_func(bool *stop)
{
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char name[20];
  snprintf(name, sizeof(name), "foo.%d", getpid());
  int fd = ceph_open(cmount, name, O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fd);

  int buf_size = 4096;
  char *buf = (char *)malloc(buf_size);
  if (!buf) {
    *stop = true;
    printf("write_func failed to allocate buffer!");
    return;
  }
  memset(buf, 1, buf_size);

  while (!(*stop)) {
    int i;

    // truncate the file size to 4096 will set the max_size to 4MB.
    ASSERT_EQ(0, ceph_ftruncate(cmount, fd, 4096));

    // write 4MB + extra 64KB data will make client to trigger to
    // call check_cap() to report new size. And if MDS is revoking
    // the Fsxrw caps and we are still holding the Fw caps and will
    // trigger tracker#57244.
    for (i = 0; i < 1040; i++) {
      ASSERT_EQ(ceph_write(cmount, fd, buf, buf_size, 0), buf_size);
    }
  }

  ceph_shutdown(cmount);
}

static void setattr_func(bool *stop)
{
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char name[20];
  snprintf(name, sizeof(name), "foo.%d", getpid());
  int fd = ceph_open(cmount, name, O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fd);

  while (!(*stop)) {
    // setattr will make the MDS to acquire xlock for the filelock and
    // force to revoke caps from clients
    struct ceph_statx stx = {.stx_size = 0};
    ASSERT_EQ(ceph_fsetattrx(cmount, fd, &stx, CEPH_SETATTR_SIZE), 0);
  }

  ceph_shutdown(cmount);
}

TEST(LibCephFS, MulticlientRevokeCaps) {
  std::thread thread1, thread2;
  bool stop = false;
  int wait = 60; // in second

  thread1 = std::thread(write_func, &stop);
  thread2 = std::thread(setattr_func, &stop);

  printf(" Will run test for %d seconds!\n", wait);
  sleep(wait);
  stop = true;

  thread1.join();
  thread2.join();
}

static void append_worker(struct ceph_mount_info *cmount, const char *path,
			  char tag, int64_t buf_size, int rounds,
			  std::atomic<bool> *go, std::atomic<int> *errors)
{
  std::unique_ptr<char[]> buf(new char[buf_size]);
  memset(buf.get(), tag, buf_size);

  int fd = ceph_open(cmount, path, O_CREAT|O_WRONLY|O_APPEND, 0644);
  if (fd < 0) {
    errors->fetch_add(1);
    printf("append_worker: failed to open %s: %s\n", path, strerror(-fd));
    return;
  }

  while (!go->load()) {
    std::this_thread::yield();
  }

  for (int i = 0; i < rounds; i++) {
    int r = ceph_write(cmount, fd, buf.get(), buf_size, -1);
    if (r != buf_size) {
      errors->fetch_add(1);
      printf("append_worker: write failed: %d (%s)\n", r,
             r < 0 ? strerror(-r) : "short write");
      break;
    }
    // give the other writer a chance to acquire caps, so that the
    // next _lseek(SEEK_END) sees a stale EOF while we wait for Fwx
    usleep(rand() % 500);
  }

  ceph_close(cmount, fd);
}

// Test that O_APPEND writes always land at the EOF that is current
// once the client actually acquires Fwx caps, even when another
// client extended the file in the meantime (tracker #7333).
TEST(LibCephFS, MulticlientAppend) {
  struct ceph_mount_info *ca, *cb;
  ASSERT_EQ(ceph_create(&ca, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(ca, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(ca, NULL));
  ASSERT_EQ(ceph_mount(ca, NULL), 0);

  ASSERT_EQ(ceph_create(&cb, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cb, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cb, NULL));
  ASSERT_EQ(ceph_mount(cb, NULL), 0);

  char name[32];
  snprintf(name, sizeof(name), "append.%d", getpid());

  const int64_t buf_size = 1 << 20;
  const int rounds = 32;

  std::atomic<bool> go{false};
  std::atomic<int> errors{0};

  std::thread thread_a(append_worker, ca, name, 'A', buf_size, rounds,
		       &go, &errors);
  std::thread thread_b(append_worker, cb, name, 'B', buf_size, rounds,
		       &go, &errors);
  go = true;

  thread_a.join();
  thread_b.join();

  ASSERT_EQ(0, errors.load());

  /*
   * Both writers used O_APPEND, so every write must land at the EOF
   * that was current when it acquired Fwx.  Any write reusing a
   * stale EOF would overwrite data and leave the file shorter than
   * the total written.
   */
  int fdr = ceph_open(ca, name, O_RDONLY, 0644);
  ASSERT_LE(0, fdr);

  struct stat st;
  ASSERT_EQ(0, ceph_fstat(ca, fdr, &st));
  ASSERT_EQ(2 * rounds * buf_size, st.st_size);

  std::unique_ptr<char[]> buf(new char[buf_size]);
  int64_t pos = 0;
  int a_blocks = 0, b_blocks = 0;
  while (pos < st.st_size) {
    int64_t got = ceph_read(ca, fdr, buf.get(), buf_size, pos);
    ASSERT_EQ(buf_size, got);

    // every block is written entirely by one client
    char tag = buf[0];
    ASSERT_TRUE(tag == 'A' || tag == 'B');
    for (int64_t i = 1; i < buf_size; i++) {
      ASSERT_EQ(tag, buf[i]);
    }
    if (tag == 'A')
      a_blocks++;
    else
      b_blocks++;
    pos += got;
  }
  ASSERT_EQ(rounds, a_blocks);
  ASSERT_EQ(rounds, b_blocks);

  ceph_close(ca, fdr);
  ASSERT_EQ(0, ceph_unlink(ca, name));

  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

// Test that client #2 can successfully read snap metadata mutation made by
// client #1.
TEST(LibCephFS, SnapMdMutate) {
  struct ceph_mount_info *cmount, *cmount2;

  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_parse_env(cmount, NULL), 0);
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  ASSERT_EQ(ceph_create(&cmount2, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount2, NULL), 0);
  ASSERT_EQ(ceph_conf_parse_env(cmount2, NULL), 0);
  ASSERT_EQ(ceph_mount(cmount2, NULL), 0);

  char dir_path[64];
  char snap_name[64];
  char snap_path[PATH_MAX];
  sprintf(dir_path, "/dir0_%d-5", getpid());
  sprintf(snap_name, "snap_%d_5", getpid());
  sprintf(snap_path, "%s/.snap/%s", dir_path, snap_name);

  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path, 0755));
  // snapshot with custom metadata
  struct snap_metadata snap_meta[] = {{"foo", "bar"},
                                      {"this", "that"},
                                      {"abcde", "12345"}};
  ASSERT_EQ(0, ceph_mksnap(cmount, dir_path, snap_name, 0755, snap_meta,
                           std::size(snap_meta)));

  // verify before update
  struct snap_info info;
  ASSERT_EQ(0, ceph_get_snap_info(cmount2, snap_path, &info));
  ASSERT_GT(info.id, 1);
  ASSERT_EQ(info.nr_snap_metadata, 3);

  for (size_t i = 0; i < info.nr_snap_metadata; ++i) {
    auto k = std::string(info.snap_metadata[i].key);
    auto v = std::string(info.snap_metadata[i].value);

    bool found = false;
    for (size_t j = 0;  j < std::size(snap_meta); ++j) {
      if (k == snap_meta[j].key and v == snap_meta[j].value) {
        found = true;
        break;
      }
    }

    ASSERT_EQ(found, true);
  }

  // actual test -
  ASSERT_EQ(0, ceph_do_snap_md_op(cmount, snap_path, "foo", "bar123",
                                  CEPH_SNAP_MD_OP_CREATE));

  ASSERT_EQ(0, ceph_get_snap_info(cmount2, snap_path, &info));
  ASSERT_GT(info.id, 1);
  ASSERT_EQ(info.nr_snap_metadata, 3);

  // verify snap metadata
  struct snap_metadata snap_meta2[] = {{"foo", "bar123"}, {"this", "that"},
                                       {"abcde", "12345"}};
  for (size_t i = 0; i < info.nr_snap_metadata; ++i) {
    auto k = std::string(info.snap_metadata[i].key);
    auto v = std::string(info.snap_metadata[i].value);

    bool found = false;
    for (size_t j = 0;  j < std::size(snap_meta2); ++j) {
      if (k == snap_meta2[j].key and v == snap_meta2[j].value) {
        found = true;
        break;
      }
    }

    ASSERT_EQ(found, true);
  }

  // remove a key
  ASSERT_EQ(0, ceph_do_snap_md_op(cmount, snap_path, "foo", "",
                                  CEPH_SNAP_MD_OP_REMOVE));

  struct snap_metadata snap_meta3[] = {{"this", "that"}, {"abcde", "12345"}};
  ASSERT_EQ(0, ceph_get_snap_info(cmount2, snap_path, &info));
  ASSERT_GT(info.id, 1);
  ASSERT_EQ(info.nr_snap_metadata, 2);

  // verify snap metadata
  for (size_t i = 0; i < info.nr_snap_metadata; ++i) {
    auto k = std::string(info.snap_metadata[i].key);
    auto v = std::string(info.snap_metadata[i].value);

    bool found = false;
    for (size_t j = 0;  j < std::size(snap_meta3); ++j) {
      if (k == snap_meta3[j].key and v == snap_meta3[j].value) {
        found = true;
        break;
      }
    }

    ASSERT_EQ(found, true);
  }

  // teardown
  ASSERT_EQ(0, ceph_rmsnap(cmount, dir_path, snap_name));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path));
  ceph_shutdown(cmount);
  ceph_shutdown(cmount2);
}
