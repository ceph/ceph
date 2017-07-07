// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <stdint.h>
#include <tuple>
#include <iostream>
#include <semaphore.h>
#include <time.h>
#include <pthread.h>

#include "include/rados/librgw.h"
#include "include/rados/rgw_file.h"

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"

#define dout_subsys ceph_subsys_rgw

namespace {
  librgw_t rgw = nullptr;
  string userid("testuser");
  string access_key("");
  string secret_key("");
  struct rgw_fs *fs = nullptr;

  uint32_t owner_uid = 867;
  uint32_t owner_gid = 5309;
  uint32_t create_mask = RGW_SETATTR_UID | RGW_SETATTR_GID | RGW_SETATTR_MODE;

  bool do_create = false;
  bool do_delete = false;
  bool do_basic = false;
  bool do_concurrent = false;

  string bucket_name = "sorry_joy";
  string basic_fname = "basic";
  string concurrent_fname = "concurrent";

  struct rgw_file_handle *bucket_fh = nullptr;
  struct rgw_file_handle *basic_fh = nullptr;
  struct rgw_file_handle *concurrent_fh = nullptr;

  struct {
    int argc;
    char **argv;
  } saved_args;
}

TEST(LibRGW, INIT) {
  int ret = librgw_create(&rgw, saved_args.argc, saved_args.argv);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(rgw, nullptr);
}

TEST(LibRGW, MOUNT) {
  int ret = rgw_mount(rgw, userid.c_str(), access_key.c_str(),
		      secret_key.c_str(), &fs, RGW_MOUNT_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(fs, nullptr);
}

TEST(LibRGW, SETUP) {
  (void) rgw_lookup(fs, fs->root_fh, bucket_name.c_str(), &bucket_fh,
		    RGW_LOOKUP_FLAG_NONE);
  if (! bucket_fh) {
    if (do_create) {
      struct stat st;

      st.st_uid = owner_uid;
      st.st_gid = owner_gid;
      st.st_mode = 755;

      int ret = rgw_mkdir(fs, fs->root_fh, bucket_name.c_str(), &st, create_mask,
                          &bucket_fh, RGW_MKDIR_FLAG_NONE);
      ASSERT_EQ(ret, 0);
      ASSERT_NE(bucket_fh, nullptr);

      if (do_basic) {
        ret = rgw_create(fs, bucket_fh, basic_fname.c_str(), &st, create_mask,
                         &basic_fh, 0, RGW_CREATE_FLAG_NONE);
        ASSERT_EQ(ret, 0);
        ret = rgw_open(fs, basic_fh, 0, 0);
        ASSERT_EQ(ret, 0);
        size_t nbytes;
        char writebuf[4096];
        ret = rgw_write(fs, basic_fh, 0, 4096, &nbytes, writebuf, RGW_WRITE_FLAG_NONE);
        ASSERT_EQ(ret, 0);
        ret = rgw_close(fs, basic_fh, RGW_CLOSE_FLAG_NONE);
        ASSERT_EQ(ret, 0);
      }
      if (do_concurrent) {
        ret = rgw_create(fs, bucket_fh, concurrent_fname.c_str(), &st, create_mask,
                         &concurrent_fh, 0, RGW_CREATE_FLAG_NONE);
        ASSERT_EQ(ret, 0);
        ret = rgw_open(fs, concurrent_fh, 0, 0);
        ASSERT_EQ(ret, 0);
        size_t nbytes;
        char writebuf[4096];
        ret = rgw_write(fs, concurrent_fh, 0, 4096, &nbytes, writebuf, RGW_WRITE_FLAG_NONE);
        ASSERT_EQ(ret, 0);
        ret = rgw_close(fs, concurrent_fh, RGW_CLOSE_FLAG_NONE);
        ASSERT_EQ(ret, 0);
      }
    }
  }
}

TEST(LibRGW, BASIC_LOCKING) {
  if (do_basic) {
    int ret;

    if (! basic_fh) {
      ret = rgw_lookup(fs, bucket_fh, basic_fname.c_str(), &basic_fh,
			RGW_LOOKUP_FLAG_NONE);
      ASSERT_EQ(ret, 0);
      ASSERT_NE(basic_fh, nullptr);
    }

    ret = rgw_open(fs, basic_fh, 0, 0);
    ASSERT_EQ(ret, 0);

    struct rgw_flock r1, r2;
    r1.client = r2.client = 233;
    r1.owner = r2.owner = 234;
    r1.pid = r2.pid = 235;
    r1.type = r2.type = RGW_FS_TYPE_FL_UNLOCK;

    // rd/wr, get, un, get
    for (auto ltype : { RGW_FS_TYPE_FL_RDLOCK, RGW_FS_TYPE_FL_WRLOCK}) {
      r1.offset = 0; r1.length = 512;
      r1.type = ltype;
      ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));

      ASSERT_EQ(0, rgw_getlk(fs, basic_fh, &r1, 0));
      ASSERT_EQ(r1.offset, 0);
      ASSERT_EQ(r1.length, 512);
      ASSERT_EQ(r1.client, 233);
      ASSERT_EQ(r1.owner, 234);
      ASSERT_EQ(r1.pid, 235);
      ASSERT_EQ(r1.type, ltype);

      r1.type = RGW_FS_TYPE_FL_UNLOCK;
      ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));

      r1.type = ltype;
      ASSERT_EQ(0, rgw_getlk(fs, basic_fh, &r1, 0));
      ASSERT_EQ(r1.offset, 0);
      ASSERT_EQ(r1.length, 512);
      ASSERT_EQ(r1.client, 233);
      ASSERT_EQ(r1.owner, 234);
      ASSERT_EQ(r1.pid, 235);
      ASSERT_EQ(r1.type, RGW_FS_TYPE_FL_UNLOCK);
    }

    // overlap, two rd
    r1.offset = 0; r1.length = 513; r2.offset = 511; r2.length = 513;
    r1.type = r2.type = RGW_FS_TYPE_FL_RDLOCK;
    ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));

    ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r2, RGW_LOCK_FLAG_NONBLOCK));

    ASSERT_EQ(0, rgw_getlk(fs, basic_fh, &r1, 0));
    ASSERT_EQ(r1.offset, 0);
    ASSERT_EQ(r1.length, 511);
    ASSERT_EQ(r1.client, 233);
    ASSERT_EQ(r1.owner, 234);
    ASSERT_EQ(r1.pid, 235);
    ASSERT_EQ(r1.type, RGW_FS_TYPE_FL_RDLOCK);

    ASSERT_EQ(0, rgw_getlk(fs, basic_fh, &r2, 0));
    ASSERT_EQ(r2.offset, 511);
    ASSERT_EQ(r2.length, 513);
    ASSERT_EQ(r2.client, 233);
    ASSERT_EQ(r2.owner, 234);
    ASSERT_EQ(r2.pid, 235);
    ASSERT_EQ(r2.type, RGW_FS_TYPE_FL_RDLOCK);

    r1.type = RGW_FS_TYPE_FL_UNLOCK;
    r1.offset = 0; r1.length = 1024;
    ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));

    r1.type = RGW_FS_TYPE_FL_RDLOCK;
    ASSERT_EQ(0, rgw_getlk(fs, basic_fh, &r1, 0));
    ASSERT_EQ(r1.offset, 0);
    ASSERT_EQ(r1.length, 1024);
    ASSERT_EQ(r1.client, 233);
    ASSERT_EQ(r1.owner, 234);
    ASSERT_EQ(r1.pid, 235);
    ASSERT_EQ(r1.type, RGW_FS_TYPE_FL_UNLOCK);

    // overlap, different type
    r1.offset = 0; r1.length = 513; r2.offset = 511; r2.length =513;
    r1.type = RGW_FS_TYPE_FL_RDLOCK;
    r2.type = RGW_FS_TYPE_FL_WRLOCK;
    ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));

    ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r2, RGW_LOCK_FLAG_NONBLOCK));

    ASSERT_EQ(0, rgw_getlk(fs, basic_fh, &r1, 0));
    ASSERT_EQ(r1.offset, 0);
    ASSERT_EQ(r1.length, 511);
    ASSERT_EQ(r1.client, 233);
    ASSERT_EQ(r1.owner, 234);
    ASSERT_EQ(r1.pid, 235);
    ASSERT_EQ(r1.type, RGW_FS_TYPE_FL_RDLOCK);

    ASSERT_EQ(0, rgw_getlk(fs, basic_fh, &r2, 0));
    ASSERT_EQ(r2.offset, 511);
    ASSERT_EQ(r2.length, 513);
    ASSERT_EQ(r2.client, 233);
    ASSERT_EQ(r2.owner, 234);
    ASSERT_EQ(r2.pid, 235);
    ASSERT_EQ(r2.type, RGW_FS_TYPE_FL_WRLOCK);

    r1.type = RGW_FS_TYPE_FL_UNLOCK;
    r1.offset = 0; r1.length = 1024;
    ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));

    r1.type = RGW_FS_TYPE_FL_RDLOCK;
    ASSERT_EQ(0, rgw_getlk(fs, basic_fh, &r1, 0));
    ASSERT_EQ(r1.offset, 0);
    ASSERT_EQ(r1.length, 1024);
    ASSERT_EQ(r1.client, 233);
    ASSERT_EQ(r1.owner, 234);
    ASSERT_EQ(r1.pid, 235);
    ASSERT_EQ(r1.type, RGW_FS_TYPE_FL_UNLOCK);

    // (upgrade): rd, wr, get, un, get
    r1.offset = 0; r1.length = 512; r2.offset = 0; r2.length =512;
    r1.type = RGW_FS_TYPE_FL_RDLOCK;
    r2.type = RGW_FS_TYPE_FL_WRLOCK;
    ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));

    ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r2, RGW_LOCK_FLAG_NONBLOCK));

    ASSERT_EQ(0, rgw_getlk(fs, basic_fh, &r1, 0));
    ASSERT_EQ(r1.offset, 0);
    ASSERT_EQ(r1.length, 512);
    ASSERT_EQ(r1.client, 233);
    ASSERT_EQ(r1.owner, 234);
    ASSERT_EQ(r1.pid, 235);
    ASSERT_EQ(r1.type, RGW_FS_TYPE_FL_WRLOCK);

    r1.type = RGW_FS_TYPE_FL_UNLOCK;
    r1.offset = 0; r1.length = 512;
    ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));

    r1.type = RGW_FS_TYPE_FL_WRLOCK;
    ASSERT_EQ(0, rgw_getlk(fs, basic_fh, &r1, 0));
    ASSERT_EQ(r1.offset, 0);
    ASSERT_EQ(r1.length, 512);
    ASSERT_EQ(r1.client, 233);
    ASSERT_EQ(r1.owner, 234);
    ASSERT_EQ(r1.pid, 235);
    ASSERT_EQ(r1.type, RGW_FS_TYPE_FL_UNLOCK);

    // (downgrade): wr, rd, get, un, get
    r1.offset = 0; r1.length = 512; r2.offset = 0; r2.length =512;
    r1.type = RGW_FS_TYPE_FL_WRLOCK;
    r2.type = RGW_FS_TYPE_FL_RDLOCK;
    ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));

    ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r2, RGW_LOCK_FLAG_NONBLOCK));

    ASSERT_EQ(0, rgw_getlk(fs, basic_fh, &r1, 0));
    ASSERT_EQ(r1.offset, 0);
    ASSERT_EQ(r1.length, 512);
    ASSERT_EQ(r1.client, 233);
    ASSERT_EQ(r1.owner, 234);
    ASSERT_EQ(r1.pid, 235);
    ASSERT_EQ(r1.type, RGW_FS_TYPE_FL_RDLOCK);

    r1.type = RGW_FS_TYPE_FL_UNLOCK;
    r1.offset = 0; r1.length = 512;
    ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));

    r1.type = RGW_FS_TYPE_FL_RDLOCK;
    ASSERT_EQ(0, rgw_getlk(fs, basic_fh, &r1, 0));
    ASSERT_EQ(r1.offset, 0);
    ASSERT_EQ(r1.length, 512);
    ASSERT_EQ(r1.client, 233);
    ASSERT_EQ(r1.owner, 234);
    ASSERT_EQ(r1.pid, 235);
    ASSERT_EQ(r1.type, RGW_FS_TYPE_FL_UNLOCK);

    // (split): rd, wr/un, get, un, get
    for (auto ltype : { RGW_FS_TYPE_FL_WRLOCK, RGW_FS_TYPE_FL_UNLOCK}) {
      r1.offset = 0; r1.length = 1024; r2.offset = 256; r2.length =256;
      r1.type = RGW_FS_TYPE_FL_RDLOCK;
      r2.type = ltype;
      ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));

      ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r2, RGW_LOCK_FLAG_NONBLOCK));

      r1.length = 256;
      ASSERT_EQ(0, rgw_getlk(fs, basic_fh, &r1, 0));
      ASSERT_EQ(r1.offset, 0);
      ASSERT_EQ(r1.length, 256);
      ASSERT_EQ(r1.client, 233);
      ASSERT_EQ(r1.owner, 234);
      ASSERT_EQ(r1.pid, 235);
      ASSERT_EQ(r1.type, RGW_FS_TYPE_FL_RDLOCK);

      r1.offset = 512; r1.length = 512;
      ASSERT_EQ(0, rgw_getlk(fs, basic_fh, &r1, 0));
      ASSERT_EQ(r1.offset, 512);
      ASSERT_EQ(r1.length, 512);
      ASSERT_EQ(r1.client, 233);
      ASSERT_EQ(r1.owner, 234);
      ASSERT_EQ(r1.pid, 235);
      ASSERT_EQ(r1.type, RGW_FS_TYPE_FL_RDLOCK);

      r1.type = RGW_FS_TYPE_FL_UNLOCK;
      r1.offset = 0; r1.length = 1024;
      ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));

      r1.type = RGW_FS_TYPE_FL_RDLOCK;
      ASSERT_EQ(0, rgw_getlk(fs, basic_fh, &r1, 0));
      ASSERT_EQ(r1.offset, 0);
      ASSERT_EQ(r1.length, 1024);
      ASSERT_EQ(r1.client, 233);
      ASSERT_EQ(r1.owner, 234);
      ASSERT_EQ(r1.pid, 235);
      ASSERT_EQ(r1.type, RGW_FS_TYPE_FL_UNLOCK);
    }

    // (split): wr, rd/un, get, un, get
    for (auto ltype : { RGW_FS_TYPE_FL_RDLOCK, RGW_FS_TYPE_FL_UNLOCK}) {
      r1.offset = 0; r1.length = 1024; r2.offset = 256; r2.length =256;
      r1.type = RGW_FS_TYPE_FL_WRLOCK;
      r2.type = ltype;
      ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));

      ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r2, RGW_LOCK_FLAG_NONBLOCK));

      r1.length = 256;
      ASSERT_EQ(0, rgw_getlk(fs, basic_fh, &r1, 0));
      ASSERT_EQ(r1.offset, 0);
      ASSERT_EQ(r1.length, 256);
      ASSERT_EQ(r1.client, 233);
      ASSERT_EQ(r1.owner, 234);
      ASSERT_EQ(r1.pid, 235);
      ASSERT_EQ(r1.type, RGW_FS_TYPE_FL_WRLOCK);

      r1.offset = 512; r1.length = 512;
      ASSERT_EQ(0, rgw_getlk(fs, basic_fh, &r1, 0));
      ASSERT_EQ(r1.offset, 512);
      ASSERT_EQ(r1.length, 512);
      ASSERT_EQ(r1.client, 233);
      ASSERT_EQ(r1.owner, 234);
      ASSERT_EQ(r1.pid, 235);
      ASSERT_EQ(r1.type, RGW_FS_TYPE_FL_WRLOCK);

      r1.type = RGW_FS_TYPE_FL_UNLOCK;
      r1.offset = 0; r1.length = 1024;
      ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));

      r1.type = RGW_FS_TYPE_FL_RDLOCK;
      ASSERT_EQ(0, rgw_getlk(fs, basic_fh, &r1, 0));
      ASSERT_EQ(r1.offset, 0);
      ASSERT_EQ(r1.length, 1024);
      ASSERT_EQ(r1.client, 233);
      ASSERT_EQ(r1.owner, 234);
      ASSERT_EQ(r1.pid, 235);
      ASSERT_EQ(r1.type, RGW_FS_TYPE_FL_UNLOCK);
    }


    // lock from others
    struct rgw_flock r3;
    r1.offset = 0; r1.length = 512; r3.offset = 256; r3.length = 512;
    r3.client = 566;
    r3.owner = 666;
    r3.pid = 766;
    r3.type = RGW_FS_TYPE_FL_UNLOCK;
    int ret2;

    // (share):
    //     A (rd), B (rd)
    // (conflict):
    //     A (wr), B (rd)
    //     A (rd), B (wr)
    //     A (wr), B (wr)
    for (auto atype : { RGW_FS_TYPE_FL_RDLOCK, RGW_FS_TYPE_FL_WRLOCK }) {
      for (auto btype : { RGW_FS_TYPE_FL_RDLOCK, RGW_FS_TYPE_FL_WRLOCK }) {
	r1.type = atype;
	r3.type = btype;

	ret = rgw_setlk(fs, basic_fh, &r1, RGW_LOCK_FLAG_NONBLOCK);
	ret2 = rgw_setlk(fs, basic_fh, &r3, RGW_LOCK_FLAG_NONBLOCK);

	if (r1.type == RGW_FS_TYPE_FL_RDLOCK && r3.type == RGW_FS_TYPE_FL_RDLOCK) {
	  ASSERT_EQ(0, ret);
	  ASSERT_EQ(0, ret2);
	} else {
	  ASSERT_EQ(0, ret);
	  ASSERT_EQ(-EWOULDBLOCK, ret2);
	}

	r1.type = r3.type = RGW_FS_TYPE_FL_UNLOCK;
	ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));
	ASSERT_EQ(0, rgw_setlk(fs, basic_fh, &r3, RGW_LOCK_FLAG_NONBLOCK));

        r1.type = atype;
        r3.type = btype;

        ASSERT_EQ(0, rgw_getlk(fs, basic_fh, &r1, 0));
        ASSERT_EQ(r1.offset, 0);
        ASSERT_EQ(r1.length, 512);
        ASSERT_EQ(r1.client, 233);
        ASSERT_EQ(r1.owner, 234);
        ASSERT_EQ(r1.pid, 235);
        ASSERT_EQ(r1.type, RGW_FS_TYPE_FL_UNLOCK);

        ASSERT_EQ(0, rgw_getlk(fs, basic_fh, &r3, 0));
        ASSERT_EQ(r3.offset, 256);
        ASSERT_EQ(r3.length, 512);
        ASSERT_EQ(r3.client, 566);
        ASSERT_EQ(r3.owner, 666);
        ASSERT_EQ(r3.pid, 766);
        ASSERT_EQ(r3.type, RGW_FS_TYPE_FL_UNLOCK);
      }
    }

    ret = rgw_close(fs, basic_fh, 0);
    ASSERT_EQ(ret, 0);
    ret = rgw_fh_rele(fs, basic_fh, 0);
    ASSERT_EQ(ret, 0);
  }
}

struct ConcurrentLockingNotifier {
  sem_t sem[2];
  sem_t semReply[2];

  void sem_init(int pshared) {
    ASSERT_EQ(0, ::sem_init(&sem[0], pshared, 0));
    ASSERT_EQ(0, ::sem_init(&sem[1], pshared, 0));
    ASSERT_EQ(0, ::sem_init(&semReply[0], pshared, 0));
    ASSERT_EQ(0, ::sem_init(&semReply[1], pshared, 0));
  }
  void sem_destroy() {
    ASSERT_EQ(0, ::sem_destroy(&sem[0]));
    ASSERT_EQ(0, ::sem_destroy(&sem[1]));
    ASSERT_EQ(0, ::sem_destroy(&semReply[0]));
    ASSERT_EQ(0, ::sem_destroy(&semReply[1]));
  }
};

// Default wait time for normal and "slow" operations
// (5" should be enough in case of network congestion)
static const long waitMs = 10;
static const long waitSlowMs = 5000;

// Get the absolute struct timespec reference from now + 'ms' milliseconds
static const struct timespec* abstime(struct timespec &ts, long ms) {
  if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
    abort();
  }
  ts.tv_nsec += ms * 1000000;
  ts.tv_sec += ts.tv_nsec / 1000000000;
  ts.tv_nsec %= 1000000000;
  return &ts;
}

// Wakeup main (for (N) steps)
#define PING_MAIN(n) ASSERT_EQ(0, sem_post(&s.sem[n%2]))
// Wait for main to wake us up (for (RN) steps)
#define WAIT_MAIN(n) \
  ASSERT_EQ(0, sem_timedwait(&s.semReply[n%2], abstime(ts, waitSlowMs)))

// Wakeup worker (for (RN) steps)
#define PING_WORKER(n) ASSERT_EQ(0, sem_post(&s.semReply[n%2]))
// Wait for worker to wake us up (for (N) steps)
#define WAIT_WORKER(n) \
  ASSERT_EQ(0, sem_timedwait(&s.sem[n%2], abstime(ts, waitSlowMs)))
// Worker shall not wake us up (for (N) steps)
#define NOT_WAIT_WORKER(n) \
  ASSERT_EQ(-1, sem_timedwait(&s.sem[n%2], abstime(ts, waitMs)))

// Do twice an operation
#define TWICE(EXPR) do {			\
    EXPR;					\
    EXPR;					\
  } while(0)

static void concurrentLockingTask(ConcurrentLockingNotifier &s) {
  struct timespec ts;

  struct rgw_flock r2;
  r2.offset = 0; r2.length = 1024;
  r2.client = 233;
  r2.owner = 234;
  r2.pid = 566;
  r2.type = RGW_FS_TYPE_FL_WRLOCK;
  ASSERT_EQ(-EWOULDBLOCK, rgw_setlk(fs, concurrent_fh, &r2, RGW_LOCK_FLAG_NONBLOCK));

  PING_MAIN(1); // (1)
  r2.offset = 0; r2.length = 1024;
  r2.client = 233;
  r2.owner = 234;
  r2.pid = 566;
  r2.type = RGW_FS_TYPE_FL_WRLOCK;
  ASSERT_EQ(0, rgw_setlk(fs, concurrent_fh, &r2, 0));
  PING_MAIN(2); // (2)

  r2.offset = 0; r2.length = 1024;
  r2.client = 233;
  r2.owner = 234;
  r2.pid = 566;
  r2.type = RGW_FS_TYPE_FL_UNLOCK;
  ASSERT_EQ(0, rgw_setlk(fs, concurrent_fh, &r2, RGW_LOCK_FLAG_NONBLOCK));
  PING_MAIN(3); // (3)

  r2.offset = 0; r2.length = 1024;
  r2.client = 233;
  r2.owner = 234;
  r2.pid = 566;
  r2.type = RGW_FS_TYPE_FL_RDLOCK;
  ASSERT_EQ(0, rgw_setlk(fs, concurrent_fh, &r2, 0));
  PING_MAIN(4); // (4)

  WAIT_MAIN(1); // (R1)
  r2.offset = 0; r2.length = 1024;
  r2.client = 233;
  r2.owner = 234;
  r2.pid = 566;
  r2.type = RGW_FS_TYPE_FL_UNLOCK;
  ASSERT_EQ(0, rgw_setlk(fs, concurrent_fh, &r2, 0));
  PING_MAIN(5); // (5)

  WAIT_MAIN(2); // (R2)
  r2.offset = 0; r2.length = 1024;
  r2.client = 233;
  r2.owner = 234;
  r2.pid = 566;
  r2.type = RGW_FS_TYPE_FL_WRLOCK;
  ASSERT_EQ(0, rgw_setlk(fs, concurrent_fh, &r2, 0));
  PING_MAIN(6); // (6)

  WAIT_MAIN(3); // (R3)
  r2.offset = 0; r2.length = 1024;
  r2.client = 233;
  r2.owner = 234;
  r2.pid = 566;
  r2.type = RGW_FS_TYPE_FL_UNLOCK;
  ASSERT_EQ(0, rgw_setlk(fs, concurrent_fh, &r2, 0));
  PING_MAIN(7); // (7)
}

static void *concurrentLockingTaskHook(void *arg) {
  ConcurrentLockingNotifier *const s = reinterpret_cast<ConcurrentLockingNotifier *>(arg);
  concurrentLockingTask(*s);
  return nullptr;
}

TEST(LibRGW, CONCURRENT_LOCKING) {
  if (do_concurrent) {
    int ret;

    if (! concurrent_fh) {
      ret = rgw_lookup(fs, bucket_fh, concurrent_fname.c_str(), &concurrent_fh,
			RGW_LOOKUP_FLAG_NONE);
      ASSERT_EQ(ret, 0);
      ASSERT_NE(concurrent_fh, nullptr);
    }

    ret = rgw_open(fs, concurrent_fh, 0, 0);
    ASSERT_EQ(ret, 0);

    // Lock
    struct rgw_flock r1;
    r1.offset = 0; r1.length = 1024;
    r1.client = 233;
    r1.owner = 234;
    r1.pid = 235;
    r1.type = RGW_FS_TYPE_FL_WRLOCK;
    ASSERT_EQ(0, rgw_setlk(fs, concurrent_fh, &r1, 0));

    // start the other thread
    pthread_t thread;
    struct timespec ts;
    ConcurrentLockingNotifier s;
    s.sem_init(0);
    ASSERT_EQ(0, pthread_create(&thread, NULL, concurrentLockingTaskHook, &s));

    // Synchronization point with thread (failure: thread is dead)
    WAIT_WORKER(1); // (1)

    // Shall not have lock immediately
    NOT_WAIT_WORKER(2); // (2)

    // Unlock
    r1.offset = 0; r1.length = 1024;
    r1.client = 233;
    r1.owner = 234;
    r1.pid = 235;
    r1.type = RGW_FS_TYPE_FL_UNLOCK;
    ASSERT_EQ(0, rgw_setlk(fs, concurrent_fh, &r1, 0));

    // Shall have lock
    // Synchronization point with thread (failure: thread is dead)
    WAIT_WORKER(2); // (2)

    // Synchronization point with thread (failure: thread is dead)
    WAIT_WORKER(3); // (3)

    // Wait for thread to share lock
    WAIT_WORKER(4); // (4)
    r1.offset = 0; r1.length = 1024;
    r1.client = 233;
    r1.owner = 234;
    r1.pid = 235;
    r1.type = RGW_FS_TYPE_FL_WRLOCK;
    ASSERT_EQ(-EWOULDBLOCK, rgw_setlk(fs, concurrent_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));

    r1.offset = 0; r1.length = 1024;
    r1.client = 233;
    r1.owner = 234;
    r1.pid = 235;
    r1.type = RGW_FS_TYPE_FL_RDLOCK;
    ASSERT_EQ(0, rgw_setlk(fs, concurrent_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));

    // Wake up thread to unlock shared lock
    PING_WORKER(1); // (R1)
    WAIT_WORKER(5); // (5)

    // Now we can lock exclusively
    r1.offset = 0; r1.length = 1024;
    r1.client = 233;
    r1.owner = 234;
    r1.pid = 235;
    r1.type = RGW_FS_TYPE_FL_WRLOCK;
    ASSERT_EQ(0, rgw_setlk(fs, concurrent_fh, &r1, 0));

    // Wake up thread to lock shared lock
    PING_WORKER(2); // (R2)

    // Shall not have lock immediately
    NOT_WAIT_WORKER(6); // (6)

    // Release lock ; thread will get it
    r1.offset = 0; r1.length = 1024;
    r1.client = 233;
    r1.owner = 234;
    r1.pid = 235;
    r1.type = RGW_FS_TYPE_FL_UNLOCK;
    ASSERT_EQ(0, rgw_setlk(fs, concurrent_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));
    WAIT_WORKER(6); // (6)

    // We no longer have the lock
    r1.offset = 0; r1.length = 1024;
    r1.client = 233;
    r1.owner = 234;
    r1.pid = 235;
    r1.type = RGW_FS_TYPE_FL_WRLOCK;
    ASSERT_EQ(-EWOULDBLOCK, rgw_setlk(fs, concurrent_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));

    r1.offset = 0; r1.length = 1024;
    r1.client = 233;
    r1.owner = 234;
    r1.pid = 235;
    r1.type = RGW_FS_TYPE_FL_RDLOCK;
    ASSERT_EQ(-EWOULDBLOCK, rgw_setlk(fs, concurrent_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));

    // Wake up thread to unlock exclusive lock
    PING_WORKER(3); // (R3)
    WAIT_WORKER(7); // (7)

    // We can lock it again
    r1.offset = 0; r1.length = 1024;
    r1.client = 233;
    r1.owner = 234;
    r1.pid = 235;
    r1.type = RGW_FS_TYPE_FL_WRLOCK;
    ASSERT_EQ(0, rgw_setlk(fs, concurrent_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));

    r1.offset = 0; r1.length = 1024;
    r1.client = 233;
    r1.owner = 234;
    r1.pid = 235;
    r1.type = RGW_FS_TYPE_FL_UNLOCK;
    ASSERT_EQ(0, rgw_setlk(fs, concurrent_fh, &r1, RGW_LOCK_FLAG_NONBLOCK));

    void *tret = (void *)(uintptr_t)-1;
    ASSERT_EQ(0, pthread_join(thread, &tret));
    ASSERT_EQ(NULL, tret);
    s.sem_destroy();

    ret = rgw_close(fs, concurrent_fh, 0);
    ASSERT_EQ(ret, 0);
    ret = rgw_fh_rele(fs, concurrent_fh, 0);
    ASSERT_EQ(ret, 0);
  }
}

TEST(LibRGW, CLEANUP) {
  if (do_delete) {
    int ret;
    if (do_basic) {
      ret = rgw_unlink(fs, bucket_fh, basic_fname.c_str(), RGW_UNLINK_FLAG_NONE);
      ASSERT_EQ(ret, 0);
    }
    if (do_concurrent) {
      ret = rgw_unlink(fs, bucket_fh, concurrent_fname.c_str(), RGW_UNLINK_FLAG_NONE);
      ASSERT_EQ(ret, 0);
    }
    ret = rgw_unlink(fs, fs->root_fh, bucket_name.c_str(), RGW_UNLINK_FLAG_NONE);
    ASSERT_EQ(ret, 0);
  }
}

TEST(LibRGW, UMOUNT) {
  if (! fs)
    return;

  int ret = rgw_umount(fs, RGW_UMOUNT_FLAG_NONE);
  ASSERT_EQ(ret, 0);
}

TEST(LibRGW, SHUTDOWN) {
  librgw_shutdown(rgw);
}

int main(int argc, char *argv[])
{
  char *v{nullptr};
  string val;
  vector<const char*> args;

  argv_to_vec(argc, const_cast<const char**>(argv), args);
  env_to_vec(args);

  v = getenv("AWS_ACCESS_KEY_ID");
  if (v) {
    access_key = v;
  }

  v = getenv("AWS_SECRET_ACCESS_KEY");
  if (v) {
    secret_key = v;
  }

  for (auto arg_iter = args.begin(); arg_iter != args.end();) {
    if (ceph_argparse_witharg(args, arg_iter, &val, "--access",
			      (char*) nullptr)) {
      access_key = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--secret",
				     (char*) nullptr)) {
      secret_key = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--userid",
				     (char*) nullptr)) {
      userid = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--bn",
				     (char*) nullptr)) {
      bucket_name = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--uid",
				     (char*) nullptr)) {
      owner_uid = std::stoi(val);
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--gid",
				     (char*) nullptr)) {
      owner_gid = std::stoi(val);
    } else if (ceph_argparse_flag(args, arg_iter, "--create",
					    (char*) nullptr)) {
      do_create = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--delete",
					    (char*) nullptr)) {
      do_delete = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--basic",
					    (char*) nullptr)) {
      do_basic = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--concurrent",
					    (char*) nullptr)) {
      do_concurrent = true;
    } else {
      ++arg_iter;
    }
  }

  /* dont accidentally run as anonymous */
  if ((access_key == "") ||
      (secret_key == "")) {
    std::cout << argv[0] << " no AWS credentials, exiting" << std::endl;
    return EPERM;
  }

  saved_args.argc = argc;
  saved_args.argv = argv;

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
