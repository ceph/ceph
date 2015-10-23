// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 New Dream Network
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

#include "include/rados/librgw.h"
#include "include/rados/rgw_file.h"

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"

#define dout_subsys ceph_subsys_rgw

namespace {
  librgw_t rgw = nullptr;
  string uid("testuser");
  string access_key("");
  string secret_key("");
  struct rgw_fs *fs = nullptr;
  typedef std::tuple<string,uint64_t, struct rgw_file_handle*> fid_type; //in c++2014 can alias...
  std::vector<fid_type> fids1;

  bool do_create = false;
  string bucket_name = "sorry_dave";

  struct {
    int argc;
    char **argv;
  } saved_args;
}

TEST(LibRGW, INIT) {
  int ret = librgw_create(&rgw, nullptr, saved_args.argc, saved_args.argv);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(rgw, nullptr);
}

TEST(LibRGW, MOUNT) {
  int ret = rgw_mount(rgw, uid.c_str(), access_key.c_str(), secret_key.c_str(),
		      &fs);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(fs, nullptr);
}

TEST(LibRGW, CREATE_BUCKET) {
  if (do_create) {
    struct stat st;
    struct rgw_file_handle fh;
    int ret = rgw_mkdir(fs, &fs->root_fh, bucket_name.c_str(), 755, &st, &fh);
    ASSERT_EQ(ret, 0);
  }
}

TEST(LibRGW, DELETE_BUCKET) {
  int ret = rgw_unlink(fs, &fs->root_fh, bucket_name.c_str());
  ASSERT_EQ(ret, 0);
}

extern "C" {
  static bool r1_cb(const char* name, void *arg, uint64_t offset) {
    // don't need arg--it would point to fids1
    fids1.push_back(fid_type(name, offset, nullptr /* handle */));
    return true; /* XXX ? */
  }
}

TEST(LibRGW, LIST_BUCKETS) {
  /* list buckets via readdir in fs root */
  using std::get;

  if (! fs)
    return;

  bool eof = false;
  uint64_t offset = 0;
  int ret = rgw_readdir(fs, &fs->root_fh, &offset, r1_cb, &fids1, &eof);
  for (auto& fid : fids1) {
    std::cout << "fname: " << get<0>(fid) << " fid: " << get<1>(fid)
	      << std::endl;
  }
  ASSERT_EQ(ret, 0);
}

TEST(LibRGW, CLEANUP) {
  using std::get;
  for (auto& fids : { fids1 }) {
    for (auto& fid : fids) {
      delete get<2>(fid);
    }
  }
}

TEST(LibRGW, UMOUNT) {
  if (! fs)
    return;

  int ret = rgw_umount(fs);
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
			      (char*) NULL)) {
      access_key = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--secret",
				     (char*) NULL)) {
      secret_key = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--uid",
				     (char*) NULL)) {
      uid = val;
    }
    else {
      ++arg_iter;
    }
  }

  saved_args.argc = argc;
  saved_args.argv = argv;

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
