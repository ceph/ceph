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
#include <stack>

#include "include/rados/librgw.h"
#include "include/rados/rgw_file.h"
#include "rgw/rgw_file.h"

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"

#define dout_subsys ceph_subsys_rgw

namespace {

  using namespace rgw;
  using std::get;
  using std::string;

  librgw_t rgw_h = nullptr;
  string uid("testuser");
  string access_key("");
  string secret_key("");
  struct rgw_fs *fs = nullptr;

  string bucket_name = "nfsroot";

  class obj_rec
  {
  public:
    string name;
    struct rgw_file_handle* fh;
    struct rgw_file_handle* parent_fh;
    RGWFileHandle* rgw_fh; // alias into fh

    obj_rec(string _name, struct rgw_file_handle* _fh,
	    struct rgw_file_handle* _parent_fh, RGWFileHandle* _rgw_fh)
      : name(std::move(_name)), fh(_fh), parent_fh(_parent_fh),
	rgw_fh(_rgw_fh) {}
  };
  
  std::stack<obj_rec> obj_stack;
  std::deque<obj_rec> obj_queue;

  struct {
    int argc;
    char **argv;
  } saved_args;
}

TEST(LibRGW, INIT) {
  int ret = librgw_create(&rgw_h, saved_args.argc, saved_args.argv);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(rgw_h, nullptr);
}

TEST(LibRGW, MOUNT) {
  int ret = rgw_mount(rgw_h, uid.c_str(), access_key.c_str(),
		      secret_key.c_str(), &fs);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(fs, nullptr);
}

#if 0
extern "C" {
  static bool r1_cb(const char* name, void *arg, uint64_t offset) {
    // don't need arg--it would point to fids1
    buckets.push_back(fid_type(name, offset, nullptr /* handle */));
    return true; /* XXX ? */
  }
}
#endif

TEST(LibRGW, ENUMERATE1) {
  int rc;
  obj_stack.push(obj_rec{bucket_name, nullptr, nullptr, nullptr});
  while (! obj_stack.empty()) {
    auto& elt = obj_stack.top();
    if (! elt.fh) {
      struct rgw_file_handle* parent_fh = elt.parent_fh
	? elt.parent_fh : fs->root_fh;
      rc = rgw_lookup(fs, parent_fh, elt.name.c_str(), &elt.fh,
		      RGW_LOOKUP_FLAG_NONE);
      ASSERT_EQ(rc, 0);
      ASSERT_NE(elt.fh, nullptr);
      elt.parent_fh = parent_fh;
      // what next? push?
    } else {
      // we have a leaf in some state in top position
      // 1. finish it?
      // 2. print it?
      // 3. push it onto obj_queue for cleanup
    }
  }
}

TEST(LibRGW, CLEANUP) {
  // do nothing
}

TEST(LibRGW, UMOUNT) {
  if (! fs)
    return;

  int ret = rgw_umount(fs);
  ASSERT_EQ(ret, 0);
}

TEST(LibRGW, SHUTDOWN) {
  librgw_shutdown(rgw_h);
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
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--uid",
				     (char*) nullptr)) {
      uid = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--bn",
				     (char*) nullptr)) {
      bucket_name = val;
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
