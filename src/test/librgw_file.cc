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
  typedef std::tuple<string, uint64_t, struct rgw_file_handle*> fid_type; //in c++2014 can alias...
  typedef std::tuple<fid_type, std::vector<fid_type>> bucket_type;
		     
  std::vector<fid_type> fids1;
  std::vector<bucket_type> bucket_matrix;

  bool do_getattr = false;

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
  int ret = rgw_mount(rgw, uid.c_str(), access_key.c_str(), secret_key.c_str(),
		      &fs, RGW_MOUNT_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(fs, nullptr);
}

TEST(LibRGW, GETATTR_ROOT) {
  if (do_getattr) {
    using std::get;

    if (! fs)
      return;

    struct stat st;
    int ret = rgw_getattr(fs, fs->root_fh, &st, RGW_GETATTR_FLAG_NONE);
    ASSERT_EQ(ret, 0);
  }
}

extern "C" {
  static bool r1_cb(const char* name, void *arg, uint64_t offset,
		    uint32_t flags) {
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
  int ret = rgw_readdir(fs, fs->root_fh, &offset, r1_cb, &fids1, &eof,
			RGW_READDIR_FLAG_NONE);
  for (auto& fid : fids1) {
    std::cout << "fname: " << get<0>(fid) << " fid: " << get<1>(fid)
	      << std::endl;
    /* push row for bucket into bucket_matrix */
    bucket_matrix.push_back(bucket_type(fid, std::vector<fid_type>()));
  }
  ASSERT_EQ(ret, 0);
}

TEST(LibRGW, LOOKUP_BUCKETS) {
  using std::get;

  if (! fs)
    return;

  int ret = 0;
  for (auto& fid_row : bucket_matrix) {
    auto& fid = get<0>(fid_row);
    // auto& obj_vector = get<1>(fid_row);
    struct rgw_file_handle *rgw_fh = nullptr;
    ret = rgw_lookup(fs, fs->root_fh, get<0>(fid).c_str(), &rgw_fh,
		    0 /* flags */);
    ASSERT_EQ(ret, 0);
    get<2>(fid) = rgw_fh;
    ASSERT_NE(get<2>(fid), nullptr);
  }
}

TEST(LibRGW, GETATTR_BUCKETS) {
  if (do_getattr) {
    using std::get;

    if (! fs)
      return;

    int ret = 0;
    struct stat st;

    for (auto& fid_row : bucket_matrix) {
      auto& fid = get<0>(fid_row);
      struct rgw_file_handle *rgw_fh = get<2>(fid);
      ret = rgw_getattr(fs, rgw_fh, &st, RGW_GETATTR_FLAG_NONE);
      ASSERT_EQ(ret, 0);
    }
  }
}

extern "C" {
  static bool r2_cb(const char* name, void *arg, uint64_t offset,
		    uint32_t flags) {
    std::vector<fid_type>& obj_vector = *(static_cast<std::vector<fid_type>*>(arg));
    obj_vector.push_back(fid_type(name, offset, nullptr));
    return true; /* XXX ? */
  }
}

TEST(LibRGW, LIST_OBJECTS) {
  /* list objects via readdir, bucketwise */
  using std::get;

  if (! fs)
    return;

  for (auto& fid_row : bucket_matrix) {
    auto& fid = get<0>(fid_row); // bucket
    std::vector<fid_type>& obj_vector = get<1>(fid_row); // objects in bucket
    struct rgw_file_handle *bucket_fh = get<2>(fid);

    ldout(g_ceph_context, 0) << __func__ << " readdir on bucket " << get<0>(fid)
			     << dendl;

    bool eof = false;
    uint64_t offset = 0;
    int ret = rgw_readdir(fs, bucket_fh, &offset, r2_cb, &obj_vector, &eof,
			  RGW_READDIR_FLAG_NONE);
    for (auto& obj : obj_vector) {
      std::cout << "fname: " << get<0>(obj) << " fid: " << get<1>(obj)
		<< std::endl;
    }
    ASSERT_EQ(ret, 0);
  }
}

extern bool global_stop;

TEST(LibRGW, GETATTR_OBJECTS) {
  if (do_getattr) {
    using std::get;
    struct stat st;
    int ret;

    global_stop = true;

    for (auto& fid_row : bucket_matrix) {
      auto& fid = get<0>(fid_row); // bucket
      std::vector<fid_type>& obj_vector = get<1>(fid_row); // objects in bucket
      struct rgw_file_handle *bucket_fh = get<2>(fid);

      for (auto& obj : obj_vector) {
	struct rgw_file_handle *obj_fh = nullptr;
	std::string object_name = get<0>(obj);
	ret = rgw_lookup(fs, bucket_fh, get<0>(obj).c_str(), &obj_fh,
			0 /* flags */);
	ASSERT_EQ(ret, 0);
	get<2>(obj) = obj_fh; // stash obj_fh for cleanup
	ASSERT_NE(get<2>(obj), nullptr);
	ret = rgw_getattr(fs, obj_fh, &st,
			  RGW_GETATTR_FLAG_NONE); // now, getattr
	ASSERT_EQ(ret, 0);
      }
    }
  }
}

TEST(LibRGW, CLEANUP) {
  int ret = 0;
  using std::get;

  /* release file handles */
  for (auto& fid_row : bucket_matrix) {
    auto& bucket = get<0>(fid_row); // bucket
    std::vector<fid_type>& obj_vector = get<1>(fid_row); // objects in bucket
    for (auto& obj : obj_vector) {
      struct rgw_file_handle *obj_fh = get<2>(obj);
      if (obj_fh) {
	ret = rgw_fh_rele(fs, obj_fh, 0 /* flags */);
	ASSERT_EQ(ret, 0);
      }
    }
    // now the bucket
    struct rgw_file_handle *bucket_fh = get<2>(bucket);
    if (bucket_fh) {
      ret = rgw_fh_rele(fs, bucket_fh, 0 /* flags */);
      ASSERT_EQ(ret, 0);
    }
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
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--uid",
				     (char*) nullptr)) {
      uid = val;
    } else if (ceph_argparse_flag(args, arg_iter, "--getattr",
					    (char*) nullptr)) {
      do_getattr = true;
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
