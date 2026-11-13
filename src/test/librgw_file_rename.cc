// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

#define dout_subsys ceph_subsys_rgw

using namespace std;

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

  string bucket1_name = "wyndemere";
  string bucket2_name = "galahad";
  string obj_name1 = "tommy1";
  string obj_name2 = "ricky1";
  string obj_name3 = "zoot";

  struct rgw_file_handle* bucket1_fh = nullptr;
  struct rgw_file_handle* bucket2_fh = nullptr;
  struct rgw_file_handle* object_fh = nullptr;

  string subdir1_name = "meep";
  struct rgw_file_handle* subdir1_fh;

  string subdir2_name = "mork";
  struct rgw_file_handle* subdir2_fh;
  
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
  int ret = rgw_mount2(rgw, userid.c_str(), access_key.c_str(),
                       secret_key.c_str(), "/", &fs, RGW_MOUNT_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(fs, nullptr);
}

/* mkdir, tolerating one that is already there.
 *
 * The bucket and subdirectory names are fixed, so a second run of this
 * suite finds them present and rgw_mkdir() returns -EEXIST.  Asserting 0
 * made the suite pass only against a freshly wiped data root, and on a
 * dirty one three tests failed -EEXIST -- which reads like a rename
 * regression and is nothing of the kind. */
static inline
int make_dir(struct rgw_file_handle* parent_fh, const string& name,
	     struct rgw_file_handle** fh_out) {
  struct stat st;
  st.st_uid = owner_uid;
  st.st_gid = owner_gid;
  st.st_mode = 755;

  int ret = rgw_mkdir(fs, parent_fh, name.c_str(), &st, create_mask,
		      fh_out, RGW_MKDIR_FLAG_NONE);
  if (ret == -EEXIST) {
    ret = rgw_lookup(fs, parent_fh, name.c_str(), fh_out,
		     nullptr, 0, RGW_LOOKUP_FLAG_NONE);
  }
  return ret;
}

TEST(LibRGW, CREATE_BUCKETS) {
  if (do_create) {
    struct stat st;
    int ret{0};

    st.st_uid = owner_uid;
    st.st_gid = owner_gid;
    st.st_mode = 755;

    ret = make_dir(fs->root_fh, bucket1_name, &bucket1_fh);
    ASSERT_EQ(ret, 0);
    ret = rgw_fh_rele(fs, bucket1_fh, 0 /* flags */);
    ASSERT_EQ(ret, 0);

    ret = make_dir(fs->root_fh, bucket2_name, &bucket2_fh);
    ASSERT_EQ(ret, 0);
    ret = rgw_fh_rele(fs, bucket2_fh, 0 /* flags */);
    ASSERT_EQ(ret, 0); 
  }
}

TEST(LibRGW, LOOKUP_BUCKETS) {
  int ret{0};
  ret = rgw_lookup(fs, fs->root_fh, bucket1_name.c_str(), &bucket1_fh,
		   nullptr, 0, RGW_LOOKUP_FLAG_NONE);
  ASSERT_EQ(ret, 0);

  ret = rgw_lookup(fs, fs->root_fh, bucket2_name.c_str(), &bucket2_fh,
		   nullptr, 0, RGW_LOOKUP_FLAG_NONE);
  ASSERT_EQ(ret, 0);
}

static inline
int make_object(struct rgw_file_handle* parent_fh, const string& name) {
  int ret{0};
  ret = rgw_lookup(fs, parent_fh, name.c_str(), &object_fh,
		   nullptr, 0, RGW_LOOKUP_FLAG_CREATE);
  if (ret != 0) {
    return ret;
  }

  /* O_RDWR and RGW_OPEN_FLAG_CREATE, as librgw_file_nfsns.cc does.  The
   * previous O_RDONLY open with no create flag could not create the
   * object:  on a driver with an FSIO view rgw_open() opens the file for
   * real, so it returned -ENOENT, the write then had no open to use and
   * returned -EPERM, and every rename below failed -ENOENT on a source
   * that had never been written. */
  ret = rgw_open(fs, object_fh, O_RDWR,
		 RGW_OPEN_FLAG_V3|RGW_OPEN_FLAG_CREATE);
  if (ret != 0) {
    return ret;
  }

  size_t nbytes;
  string data = "hi mom";
  ret = rgw_write(fs, object_fh, 0, data.length(), &nbytes,
		  (void*) data.c_str(), RGW_WRITE_FLAG_NONE);
  if (ret != 0) {
    return ret;
  }
  /* commit write transaction */
  return rgw_close(fs, object_fh, 0 /* flags */);
}

/* The bare v1 create sequence, with none of the flags an FSIO driver needs
 * spelled out:  rgw_lookup(RGW_LOOKUP_FLAG_CREATE), then rgw_open() with
 * posix_flags 0 and no RGW_OPEN_FLAG_CREATE, then write, then close.
 *
 * This is what a v1 consumer written against the rados behaviour does, and
 * it has to keep working:  rgw_open() infers write and create intent from
 * the handle, which rgw_lookup() marked when it found the object absent and
 * was asked to create it.  make_object() above passes the flags explicitly,
 * as librgw_file_nfsns.cc does, so without this test nothing would exercise
 * the inference. */
TEST(LibRGW, V1_CREATE_INFERS_FLAGS) {
  const string name{"v1inferred"};
  struct rgw_file_handle* fh = nullptr;

  int ret = rgw_lookup(fs, bucket1_fh, name.c_str(), &fh,
		       nullptr, 0, RGW_LOOKUP_FLAG_CREATE);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(fh, nullptr);

  /* deliberately the pre-FSIO flags */
  ret = rgw_open(fs, fh, 0 /* posix flags */, 0 /* flags */);
  ASSERT_EQ(ret, 0) << "the v1 create sequence must not require flags it "
		       "never had to pass";

  const string data{"hi mom"};
  size_t nbytes{0};
  ret = rgw_write(fs, fh, 0, data.length(), &nbytes,
		  (void*) data.c_str(), RGW_WRITE_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(nbytes, data.length());
  ASSERT_EQ(rgw_close(fs, fh, 0 /* flags */), 0);
  ASSERT_EQ(rgw_fh_rele(fs, fh, 0 /* flags */), 0);

  /* and the object is really there, with its content -- a create that
   * silently produced nothing is exactly the failure this guards */
  struct rgw_file_handle* rfh = nullptr;
  ret = rgw_lookup(fs, bucket1_fh, name.c_str(), &rfh,
		   nullptr, 0, RGW_LOOKUP_FLAG_NONE);
  ASSERT_EQ(ret, 0) << "object was not created";
  ASSERT_NE(rfh, nullptr);

  struct stat st;
  ASSERT_EQ(rgw_getattr(fs, rfh, &st, RGW_GETATTR_FLAG_NONE), 0);
  ASSERT_EQ(st.st_size, (off_t) data.length());

  ASSERT_EQ(rgw_fh_rele(fs, rfh, 0 /* flags */), 0);
}

TEST(LibRGW, TOPDIR_RENAME) {
  /* rename a file directly residing at the bucket */
  int ret{0};

  ret = make_object(bucket1_fh, obj_name1);
  ASSERT_EQ(ret, 0);

  /* now move it */
  ret = rgw_rename(fs,
		   bucket1_fh, obj_name1.c_str(),
		   bucket1_fh, obj_name2.c_str(),
		   0 /* flags */);
  ASSERT_EQ(ret, 0);

  /* now check the result */
  struct rgw_file_handle* name2_fh;
  ret = rgw_lookup(fs, bucket1_fh, obj_name2.c_str(), &name2_fh,
		   nullptr, 0, RGW_LOOKUP_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  /* release file handle */
  ret = rgw_fh_rele(fs, name2_fh, 0 /* flags */);
  ASSERT_EQ(ret, 0);
}

TEST(LibRGW, SUBDIR_RENAME) {
  int ret{0};

  if (do_create) {
      struct stat st;

      st.st_uid = owner_uid;
      st.st_gid = owner_gid;
      st.st_mode = 755;

      ret = make_dir(bucket1_fh, subdir1_name, &subdir1_fh);
      ASSERT_EQ(ret, 0);

      ret = make_object(subdir1_fh, obj_name1);
      ASSERT_EQ(ret, 0);
  } else {
    ret = rgw_lookup(fs, bucket1_fh, subdir1_name.c_str(), &subdir1_fh,
		     nullptr, 0, RGW_LOOKUP_FLAG_NONE);
    ASSERT_EQ(ret, 0);
  }

  /* now move it */
  ret = rgw_rename(fs,
		   subdir1_fh, obj_name1.c_str(),
		   subdir1_fh, obj_name2.c_str(),
		   0 /* flags */);
  ASSERT_EQ(ret, 0);
  /* now check the result */
  struct rgw_file_handle* name2_fh;
  ret = rgw_lookup(fs, subdir1_fh, obj_name2.c_str(), &name2_fh,
		   nullptr, 0, RGW_LOOKUP_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  /* release file handle */
  ret = rgw_fh_rele(fs, name2_fh, 0 /* flags */);
  ASSERT_EQ(ret, 0);
  /* we'll re-use subdir1_fh */
}

TEST(LibRGW, CROSS_BUCKET_RENAME) {
  /* rename a file across bucket boundaries */
  int ret{0};

  if (do_create) {
      struct stat st;

      st.st_uid = owner_uid;
      st.st_gid = owner_gid;
      st.st_mode = 755;

      ret = make_dir(bucket2_fh, subdir2_name, &subdir2_fh); // galahad/mork
      ASSERT_EQ(ret, 0);

      ret = make_object(subdir1_fh, obj_name1); // wyndemere/meep/tommy1
      ASSERT_EQ(ret, 0);
  } else {
    ret = rgw_lookup(fs, bucket2_fh, subdir2_name.c_str(), &subdir2_fh,
		     nullptr, 0, RGW_LOOKUP_FLAG_NONE);
    ASSERT_EQ(ret, 0);
  }

  /* now move it -- subdir2 is directory mork in bucket galahad */
  ret = rgw_rename(fs,
		   subdir1_fh, obj_name1.c_str(),
		   subdir2_fh, obj_name3.c_str(),
		   0 /* flags */);
  ASSERT_EQ(ret, 0);
  /* now check the result */
  struct rgw_file_handle* name3_fh; // galahad/mork/zoot
  ret = rgw_lookup(fs, subdir2_fh, obj_name3.c_str(), &name3_fh,
		   nullptr, 0, RGW_LOOKUP_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  /* release file handle */
  ret = rgw_fh_rele(fs, name3_fh, 0 /* flags */);
  ASSERT_EQ(ret, 0);
}

TEST(LibRGW, CLEANUP) {
  // do nothing
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
  auto args = argv_to_vec(argc, argv);
  env_to_vec(args);

  char* v = getenv("AWS_ACCESS_KEY_ID");
  if (v) {
    access_key = v;
  }

  v = getenv("AWS_SECRET_ACCESS_KEY");
  if (v) {
    secret_key = v;
  }

  string val;
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
    } else {
      ++arg_iter;
    }
  }

  /* don't accidentally run as anonymous */
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
