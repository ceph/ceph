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
#include <fstream>
#include <stack>

#include "include/rados/librgw.h"
#include "include/rados/rgw_file.h"
#include "rgw/rgw_file_int.h"
#include "rgw/rgw_lib_frontend.h" // direct requests

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"
#include "include/ceph_assert.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace {

  using namespace rgw;
  using std::get;
  using std::string;

  librgw_t rgw_h = nullptr;
  string userid("testuser");
  string access_key("");
  string secret_key("");
  struct rgw_fs *fs = nullptr;
  CephContext* cct = nullptr;

  uint32_t owner_uid = 867;
  uint32_t owner_gid = 5309;

  uint32_t create_mask = RGW_SETATTR_UID | RGW_SETATTR_GID | RGW_SETATTR_MODE;

  string bucket_name("dmarker");

  class obj_rec
  {
  public:
    string name;
    struct rgw_file_handle* fh;
    struct rgw_file_handle* parent_fh;
    RGWFileHandle* rgw_fh; // alias into fh

    struct state {
      bool readdir;
      state() : readdir(false) {}
    } state;

    obj_rec(string _name, struct rgw_file_handle* _fh,
	    struct rgw_file_handle* _parent_fh, RGWFileHandle* _rgw_fh)
      : name(std::move(_name)), fh(_fh), parent_fh(_parent_fh),
	rgw_fh(_rgw_fh) {}

    void clear() {
      fh = nullptr;
      rgw_fh = nullptr;
    }

    void sync() {
      if (fh)
	rgw_fh = get_rgwfh(fh);
    }

    friend ostream& operator<<(ostream& os, const obj_rec& rec);
  };

  /* Unused
  ostream& operator<<(ostream& os, const obj_rec& rec)
  {
    RGWFileHandle* rgw_fh = rec.rgw_fh;
    if (rgw_fh) {
      const char* type = rgw_fh->is_dir() ? "DIR " : "FILE ";
      os << rec.rgw_fh->full_object_name()
	 << " (" << rec.rgw_fh->object_name() << "): "
	 << type;
    }
    return os;
  }
  */

  std::stack<obj_rec> obj_stack;
  std::deque<obj_rec> cleanup_queue;

  typedef std::vector<obj_rec> obj_vec;
  typedef std::tuple<obj_rec, obj_vec> dirs1_rec;
  typedef std::vector<dirs1_rec> dirs1_vec;

  dirs1_vec dirs_vec;

  struct obj_rec_st
  {
    const obj_rec& obj;
    const struct stat& st;

    obj_rec_st(const obj_rec& _obj, const struct stat& _st)
      : obj(_obj), st(_st) {}
  };

  /* Unused
  ostream& operator<<(ostream& os, const obj_rec_st& rec)
  {
    RGWFileHandle* rgw_fh = rec.obj.rgw_fh;
    if (rgw_fh) {
      const char* type = rgw_fh->is_dir() ? "DIR " : "FILE ";
      os << rgw_fh->full_object_name()
	 << " (" << rgw_fh->object_name() << "): "
	 << type;
      const struct stat& st = rec.st;
      switch(uint8_t(rgw_fh->is_dir())) {
      case 1:
	os << " mode: " << st.st_mode;
	os << " nlinks: " << st.st_nlink;
	break;
      case 0:
      default:
	os << " mode: " << st.st_mode;
	os << " size: " << st.st_size;
	// xxx
	break;
      }
    }
    return os;
  }
  */

  bool do_marker1 = false;
  bool do_marker2 = true;
  bool do_create = false;
  bool do_delete = false;
  bool verbose = false;

  string marker_dir("nfs_marker");
  struct rgw_file_handle *bucket_fh = nullptr;
  struct rgw_file_handle *marker_fh;
  uint32_t marker_nobjs = 2*1024;
  std::deque<obj_rec> marker_objs;

  using dirent_t = std::tuple<std::string, uint64_t>;
  struct dirent_vec
  {
    std::vector<dirent_t> obj_names;
    uint32_t count;
    dirent_vec() : count(0) {}
  };

  struct {
    int argc;
    char **argv;
  } saved_args;
}

TEST(LibRGW, TVAR) {
  typedef boost::variant<uint64_t*, const char*> readdir_offset;

  uint64_t i1{64001};
  std::string s1{"blunderbuss"};

  readdir_offset v1{&i1};
  readdir_offset v2{s1.c_str()};
  readdir_offset v3{static_cast<const char*>(nullptr)};

  uint64_t* pi1 = get<uint64_t*>(v1);
  ASSERT_NE(pi1, nullptr);
  std::cout << "read i1: " << *pi1 << std::endl;

  const char* ps1 = get<const char*>(v2);
  ASSERT_NE(ps1, nullptr);
  std::cout << "read s1: " << ps1 << std::endl;

  const char* ps3 = get<const char*>(v3);
  ASSERT_EQ(ps3, nullptr);
  std::cout << "read s3: " << ps3 << std::endl;
}

TEST(LibRGW, INIT) {
  int ret = librgw_create(&rgw_h, saved_args.argc, saved_args.argv);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(rgw_h, nullptr);
}

TEST(LibRGW, MOUNT) {
  int ret = rgw_mount2(rgw_h, userid.c_str(), access_key.c_str(),
                       secret_key.c_str(), "/", &fs, RGW_MOUNT_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(fs, nullptr);

  cct = static_cast<RGWLibFS*>(fs->fs_private)->get_context();
}

TEST(LibRGW, MARKER1_SETUP_BUCKET) {
  /* "large" directory enumeration test.  this one deals only with
   * file objects */
  struct stat st;
  int ret;

  st.st_uid = owner_uid;
  st.st_gid = owner_gid;
  st.st_mode = 755;

  (void) rgw_lookup(fs, fs->root_fh, bucket_name.c_str(), &bucket_fh,
		    nullptr, 0, RGW_LOOKUP_FLAG_NONE);
  if (! bucket_fh) {
    if (do_create) {
      struct stat st;

      st.st_uid = owner_uid;
      st.st_gid = owner_gid;
      st.st_mode = 755;

      ret = rgw_mkdir(fs, fs->root_fh, bucket_name.c_str(), &st, create_mask,
		      &bucket_fh, RGW_MKDIR_FLAG_NONE);
      ASSERT_EQ(ret, 0);
    }
  }

  ASSERT_NE(bucket_fh, nullptr);

  (void) rgw_lookup(fs, bucket_fh, marker_dir.c_str(), &marker_fh,
		    nullptr, 0, RGW_LOOKUP_FLAG_NONE);
  if (! marker_fh) {
    if (do_create) {
      ret = rgw_mkdir(fs, bucket_fh, marker_dir.c_str(), &st, create_mask,
		      &marker_fh, RGW_MKDIR_FLAG_NONE);
      ASSERT_EQ(ret, 0);
    }
  }

  ASSERT_NE(marker_fh, nullptr);
}

TEST(LibRGW, MARKER1_SETUP_OBJECTS)
{
  /* "large" directory enumeration test.  this one deals only with
   * file objects */

  if (do_create) {
    int ret;

    for (uint32_t ix = 0; ix < marker_nobjs; ++ix) {
      std::string object_name("f_");
      object_name += to_string(ix);
      obj_rec obj{object_name, nullptr, marker_fh, nullptr};
      // lookup object--all operations are by handle
      ret = rgw_lookup(fs, marker_fh, obj.name.c_str(), &obj.fh,
		       nullptr, 0, RGW_LOOKUP_FLAG_CREATE);
      ASSERT_EQ(ret, 0);
      obj.rgw_fh = get_rgwfh(obj.fh);
      // open object--open transaction
      ret = rgw_open(fs, obj.fh, 0 /* posix flags */, RGW_OPEN_FLAG_NONE);
      ASSERT_EQ(ret, 0);
      ASSERT_TRUE(obj.rgw_fh->is_open());
      // unstable write data
      size_t nbytes;
      string data("data for ");
      data += object_name;
      int ret = rgw_write(fs, obj.fh, 0, data.length(), &nbytes,
			  (void*) data.c_str(), RGW_WRITE_FLAG_NONE);
      ASSERT_EQ(ret, 0);
      ASSERT_EQ(nbytes, data.length());
      // commit transaction (write on close)
      ret = rgw_close(fs, obj.fh, 0 /* flags */);
      ASSERT_EQ(ret, 0);
      if (verbose) {
	/* XXX std:cout fragged...did it get /0 in the stream
	 * somewhere? */
	printf("created: %s:%s\n", bucket_name.c_str(), obj.name.c_str());
      }
      // save for cleanup
      marker_objs.push_back(obj);
    }
  }
}

extern "C" {
  static int r2_cb(const char* name, void *arg, uint64_t offset,
		    struct stat* st, uint32_t st_mask,
		    uint32_t flags) {
    dirent_vec& dvec =
      *(static_cast<dirent_vec*>(arg));

    printf("%s bucket=%s dir=%s iv count=%d called back name=%s flags=%d\n",
	   __func__,
	   bucket_name.c_str(),
	   marker_dir.c_str(),
	   dvec.count,
	   name,
	   flags);

    string name_str{name};
    if (! ((name_str == ".") ||
	   (name_str == ".."))) {
      dvec.obj_names.push_back(dirent_t{std::move(name_str), offset});
    }
    return true; /* XXX */
  }
}

TEST(LibRGW, MARKER1_READDIR)
{
  if (do_marker1) {
    using std::get;

    dirent_vec dvec;
    uint64_t offset = 0;
    bool eof = false;

    /* because RGWReaddirRequest::default_max is 1000 (XXX make
     * configurable?) and marker_nobjs is 5*1024, the number
     * of required rgw_readdir operations N should be
     * marker_nobjs/1000 < N < marker_nobjs/1000+1, i.e., 6 when
     * marker_nobjs==5*1024 */
    uint32_t max_iterations = marker_nobjs/1000+1;

    do {
      ASSERT_TRUE(dvec.count <= max_iterations);
      int ret = rgw_readdir(fs, marker_fh, &offset, r2_cb, &dvec, &eof,
			    RGW_READDIR_FLAG_DOTDOT);
      ASSERT_EQ(ret, 0);
      ASSERT_GE(dvec.obj_names.size(), 0);
      ASSERT_EQ(offset, get<1>(dvec.obj_names.back())); // cookie check
      ++dvec.count;
    } while(!eof);
    std::cout << "Read " << dvec.obj_names.size() << " objects in "
	      << marker_dir.c_str() << std::endl;
  }
}

TEST(LibRGW, MARKER2_READDIR)
{
  if (do_marker2) {
    using std::get;

    dirent_vec dvec;
    std::string marker{""};
    bool eof = false;

    /* because RGWReaddirRequest::default_max is 1000 (XXX make
     * configurable?) and marker_nobjs is 5*1024, the number
     * of required rgw_readdir operations N should be
     * marker_nobjs/1000 < N < marker_nobjs/1000+1, i.e., 6 when
     * marker_nobjs==5*1024 */
    uint32_t max_iterations = marker_nobjs/1000+1;

    do {
      ASSERT_TRUE(dvec.count <= max_iterations);
      int ret = rgw_readdir2(fs, marker_fh,
			     (marker.length() > 0) ? marker.c_str() : nullptr,
			     r2_cb, &dvec, &eof,
			     RGW_READDIR_FLAG_NONE);
      ASSERT_EQ(ret, 0);
      ASSERT_GE(dvec.obj_names.size(), 0);
      marker = get<0>(dvec.obj_names.back());
      ++dvec.count;
    } while((!eof) && dvec.count < 4);
    std::cout << "Read " << dvec.obj_names.size() << " objects in "
	      << marker_dir.c_str() << std::endl;
  }
}

TEST(LibRGW, MARKER1_OBJ_CLEANUP)
{
  int rc;
  for (auto& obj : marker_objs) {
    if (obj.fh) {
      if (do_delete) {
	if (verbose) {
	  std::cout << "unlinking: " << bucket_name << ":" << obj.name
		    << std::endl;
	}
	rc = rgw_unlink(fs, marker_fh, obj.name.c_str(), RGW_UNLINK_FLAG_NONE);
      }
      rc = rgw_fh_rele(fs, obj.fh, 0 /* flags */);
      ASSERT_EQ(rc, 0);
    }
  }
  marker_objs.clear();
}

TEST(LibRGW, CLEANUP) {
  int rc;

  if (do_marker1) {
    cleanup_queue.push_back(
      obj_rec{bucket_name, bucket_fh, fs->root_fh, get_rgwfh(fs->root_fh)});
  }

  for (auto& elt : cleanup_queue) {
    if (elt.fh) {
      rc = rgw_fh_rele(fs, elt.fh, 0 /* flags */);
      ASSERT_EQ(rc, 0);
    }
  }
  cleanup_queue.clear();
}

TEST(LibRGW, UMOUNT) {
  if (! fs)
    return;

  int ret = rgw_umount(fs, RGW_UMOUNT_FLAG_NONE);
  ASSERT_EQ(ret, 0);
}

TEST(LibRGW, SHUTDOWN) {
  librgw_shutdown(rgw_h);
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
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--bn",
				     (char*) nullptr)) {
      bucket_name = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--uid",
				     (char*) nullptr)) {
      owner_uid = std::stoi(val);
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--gid",
				     (char*) nullptr)) {
      owner_gid = std::stoi(val);
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--nobjs",
				     (char*) nullptr)) {
      marker_nobjs = std::stoi(val);
    } else if (ceph_argparse_flag(args, arg_iter, "--marker1",
					    (char*) nullptr)) {
      do_marker1 = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--create",
					    (char*) nullptr)) {
      do_create = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--delete",
					    (char*) nullptr)) {
      do_delete = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--verbose",
					    (char*) nullptr)) {
      verbose = true;
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
