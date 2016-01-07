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
#include <stack>

#include "include/rados/librgw.h"
#include "include/rados/rgw_file.h"
#include "rgw/rgw_file.h"
#include "rgw/rgw_lib_frontend.h" // direct requests

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"
#include "include/assert.h"

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
  CephContext* cct = nullptr;

  string bucket_name("nfsroot");

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

    friend ostream& operator<<(ostream& os, const obj_rec& rec);
  };

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
  
  std::stack<obj_rec> obj_stack;
  std::deque<obj_rec> cleanup_queue;

  bool do_marker1 = false;
  bool do_create = false;
  bool do_delete = false;
  bool verbose = false;

  string marker_dir("nfs_marker");
  struct rgw_file_handle *bucket_fh = nullptr;
  struct rgw_file_handle *marker_fh;
  static constexpr int marker_nobjs = 2*1024;
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

  cct = static_cast<RGWLibFS*>(fs->fs_private)->get_context();
}

TEST(LibRGW, SETUP_ENUMERATE1)
{
  if (do_create) {
    /* create objects directly */
    std::vector<std::string> obj_names =
      {"foo/bar/baz/quux",
       "foo/f1",
       "foo/f2",
       "foo/bar/f1",
       "foo/bar/d1/",
       "foo/bar/baz/hungry",
       "foo/bar/baz/hungry/",
       "foo/bar/baz/momma",
       "foo/bar/baz/bear/",
       "foo/bar/baz/sasquatch",
       "foo/bar/baz/sasquatch/",
       "foo/bar/baz/frobozz"};

    buffer::list bl; // empty object
    RGWLibFS *fs_private = static_cast<RGWLibFS*>(fs->fs_private);

    for (const auto& obj_name : obj_names) {
      if (verbose) {
	std::cout << "creating: " << bucket_name << ":" << obj_name
		  << std::endl;
      }
      RGWPutObjRequest req(cct, fs_private->get_user(), bucket_name, obj_name,
			   bl);
      int rc = rgwlib.get_fe()->execute_req(&req);
      int rc2 = req.get_ret();
      ASSERT_EQ(rc, 0);
      ASSERT_EQ(rc2, 0);
    }
  }
}

extern "C" {
  static bool r1_cb(const char* name, void *arg, uint64_t offset) {
    struct rgw_file_handle* parent_fh
      = static_cast<struct rgw_file_handle*>(arg);
    RGWFileHandle* rgw_fh = get_rgwfh(parent_fh);
    lsubdout(cct, rgw, 10) << __func__
			   << " bucket=" << rgw_fh->bucket_name()
			   << " dir=" << rgw_fh->full_object_name()
			   << " called back name=" << name
			   << dendl;
    obj_stack.push(
      obj_rec{name, nullptr, parent_fh, nullptr});
    return true; /* XXX */
  }
}

TEST(LibRGW, ENUMERATE1) {
  int rc;
  obj_stack.push(
    obj_rec{bucket_name, nullptr, nullptr, nullptr});
  while (! obj_stack.empty()) {
    auto& elt = obj_stack.top();
    if (! elt.fh) {
      struct rgw_file_handle* parent_fh = elt.parent_fh
	? elt.parent_fh : fs->root_fh;
      RGWFileHandle* pfh = get_rgwfh(parent_fh);
      rgw::ignore(pfh);
      lsubdout(cct, rgw, 10)
	<< "rgw_lookup:"
	<< " parent object_name()=" << pfh->object_name()
	<< " parent full_object_name()=" << pfh->full_object_name()
	<< " elt.name=" << elt.name
	<< dendl;
      rc = rgw_lookup(fs, parent_fh, elt.name.c_str(), &elt.fh,
		      RGW_LOOKUP_FLAG_NONE);
      ASSERT_EQ(rc, 0);
      // XXXX
      RGWFileHandle* efh = get_rgwfh(elt.fh);
      rgw::ignore(efh);
      lsubdout(cct, rgw, 10)
	<< "rgw_lookup result:"
	<< " elt object_name()=" << efh->object_name()
	<< " elt full_object_name()=" << efh->full_object_name()
	<< " elt.name=" << elt.name
	<< dendl;

      ASSERT_NE(elt.fh, nullptr);
      elt.rgw_fh = get_rgwfh(elt.fh);
      elt.parent_fh = elt.rgw_fh->get_parent()->get_fh();
      ASSERT_EQ(elt.parent_fh, parent_fh);
      continue;
    } else {
      // we have a handle in some state in top position
      switch(elt.fh->fh_type) {
      case RGW_FS_TYPE_DIRECTORY:
	if (! elt.state.readdir) {
	  // descending
	  uint64_t offset = 0;
	  bool eof; // XXX
	  lsubdout(cct, rgw, 10)
	    << "readdir in"
	    << " bucket: " << elt.rgw_fh->bucket_name()
	    << " object_name: " << elt.rgw_fh->object_name()
	    << " full_name: " << elt.rgw_fh->full_object_name()
	    << dendl;
	  rc = rgw_readdir(fs, elt.fh, &offset, r1_cb, elt.fh, &eof);
	  elt.state.readdir = true;
	  ASSERT_EQ(rc, 0);
	  // ASSERT_TRUE(eof); // XXXX working incorrectly w/single readdir
	} else {
	  // ascending
	  std::cout << elt << std::endl;
	  cleanup_queue.push_back(elt);
	  obj_stack.pop();
	}
	break;
      case RGW_FS_TYPE_FILE:
	// ascending
	std::cout << elt << std::endl;
	cleanup_queue.push_back(elt);
	obj_stack.pop();
	break;
      default:
	abort();
      };
    }
  }
}

TEST(LibRGW, MARKER1_SETUP_BUCKET)
{
  /* "large" directory enumeration test.  this one deals only with
   * file objects */

  if (do_marker1) {
    struct stat st;
    int ret;

    if (do_create) {
      ret = rgw_mkdir(fs, fs->root_fh, bucket_name.c_str(), 755, &st,
		      &bucket_fh);
    }
    if (! bucket_fh) {
      ret = rgw_lookup(fs, fs->root_fh, bucket_name.c_str(), &bucket_fh,
		       RGW_LOOKUP_FLAG_NONE);
      ASSERT_EQ(ret, 0);
    }
    ASSERT_NE(bucket_fh, nullptr);

    if (do_create) {
      ret = rgw_mkdir(fs, bucket_fh, marker_dir.c_str(), 755, &st, &marker_fh);
    } else {
      ret = rgw_lookup(fs, bucket_fh, marker_dir.c_str(), &marker_fh,
		       RGW_LOOKUP_FLAG_NONE);
    }
    ASSERT_EQ(ret, 0);
  }
}

TEST(LibRGW, MARKER1_SETUP_OBJECTS)
{
  /* "large" directory enumeration test.  this one deals only with
   * file objects */

  if (do_marker1 && do_create) {
    int ret;

    for (int ix = 0; ix < marker_nobjs; ++ix) {
      std::string object_name("f_");
      object_name += to_string(ix);
      obj_rec obj{object_name, nullptr, marker_fh, nullptr};
      // lookup object--all operations are by handle
      ret = rgw_lookup(fs, marker_fh, obj.name.c_str(), &obj.fh,
		       RGW_LOOKUP_FLAG_CREATE);
      ASSERT_EQ(ret, 0);
      obj.rgw_fh = get_rgwfh(obj.fh);
      // open object--open transaction
      ret = rgw_open(fs, obj.fh, 0 /* flags */);
      ASSERT_EQ(ret, 0);
      ASSERT_TRUE(obj.rgw_fh->is_open());
      // unstable write data
      size_t nbytes;
      string data("data for ");
      data += object_name;
      int ret = rgw_write(fs, obj.fh, 0, data.length(), &nbytes,
			  (void*) data.c_str());
      ASSERT_EQ(ret, 0);
      ASSERT_EQ(nbytes, data.length());
      // commit transaction (write on close)
      ret = rgw_close(fs, obj.fh, 0 /* flags */);
      ASSERT_EQ(ret, 0);
      // save for cleanup
      marker_objs.push_back(obj);
    }
  }
}

extern "C" {
  static bool r2_cb(const char* name, void *arg, uint64_t offset) {
    dirent_vec& dvec =
      *(static_cast<dirent_vec*>(arg));
    lsubdout(cct, rgw, 10) << __func__
			   << " bucket=" << bucket_name
			   << " dir=" << marker_dir
			   << " iv count=" << dvec.count
			   << " called back name=" << name
			   << dendl;
    dvec.obj_names.push_back(dirent_t{name, offset});
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
      int ret = rgw_readdir(fs, marker_fh, &offset, r2_cb, &dvec, &eof);
      ASSERT_EQ(ret, 0);
      ASSERT_EQ(offset, get<1>(dvec.obj_names.back())); // cookie check
      ++dvec.count;
    } while(!eof);
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
	rc = rgw_unlink(fs, marker_fh, obj.name.c_str());
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
