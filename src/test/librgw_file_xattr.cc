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
#include <vector>
#include <map>
#include <random>
#include <boost/algorithm/string.hpp>

#include "include/rados/librgw.h"
#include "include/rados/rgw_file.h"
#include "rgw/rgw_file_int.h"

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "common/errno.h"
#include "common/debug.h"
#include "global/global_init.h"
#include "include/ceph_assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace {

  using namespace rgw;

  string uid("testuser");
  string access_key("");
  string secret_key("");

  librgw_t rgw_h = nullptr;
  struct rgw_fs *fs = nullptr;

  uint32_t owner_uid = 867;
  uint32_t owner_gid = 5309;
  uint32_t create_mask = RGW_SETATTR_UID | RGW_SETATTR_GID | RGW_SETATTR_MODE;

  string bucket_name{"v4recov"};
  string object_path{"node0/clientids"};

  string key1{"black"};
  string val1{"metallic"};

  struct rgw_file_handle *bucket_fh = nullptr;
  struct rgw_file_handle *object_fh = nullptr;

  typedef std::tuple<string,uint64_t, struct rgw_file_handle*> fid_type;
  std::vector<fid_type> fids;

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

  [[maybe_unused]] ostream& operator<<(ostream& os, const obj_rec& rec)
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

  typedef std::vector<obj_rec> obj_vec;
  obj_vec ovec;

  bool do_stat = false;
  bool do_create = false;
  bool do_delete = false;
  bool do_hexdump = false;
  bool verbose = false;

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
		      secret_key.c_str(), &fs, RGW_MOUNT_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(fs, nullptr);
}

TEST(LibRGW, LOOKUP_BUCKET) {
  int ret = rgw_lookup(fs, fs->root_fh, bucket_name.c_str(), &bucket_fh,
		       nullptr, 0, RGW_LOOKUP_FLAG_NONE);
  ASSERT_EQ(ret, 0);
}

TEST(LibRGW, CREATE_BUCKET) {
  if ((! bucket_fh) && do_create) {
    struct stat st;

    st.st_uid = owner_uid;
    st.st_gid = owner_gid;
    st.st_mode = 755;

    int ret = rgw_mkdir(fs, fs->root_fh, bucket_name.c_str(), &st, create_mask,
			&bucket_fh, RGW_MKDIR_FLAG_NONE);
    ASSERT_EQ(ret, 0);
  }
}

TEST(LibRGW, CREATE_PATH) {

  if (!bucket_fh)
    return;

  vector<string> segs;
  boost::split(segs, object_path, boost::is_any_of("/"));

  struct stat st;
  st.st_uid = owner_uid;
  st.st_gid = owner_gid;
  st.st_mode = 755;

  int ix, ret, sz = segs.size();
  for (ix = 0; ix < sz; ++ix) {
    auto& seg = segs[ix];
    struct rgw_file_handle* parent_fh = (ix > 0) ? ovec[ix-1].fh : bucket_fh;
    obj_rec dir{seg, nullptr, parent_fh, nullptr};
    if (do_create) {
      ret = rgw_mkdir(fs, dir.parent_fh, dir.name.c_str(), &st, create_mask,
		      &dir.fh, RGW_MKDIR_FLAG_NONE);
    } else {
      ret = rgw_lookup(fs, dir.parent_fh, dir.name.c_str(), &dir.fh,
		       nullptr, 0, RGW_LOOKUP_FLAG_NONE);
    }
    ASSERT_EQ(ret, 0);
    dir.sync();
    ovec.push_back(dir);
    if (verbose) {
      std::cout << "create: " << dir.name << std::endl;
    }
  }
}

TEST(LibRGW, CHECK_PATH_REFS) {

  if (!bucket_fh)
    return;

  int ix, sz = ovec.size();
  for (ix = 0; ix < sz; ++ix) {
    auto& dir = ovec[ix];
    if (verbose) {
      std::cout << "name: " << dir.name
		<< " refcnt: " << dir.rgw_fh->get_refcnt()
		<< std::endl;
    }
    if (ix == 0) {
      // sentinel, +1 parent, +1 path
      ASSERT_EQ(dir.rgw_fh->get_refcnt(), 3U);
    }
    if (ix == 1) {
      // sentinel, +1 path
      ASSERT_EQ(dir.rgw_fh->get_refcnt(), 2U);
    }
  }
}

TEST(LibRGW, SETXATTR1) {

  if (!bucket_fh)
    return;

  auto& dir = ovec[ovec.size()-1];
  rgw_xattrstr xattr_k = { const_cast<char*>(key1.c_str()),
			   uint32_t(key1.length()) };
  rgw_xattrstr xattr_v = { const_cast<char*>(val1.c_str()),
			   uint32_t(val1.length()) };

  rgw_xattr xattr = { xattr_k, xattr_v };
  rgw_xattrlist xattrlist = { &xattr, 1 };

  int ret = rgw_setxattrs(fs, dir.fh, &xattrlist, RGW_SETXATTR_FLAG_NONE);
  ASSERT_EQ(ret, 0);
}

extern "C" {
  static int getattr_cb(rgw_xattrlist *attrs, void *arg, uint32_t flags)
  {
    auto& attrmap =
      *(static_cast<std::map<std::string, std::string>*>(arg));
    for (uint32_t ix = 0; ix < attrs->xattr_cnt; ++ix) {
      auto& xattr = attrs->xattrs[ix];
      string k{xattr.key.val, xattr.key.len};
      string v{xattr.val.val, xattr.val.len};
      if (verbose) {
	std::cout << __func__
		  << " attr k: " << k << " v: " << v
		  << std::endl;
      }
      attrmap.insert(std::map<std::string, std::string>::value_type(k, v));
    }
    return 0;
  }
}

TEST(LibRGW, GETXATTR1) {

  if (!bucket_fh)
    return;

  using std::get;
  auto& dir = ovec[ovec.size()-1];

  rgw_xattrstr xattr_k1 =
    {const_cast<char*>(key1.c_str()), uint32_t(key1.length())};
  rgw_xattrstr xattr_v1 = {nullptr, 0};

  std::string key2 = "user.rgw.etag";
  rgw_xattrstr xattr_k2 =
    {const_cast<char*>(key2.c_str()), uint32_t(key2.length())};
  rgw_xattrstr xattr_v2 = {nullptr, 0};

  rgw_xattr xattrs[2] = {{xattr_k1, xattr_v1},
			 {xattr_k2, xattr_v2}};

  /* XXX gcc won't accept static_cast here, don't see why */
  rgw_xattrlist xattrlist = {reinterpret_cast<rgw_xattr*>(&xattrs), 2};

  std::map<std::string, std::string> out_attrmap;

  int ret = rgw_getxattrs(fs, dir.fh, &xattrlist, getattr_cb, &out_attrmap,
			  RGW_GETXATTR_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  /* check exposed attrs */
  ASSERT_TRUE(out_attrmap.find("user.rgw.etag") != out_attrmap.end());
  /* check our user attr */
  ASSERT_TRUE(out_attrmap.find(key1) != out_attrmap.end());
}

TEST(LibRGW, LSXATTR1) {

  if (!bucket_fh)
    return;

  using std::get;
  auto& dir = ovec[ovec.size()-1];

  rgw_xattrstr filter_prefix = { nullptr, 0}; // XXX ignored, for now

  std::map<std::string, std::string> out_attrmap;

  int ret = rgw_lsxattrs(fs, dir.fh, &filter_prefix, getattr_cb,
			 &out_attrmap, RGW_LSXATTR_FLAG_NONE);
  ASSERT_EQ(ret, 0);

  /* check exposed attrs */
  ASSERT_TRUE(out_attrmap.find("user.rgw.etag") != out_attrmap.end());
}

TEST(LibRGW, RMXATTR1) {

  if (!bucket_fh)
    return;

  using std::get;
  auto& dir = ovec[ovec.size()-1];

  rgw_xattrstr xattr_k = { const_cast<char*>(key1.c_str()),
			   uint32_t(key1.length()) };
  rgw_xattrstr xattr_v = { nullptr, 0 };

  rgw_xattr xattr = { xattr_k, xattr_v };
  rgw_xattrlist xattrlist = { &xattr, 1 };

  int ret = rgw_rmxattrs(fs, dir.fh, &xattrlist, RGW_RMXATTR_FLAG_NONE);
  ASSERT_EQ(ret, 0);
}

TEST(LibRGW, LSXATTR2) {

  if (!bucket_fh)
    return;

  using std::get;
  auto& dir = ovec[ovec.size()-1];

  rgw_xattrstr filter_prefix = { nullptr, 0 }; // XXX ignored, for now

  std::map<std::string, std::string> out_attrmap;

  int ret = rgw_lsxattrs(fs, dir.fh, &filter_prefix, getattr_cb,
			 &out_attrmap, RGW_LSXATTR_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  /* check exposed attrs */
  ASSERT_TRUE(out_attrmap.find("user.rgw.etag") != out_attrmap.end());
  /* verify deletion */
  ASSERT_TRUE(out_attrmap.find(key1) == out_attrmap.end());
}

TEST(LibRGW, CLEANUP) {
  int ret = 0;
  if (object_fh) {
    ret = rgw_fh_rele(fs, object_fh, 0 /* flags */);
    ASSERT_EQ(ret, 0);
  }
  if (bucket_fh) {
    ret = rgw_fh_rele(fs, bucket_fh, 0 /* flags */);
  }
  ASSERT_EQ(ret, 0);
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
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--uid",
				     (char*) nullptr)) {
      uid = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--bn",
				     (char*) nullptr)) {
      bucket_name = val;
    } else if (ceph_argparse_flag(args, arg_iter, "--stat",
					    (char*) nullptr)) {
      do_stat = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--create",
					    (char*) nullptr)) {
      do_create = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--delete",
					    (char*) nullptr)) {
      do_delete = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--hexdump",
					    (char*) nullptr)) {
      do_hexdump = true;
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
