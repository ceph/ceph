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
#include <vector>
#include <map>
#include <random>
#include "xxhash.h"

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

  bool do_put = false;
  bool do_get = false;
  bool do_delete = false;

  string bucket_name = "sorry_dave";
  string object_name = "jocaml";

  struct rgw_file_handle *fh = nullptr;

  std::uniform_int_distribution<uint8_t> uint_dist;
  std::mt19937 rng;
  
  struct ZPage
  {
    char data[65536];
    uint64_t cksum;
  }; /* ZPage */
  
  struct ZPageSet
  {
    std::vector<ZPage*> pages;
    struct iovec* iovs;

    ZPageSet(int n) {
      pages.reserve(n);
      iovs = (struct iovec*) calloc(n, sizeof(struct iovec));
      for (int page_ix = 0; page_ix < n; ++page_ix) {
	ZPage* p = new ZPage();
	for (int data_ix = 0; data_ix < 65536; ++data_ix) {
	  p->data[data_ix] = uint_dist(rng);
	} // data_ix
	p->cksum = XXH64(p->data, 65536, 8675309);
	pages.emplace_back(p);
	// and iovs
	struct iovec* iov = &iovs[page_ix];
	iov->iov_base = p->data;
	iov->iov_len = 65536;
      } // page_ix
    }

    int size() { return pages.size(); }

    struct iovec* get_iovs() { return iovs; }

    bool operator==(const ZPageSet& rhs) {
      int n = size();
      for (int page_ix = 0; page_ix < n; ++page_ix) {
	ZPage* p1 = pages[page_ix];
	ZPage* p2 = rhs.pages[page_ix];
	if (p1->cksum != p2->cksum)
	  return false;
      }
      return true;
    }

    void cksum() {
      int n = size();
      for (int page_ix = 0; page_ix < n; ++page_ix) {
	ZPage* p = pages[page_ix];
	p->cksum = XXH64(p->data, 65536, 8675309);
      }
    }

    void reset_iovs() { // VOP_READ and VOP_WRITE update
      int n = size();
      for (int page_ix = 0; page_ix < n; ++page_ix) {
	ZPage* p = pages[page_ix];
	struct iovec* iov = &iovs[page_ix];
	iov->iov_base = p->data;
	iov->iov_len = 65536;
      }
    }

    ~ZPageSet() {
      for (int ix = 0; ix < pages.size(); ++ix)
	delete pages[ix];
      free(iovs);
    }
  }; /* ZPageSet */
  
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

TEST(LibRGW, OBJ_OPEN) {
  int ret = rgw_lookup(fs, fs->root_fh, bucket_name.c_str(), &fh,
		      0 /* flags */);
  ASSERT_EQ(ret, 0);
  ret = rgw_open(fs, fh, 0 /* flags */);
  ASSERT_EQ(ret, 0);
}

TEST(LibRGW, PUT_OBJECT) {
  if (do_put) {
    string data = "hi mom"; // fix this
    int ret = rgw_write(fs, fh, 0, data.length(), (void*) data.c_str());
    ASSERT_EQ(ret, 0);
  }
}

TEST(LibRGW, GET_OBJECT) {
  /* XXX soon */
}

TEST(LibRGW, CLEANUP) {
  int ret = rgw_close(fs, fh, 0 /* flags */);
  ASSERT_EQ(ret, 0);
  ret = rgw_fh_rele(fs, fh, 0 /* flags */);
  ASSERT_EQ(ret, 0);
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
    } else if (ceph_argparse_flag(args, arg_iter, "--get",
					    (char*) nullptr)) {
      do_get = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--put",
					    (char*) nullptr)) {
      do_put = true;
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
